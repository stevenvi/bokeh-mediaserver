package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/auth"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
)

type adminHandler struct {
	db          *pgxpool.Pool // kept for Begin() in setCollectionAccess
	users       *repository.UserRepository
	devices     *repository.DeviceRepository
	guard       *DeviceGuard
	collections *repository.CollectionRepository
	media       *repository.MediaItemRepository
	jobs        *repository.JobRepository
	pool        *jobs.Pool
	authPlugins map[string]auth.Plugin
	authHandler *authHandler
	mediaPath   string
	dataPath    string
}

// POST /api/v1/admin/collections
func (h *adminHandler) createCollection(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name         string `json:"name"`
		Type         string `json:"type"`
		RelativePath string `json:"relative_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	body.Name = strings.TrimSpace(body.Name)
	body.Type = strings.TrimSpace(body.Type)
	body.RelativePath = strings.TrimSpace(body.RelativePath)
	if body.Name == "" || body.Type == "" || body.RelativePath == "" {
		writeError(w, http.StatusBadRequest, "name, type, and relative_path are required")
		return
	}

	// Verify the path exists on the filesystem.
	fullPath := filepath.Join(h.mediaPath, body.RelativePath)
	if info, err := os.Stat(fullPath); err != nil {
		writeError(w, http.StatusBadRequest, "path does not exist: "+body.RelativePath)
		return
	} else if !info.IsDir() {
		writeError(w, http.StatusBadRequest, "path is not a directory: "+body.RelativePath)
		return
	}

	// Verify no other collection already uses this path.
	if exists, err := h.collections.ExistsByRelativePath(r.Context(), body.RelativePath); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	} else if exists {
		writeError(w, http.StatusConflict, "a collection with that path already exists")
		return
	}

	id, err := h.collections.Create(r.Context(), body.Name, body.Type, body.RelativePath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create collection: "+err.Error())
		return
	}

	// Auto-queue a scan for the newly created collection
	relatedType := "collection"
	jobID, err := h.jobs.Create(r.Context(), "library_scan", &id, &relatedType)
	if err != nil {
		slog.Warn("auto-queue scan for new collection", "collection_id", id, "err", err)
	} else {
		slog.Info("auto-queued scan for new collection", "collection_id", id, "job_id", jobID)
	}

	writeJSON(w, http.StatusCreated, map[string]any{"id": id, "scan_job_id": jobID})
}

// DELETE /api/v1/admin/collections/:id
func (h *adminHandler) deleteCollection(w http.ResponseWriter, r *http.Request) {
	collectionID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	affected, err := h.collections.Delete(r.Context(), collectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if affected == 0 {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/collections
func (h *adminHandler) listCollections(w http.ResponseWriter, r *http.Request) {
	collections, err := h.collections.ListTopLevel(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, collections)
}

// POST /api/v1/admin/collections/:id/scan
func (h *adminHandler) triggerScan(w http.ResponseWriter, r *http.Request) {
	collectionID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	isEnabled, err := h.collections.GetEnabled(r.Context(), collectionID)
	if err != nil {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}
	if !isEnabled {
		writeError(w, http.StatusBadRequest, "collection is disabled")
		return
	}

	// Prevent duplicate scans
	active, err := h.jobs.IsActive(r.Context(), "library_scan", collectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "job check failed")
		return
	}
	if active {
		writeError(w, http.StatusConflict, "a scan is already queued or running for this collection")
		return
	}

	// Create the job row — the dispatcher picks it up on next poll
	relatedType := "collection"
	jobID, err := h.jobs.Create(r.Context(), "library_scan", &collectionID, &relatedType)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	slog.Info("scan job queued", "job_id", jobID, "collection_id", collectionID)
	writeJSON(w, http.StatusAccepted, map[string]int64{"job_id": jobID})
}

// GET /api/v1/admin/jobs/:id
func (h *adminHandler) getJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	job, err := h.jobs.GetByID(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// POST /api/v1/admin/maintenance/orphan-cleanup
func (h *adminHandler) triggerOrphanCleanup(w http.ResponseWriter, r *http.Request) {
	jobID, err := h.jobs.Create(r.Context(), "orphan_cleanup", nil, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}
	slog.Info("orphan cleanup job queued", "job_id", jobID)
	writeJSON(w, http.StatusAccepted, map[string]int64{"job_id": jobID})
}

// POST /api/v1/admin/maintenance/integrity-check
func (h *adminHandler) triggerIntegrityCheck(w http.ResponseWriter, r *http.Request) {
	jobID, err := h.jobs.Create(r.Context(), "integrity_check", nil, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}
	slog.Info("integrity check job queued", "job_id", jobID)
	writeJSON(w, http.StatusAccepted, map[string]int64{"job_id": jobID})
}

// POST /api/v1/admin/maintenance/device-cleanup
func (h *adminHandler) triggerDeviceCleanup(w http.ResponseWriter, r *http.Request) {
	jobID, err := h.jobs.Create(r.Context(), "device_cleanup", nil, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}
	slog.Info("device cleanup job queued", "job_id", jobID)
	writeJSON(w, http.StatusAccepted, map[string]int64{"job_id": jobID})
}

// GET /api/v1/admin/users
func (h *adminHandler) listUsers(w http.ResponseWriter, r *http.Request) {
	users, err := h.users.ListAll(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	type userSummary struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	out := make([]userSummary, len(users))
	for i, u := range users {
		out[i] = userSummary{ID: u.ID, Name: u.Name}
	}
	writeJSON(w, http.StatusOK, out)
}

// POST /api/v1/admin/users
func (h *adminHandler) createUser(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name         string          `json:"name"`
		IsAdmin      bool            `json:"is_admin"`
		AuthProvider string          `json:"auth_provider"`
		Credentials  json.RawMessage `json:"credentials"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if body.AuthProvider == "" {
		body.AuthProvider = "local"
	}

	plugin, ok := h.authPlugins[body.AuthProvider]
	if !ok {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown auth provider: %s", body.AuthProvider))
		return
	}

	userID, err := plugin.CreateUser(r.Context(), h.db, body.Name, body.Credentials)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if body.IsAdmin {
		if err := h.users.SetAdmin(r.Context(), userID, true); err != nil {
			writeError(w, http.StatusInternalServerError, "db error")
			return
		}
	}

	writeJSON(w, http.StatusCreated, map[string]int64{"id": userID})
}

// DELETE /api/v1/admin/users/:id
func (h *adminHandler) deleteUser(w http.ResponseWriter, r *http.Request) {
	targetID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}

	claims := ClaimsFromContext(r.Context())
	selfID, _ := strconv.ParseInt(claims.Subject, 10, 64)
	if targetID == selfID {
		writeError(w, http.StatusForbidden, "cannot delete your own account")
		return
	}

	affected, err := h.users.Delete(r.Context(), targetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if affected == 0 {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/admin/users/:id/credentials
func (h *adminHandler) changeUserCredentials(w http.ResponseWriter, r *http.Request) {
	targetID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}

	providerName, err := h.users.GetAuthProvider(r.Context(), targetID)
	if err != nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	plugin, ok := h.authPlugins[providerName]
	if !ok {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("auth provider %q does not support credential updates", providerName))
		return
	}

	var body struct {
		Credentials json.RawMessage `json:"credentials"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := plugin.UpdateCredentials(r.Context(), h.db, targetID, body.Credentials); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/users/:id/devices
func (h *adminHandler) listUserDevices(w http.ResponseWriter, r *http.Request) {
	targetID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}
	h.authHandler.writeDeviceList(w, r, targetID)
}

// DELETE /api/v1/admin/users/:id/devices/:deviceId
func (h *adminHandler) revokeUserDevice(w http.ResponseWriter, r *http.Request) {
	targetID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}
	deviceID, err := strconv.ParseInt(chi.URLParam(r, "deviceId"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid device id")
		return
	}

	affected, err := h.devices.Delete(r.Context(), deviceID, targetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if affected == 0 {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	h.guard.Revoke(deviceID, auth.AccessTokenTTL)
	w.WriteHeader(http.StatusNoContent)
}

// DELETE /api/v1/admin/users/:id/devices
func (h *adminHandler) revokeAllUserDevices(w http.ResponseWriter, r *http.Request) {
	targetID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}

	ids, err := h.devices.DeleteAllForUser(r.Context(), targetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	h.guard.RevokeMany(ids, auth.AccessTokenTTL)
	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/collections/:id/users — list user IDs that have access to a collection
func (h *adminHandler) listCollectionUsers(w http.ResponseWriter, r *http.Request) {
	collectionID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid collection id")
		return
	}

	ids, err := h.collections.ListUsersWithAccess(r.Context(), collectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	writeJSON(w, http.StatusOK, ids)
}

// POST /api/v1/admin/collections/:id/users — grant access to a collection for a list of users
func (h *adminHandler) grantUsersCollectionAccess(w http.ResponseWriter, r *http.Request) {
	collectionID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid collection id")
		return
	}

	var body struct {
		UserIDs []int64 `json:"user_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.UserIDs) == 0 {
		writeError(w, http.StatusBadRequest, "user_ids must be a non-empty array")
		return
	}

	if err := h.collections.GrantAccessToUsers(r.Context(), collectionID, body.UserIDs); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/users/:userId/collection_access — list collection IDs the user has access to
func (h *adminHandler) getCollectionAccess(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.ParseInt(chi.URLParam(r, "userId"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}

	ids, err := h.collections.ListAccessForUser(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	writeJSON(w, http.StatusOK, ids)
}

// PATCH /api/v1/admin/users/:userId/collection_access — grant access to collections (duplicates silently ignored)
func (h *adminHandler) grantCollectionAccess(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.ParseInt(chi.URLParam(r, "userId"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}

	var body struct {
		CollectionIDs []int64 `json:"collection_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.CollectionIDs) == 0 {
		writeError(w, http.StatusBadRequest, "collection_ids must be a non-empty array")
		return
	}

	if msg, status := h.collections.ValidateTopLevel(r.Context(), body.CollectionIDs); msg != "" {
		writeError(w, status, msg)
		return
	}

	if err := h.collections.GrantAccess(r.Context(), userID, body.CollectionIDs); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/admin/users/:userId/collection_access — set access to exactly this set of collections
func (h *adminHandler) setCollectionAccess(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.ParseInt(chi.URLParam(r, "userId"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}

	var body struct {
		CollectionIDs []int64 `json:"collection_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if len(body.CollectionIDs) > 0 {
		if msg, status := h.collections.ValidateTopLevel(r.Context(), body.CollectionIDs); msg != "" {
			writeError(w, status, msg)
			return
		}
	}

	// Transaction: atomically replace all access for this user
	tx, err := h.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	defer tx.Rollback(r.Context())

	txCollections := repository.NewCollectionRepository(tx)

	if err := txCollections.DeleteAllAccess(r.Context(), userID); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	if len(body.CollectionIDs) > 0 {
		if err := txCollections.GrantAccess(r.Context(), userID, body.CollectionIDs); err != nil {
			writeError(w, http.StatusInternalServerError, "db error")
			return
		}
	}

	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// DELETE /api/v1/admin/users/:userId/collection_access/:collectionId — revoke access (silent if not present)
func (h *adminHandler) revokeCollectionAccess(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.ParseInt(chi.URLParam(r, "userId"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}
	collectionID, err := strconv.ParseInt(chi.URLParam(r, "collectionId"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid collection id")
		return
	}

	if err := h.collections.RevokeAccess(r.Context(), userID, collectionID); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/admin/media/:id/hide
func (h *adminHandler) hideMediaItem(w http.ResponseWriter, r *http.Request) {
	itemID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	if err := h.media.SetHidden(r.Context(), itemID, true); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DELETE /api/v1/admin/media/:id/hide
func (h *adminHandler) unhideMediaItem(w http.ResponseWriter, r *http.Request) {
	itemID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	if err := h.media.SetHidden(r.Context(), itemID, false); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/jobs/:id/events — SSE progress stream
// TODO: We need to actually properly send down progress updates while jobs are in progress
func (h *adminHandler) jobEvents(w http.ResponseWriter, r *http.Request) {
	jobID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable Nginx/Caddy buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			job, err := h.jobs.GetByID(r.Context(), jobID)
			if err != nil {
				return
			}

			data, _ := json.Marshal(job)
			_, _ = w.Write([]byte("data: "))
			_, _ = w.Write(data)
			_, _ = w.Write([]byte("\n\n"))
			flusher.Flush()

			if job.Status == "done" || job.Status == "failed" {
				return
			}
		}
	}
}
