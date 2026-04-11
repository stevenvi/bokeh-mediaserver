package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/auth"
	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
)

type adminHandler struct {
	db          *pgxpool.Pool
	guard       *DeviceGuard
	authPlugins map[string]auth.Plugin
	authHandler *authHandler
	dispatcher  *jobs.Dispatcher
	scheduler   *jobs.Scheduler
	mediaPath   string
	dataPath    string
}

// POST /api/v1/admin/collections
func (h *adminHandler) createCollection(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name         string                   `json:"name"`
		Type         constants.CollectionType `json:"type"`
		RelativePath string                   `json:"relative_path"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}

	body.Name = strings.TrimSpace(body.Name)
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

	id, err := repository.CollectionCreate(r.Context(), h.db, body.Name, body.Type, body.RelativePath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create collection: "+err.Error())
		return
	}

	// Auto-queue a collection scan for the newly created collection.
	relatedType := "collection"
	jobID, err := h.dispatcher.Enqueue(r.Context(), "collection_scan", &id, &relatedType)
	if err != nil {
		slog.Warn("auto-queue collection scan for new collection", "collection_id", id, "err", err)
	} else {
		slog.Info("auto-queued collection scan for new collection", "collection_id", id, "job_id", jobID)
	}

	writeJSON(w, http.StatusCreated, map[string]any{"id": id, "scan_job_id": jobID})
}

// DELETE /api/v1/admin/collections/:id
func (h *adminHandler) deleteCollection(w http.ResponseWriter, r *http.Request) {
	collectionID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	// Only top-level collections can be deleted via this endpoint.
	col, err := repository.CollectionGet(r.Context(), h.db, collectionID)
	if err != nil {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}
	if col.ParentCollectionID != nil {
		writeError(w, http.StatusBadRequest, "only top-level collections can be deleted")
		return
	}

	// Gather descendant IDs before deletion so we can clean up their covers.
	descendantIDs, err := repository.CollectionGetDescendantCollectionIDs(r.Context(), h.db, collectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	affected, err := repository.CollectionDelete(r.Context(), h.db, collectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if affected == 0 {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	// Clean up thumbnail images on disk for the top-level collection and all descendants (best-effort).
	for _, id := range append([]int64{collectionID}, descendantIDs...) {
		_ = os.RemoveAll(imaging.CollectionThumbnailDir(h.dataPath, id))
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/collections
func (h *adminHandler) listCollections(w http.ResponseWriter, r *http.Request) {
	collections, err := repository.CollectionsTopLevel(r.Context(), h.db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, collections)
}

// subJobTypes lists job types that are not user-invocable via POST /api/v1/admin/jobs.
var subJobTypes = map[string]bool{
	"scan_photo":            true,
	"scan_video":            true,
	"scan_audio":            true,
	"video_transcode_item":  true,
}

// jobResponse is the consistent shape returned for all job API responses.
type jobResponse struct {
	ID              int64     `json:"id"`
	Type            string    `json:"type"`
	Status          string    `json:"status"`
	Step            int       `json:"step"`
	TotalSteps      int       `json:"total_steps"`
	SupportsSubJobs  bool      `json:"supports_sub_jobs"`
	SubJobsCompleted int64     `json:"subjobs_completed"`
	TotalSubJobs     int64     `json:"total_sub_jobs"`
	SubjobsEnqueued  int       `json:"subjobs_enqueued"`
	RelatedID       *int64    `json:"related_id"`
	RelatedType     *string   `json:"related_type"`
	RelatedName     *string   `json:"related_name"`
	Log             *string   `json:"log"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// enrichJob converts a *models.Job to a jobResponse with meta info resolved.
func (h *adminHandler) enrichJob(r *http.Request, job *models.Job) jobResponse {
	meta, _ := h.dispatcher.GetMeta(job.Type)

	updatedAt := job.QueuedAt
	if job.CompletedAt != nil {
		updatedAt = *job.CompletedAt
	} else if job.StartedAt != nil {
		updatedAt = *job.StartedAt
	}

	resp := jobResponse{
		ID:              job.ID,
		Type:            job.Type,
		Status:          job.Status,
		Step:            job.CurrentStep,
		TotalSteps:      meta.TotalSteps,
		SupportsSubJobs: meta.SupportsSubjobs,
		SubjobsEnqueued: job.SubjobsEnqueued,
		RelatedID:       job.RelatedID,
		RelatedType:     job.RelatedType,
		Log:             job.Log,
		CreatedAt:       job.QueuedAt,
		UpdatedAt:       updatedAt,
	}

	// Resolve related_name
	if job.RelatedID != nil && job.RelatedType != nil && *job.RelatedType == "collection" {
		if coll, err := repository.CollectionGet(r.Context(), h.db, *job.RelatedID); err == nil {
			resp.RelatedName = &coll.Name
		}
	}

	// Sub-job counts
	if meta.SupportsSubjobs {
		completed, total, _ := repository.JobSubJobCounts(r.Context(), h.db, job.ID)
		resp.SubJobsCompleted = completed
		resp.TotalSubJobs = total
	}

	return resp
}

// GET /api/v1/admin/jobs
func (h *adminHandler) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	inactive := q.Get("inactive") == "true"

	page := 1
	if p := q.Get("page"); strings.TrimSpace(p) != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			page = n
		}
	}
	limit := 50
	if l := q.Get("limit"); strings.TrimSpace(l) != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = min(200, n)
		}
	}

	jobs, total, err := repository.JobListTopLevel(r.Context(), h.db, page, limit, inactive)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	responses := make([]jobResponse, len(jobs))
	for i, job := range jobs {
		responses[i] = h.enrichJob(r, job)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"jobs":  responses,
		"total": total,
		"page":  page,
		"limit": limit,
	})
}

// POST /api/v1/admin/jobs
func (h *adminHandler) createJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Type        string  `json:"type"`
		RelatedID   *int64  `json:"related_id"`
		RelatedType *string `json:"related_type"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.Type) == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}

	// Reject sub-job types
	if subJobTypes[body.Type] {
		writeError(w, http.StatusBadRequest, "job type "+body.Type+" is not user-invocable")
		return
	}

	// Check for already-active job
	var active bool
	var err error
	if body.RelatedID != nil {
		active, err = repository.JobIsActive(r.Context(), h.db, body.Type, *body.RelatedID)
	} else {
		active, err = repository.JobIsActiveByType(r.Context(), h.db, body.Type)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "job check failed")
		return
	}
	if active {
		writeError(w, http.StatusConflict, "a job of type "+body.Type+" is already queued or running")
		return
	}

	jobID, err := h.dispatcher.Enqueue(r.Context(), body.Type, body.RelatedID, body.RelatedType)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	slog.Info("job queued via API", "type", body.Type, "job_id", jobID)

	job, err := repository.JobGet(r.Context(), h.db, jobID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch job")
		return
	}
	writeJSON(w, http.StatusCreated, h.enrichJob(r, job))
}

// DELETE /api/v1/admin/collections/:id/derivatives
// Deletes all derived files (variants, DZI tiles) for items in a collection.
// A subsequent scan will regenerate them via process_media.
func (h *adminHandler) deleteDerivatives(w http.ResponseWriter, r *http.Request) {
	collectionID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	hashes, err := repository.MediaItemHashesByCollection(r.Context(), h.db, collectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list items")
		return
	}

	var deleted int
	for _, hash := range hashes {
		itemPath := imaging.ItemDataPath(h.dataPath, hash)
		if err := os.RemoveAll(itemPath); err != nil {
			slog.Info("delete derivatives", "hash", hash, "err", err)
			continue
		}
		deleted++
	}

	// Clear variants_generated_at so the integrity check knows to re-queue
	if err := repository.PhotoClearVariantsGenerated(r.Context(), h.db, collectionID); err != nil {
		slog.Warn("clear variants_generated_at", "collection_id", collectionID, "err", err)
	}

	slog.Info("deleted derivatives", "collection_id", collectionID, "items", deleted)
	writeJSON(w, http.StatusOK, map[string]int{"deleted": deleted})
}

// GET /api/v1/admin/jobs/:id
func (h *adminHandler) getJob(w http.ResponseWriter, r *http.Request) {
	jobID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	job, err := repository.JobGet(r.Context(), h.db, jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	writeJSON(w, http.StatusOK, h.enrichJob(r, job))
}

// GET /api/v1/admin/schedules
func (h *adminHandler) listSchedules(w http.ResponseWriter, r *http.Request) {
	schedules, err := repository.JobScheduleList(r.Context(), h.db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, schedules)
}

// PUT /api/v1/admin/schedules/:name
func (h *adminHandler) upsertSchedule(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "schedule name is required")
		return
	}

	var body struct {
		Cron        string  `json:"cron"`
		Description *string `json:"description"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if body.Cron == "" {
		writeError(w, http.StatusBadRequest, "cron is required")
		return
	}

	if err := repository.JobScheduleUpsert(r.Context(), h.db, name, body.Cron, body.Description); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to upsert schedule")
		return
	}

	if h.scheduler != nil {
		h.scheduler.NotifyReload()
	}

	w.WriteHeader(http.StatusNoContent)
}

// DELETE /api/v1/admin/schedules/:name
func (h *adminHandler) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "schedule name is required")
		return
	}

	deleted, err := repository.JobScheduleDelete(r.Context(), h.db, name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if !deleted {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}

	if h.scheduler != nil {
		h.scheduler.NotifyReload()
	}

	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/admin/collections/{id}/cover
func (h *adminHandler) uploadCollectionCover(w http.ResponseWriter, r *http.Request) {
	collectionID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	if err := r.ParseMultipartForm(10 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart form")
		return
	}

	file, _, err := r.FormFile("cover")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing cover file")
		return
	}
	defer file.Close()

	// Write to temp file for govips to load
	tmp, err := os.CreateTemp("", "bokeh-cover-upload-*.tmp")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create temp file")
		return
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	if _, err := io.Copy(tmp, file); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read upload")
		return
	}
	tmp.Close()

	// Load, crop to square, resize to 400x400, export AVIF + WebP
	if err := imaging.GenerateCollectionThumbnailFromUpload(tmp.Name(), h.dataPath, collectionID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to process cover image")
		return
	}

	if err := repository.CollectionSetManualThumbnail(r.Context(), h.db, collectionID, true); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update collection")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/users
func (h *adminHandler) listUsers(w http.ResponseWriter, r *http.Request) {
	users, err := repository.UsersGet(r.Context(), h.db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	type userSummary struct {
		Name string `json:"name"`
		ID   int64  `json:"id"`
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
		Name            string          `json:"name"`
		AuthProvider    string          `json:"auth_provider"`
		Credentials     json.RawMessage `json:"credentials"`
		IsAdmin         bool            `json:"is_admin"`
		LocalAccessOnly bool            `json:"local_access_only"`
	}
	if !decodeJSON(w, r, &body) {
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

	if err := repository.UserSetAdmin(r.Context(), h.db, userID, body.IsAdmin); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if err := repository.UserSetLocalOnly(r.Context(), h.db, userID, body.LocalAccessOnly); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]int64{"id": userID})
}

// DELETE /api/v1/admin/users/:id
func (h *adminHandler) deleteUser(w http.ResponseWriter, r *http.Request) {
	targetID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	claims := ClaimsFromContext(r.Context())
	selfID, _ := strconv.ParseInt(claims.Subject, 10, 64)
	if targetID == selfID {
		writeError(w, http.StatusForbidden, "cannot delete your own account")
		return
	}

	affected, err := repository.UserDelete(r.Context(), h.db, targetID)
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
	targetID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	providerName, err := repository.UserAuthProvider(r.Context(), h.db, targetID)
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
	if !decodeJSON(w, r, &body) {
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
	targetID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}
	h.authHandler.writeDeviceList(w, r, targetID)
}

// DELETE /api/v1/admin/users/:id/devices/:deviceId
func (h *adminHandler) revokeUserDevice(w http.ResponseWriter, r *http.Request) {
	targetID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}
	deviceID, ok := urlIntParam(w, r, "deviceId")
	if !ok {
		return
	}

	affected, err := repository.DeviceDelete(r.Context(), h.db, deviceID, targetID)
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
	targetID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	ids, err := repository.DevicesDeleteForUser(r.Context(), h.db, targetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	h.guard.RevokeMany(ids, auth.AccessTokenTTL)
	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/collections/:id/users — list user IDs that have access to a collection
func (h *adminHandler) listCollectionUsers(w http.ResponseWriter, r *http.Request) {
	collectionID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	ids, err := repository.CollectionGetUsersWithAccess(r.Context(), h.db, collectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	writeJSON(w, http.StatusOK, ids)
}

// POST /api/v1/admin/collections/:id/users — grant access to a collection for a list of users
func (h *adminHandler) grantUsersCollectionAccess(w http.ResponseWriter, r *http.Request) {
	collectionID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	var body struct {
		UserIDs []int64 `json:"user_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.UserIDs) == 0 {
		writeError(w, http.StatusBadRequest, "user_ids must be a non-empty array")
		return
	}

	if err := repository.CollectionGrantAccessToUsers(r.Context(), h.db, collectionID, body.UserIDs); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/users/:userId/collection_access — list collection IDs the user has access to
func (h *adminHandler) getCollectionAccess(w http.ResponseWriter, r *http.Request) {
	userID, ok := urlIntParam(w, r, "userId")
	if !ok {
		return
	}

	ids, err := repository.CollectionsAccessibleByUser(r.Context(), h.db, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	writeJSON(w, http.StatusOK, ids)
}

// PATCH /api/v1/admin/users/:userId/collection_access — grant access to collections (duplicates silently ignored)
func (h *adminHandler) grantCollectionAccess(w http.ResponseWriter, r *http.Request) {
	userID, ok := urlIntParam(w, r, "userId")
	if !ok {
		return
	}

	var body struct {
		CollectionIDs []int64 `json:"collection_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.CollectionIDs) == 0 {
		writeError(w, http.StatusBadRequest, "collection_ids must be a non-empty array")
		return
	}

	if err := repository.CollectionGrantAccessToUser(r.Context(), h.db, userID, body.CollectionIDs); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/admin/users/:userId/collection_access — set access to exactly this set of collections
func (h *adminHandler) setCollectionAccess(w http.ResponseWriter, r *http.Request) {
	userID, ok := urlIntParam(w, r, "userId")
	if !ok {
		return
	}

	var body struct {
		CollectionIDs []int64 `json:"collection_ids"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}

	// Transaction: atomically replace all access for this user
	tx, err := h.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	defer tx.Rollback(r.Context()) //nolint:errcheck

	if err := repository.UserDeleteAllCollectionAccess(r.Context(), tx, userID); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	if len(body.CollectionIDs) > 0 {
		if err := repository.CollectionGrantAccessToUser(r.Context(), tx, userID, body.CollectionIDs); err != nil {
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
	userID, ok := urlIntParam(w, r, "userId")
	if !ok {
		return
	}
	collectionID, ok := urlIntParam(w, r, "collectionId")
	if !ok {
		return
	}

	if err := repository.UserDeleteCollectionAccess(r.Context(), h.db, userID, collectionID); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/admin/media/:id/hide
func (h *adminHandler) hideMediaItem(w http.ResponseWriter, r *http.Request) {
	itemID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}
	if err := repository.MediaItemSetHidden(r.Context(), h.db, itemID, true); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/directories
// GET /api/v1/admin/directories/*
// Lists subdirectories at the given path within the media root.
// The wildcard path is appended to mediaPath and must resolve within it.
func (h *adminHandler) listDirectories(w http.ResponseWriter, r *http.Request) {
	raw := chi.URLParam(r, "*")
	subPath, err := url.PathUnescape(raw)
	if err != nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	base := filepath.Clean(h.mediaPath)
	full := filepath.Clean(filepath.Join(base, subPath))

	// Reject anything that escapes the media root.
	if full != base && !strings.HasPrefix(full, base+string(filepath.Separator)) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	entries, err := os.ReadDir(full)
	if err != nil {
		slog.Debug("listDirectories", full, err)
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	type DirectoryEntry struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}

	names := make([]DirectoryEntry, 0)
	for _, entry := range entries {
		// Use os.Stat to follow symlinks when deciding if the target is a directory.
		info, err := os.Stat(filepath.Join(full, entry.Name()))
		if err != nil {
			continue
		}
		if info.IsDir() {
			// Try to guess the file type in this folder
			entries2, err2 := os.ReadDir(filepath.Join(full, entry.Name()))
			if err2 != nil {
				slog.Error("Cannot read directory, will not return it to user", "directory", entry.Name())
				continue
			}

			type_guess := constants.CollectionTypeMovie
			for _, entry2 := range entries2 {
				if entry2.Type().IsRegular() {
					mimeType, ok := constants.SupportedExtensions[filepath.Ext(entry2.Name())]
					if ok {
						if strings.HasPrefix(mimeType, "image") {
							type_guess = constants.CollectionTypePhoto
							break
						} else if strings.HasPrefix(mimeType, "audio") {
							type_guess = constants.CollectionTypeMusic
							break
						} else if strings.HasPrefix(mimeType, "video") {
							type_guess = constants.CollectionTypeMovie
							break
						}
					}
				}
			}
			names = append(names, DirectoryEntry{Name: entry.Name(), Type: type_guess.String()})
		}
	}

	writeJSON(w, http.StatusOK, names)
}

// DELETE /api/v1/admin/media/:id/hide
func (h *adminHandler) unhideMediaItem(w http.ResponseWriter, r *http.Request) {
	itemID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}
	if err := repository.MediaItemSetHidden(r.Context(), h.db, itemID, false); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/admin/jobs/:id/events — SSE progress stream
// TODO: We need to actually properly send down progress updates while jobs are in progress
func (h *adminHandler) jobEvents(w http.ResponseWriter, r *http.Request) {
	jobID, ok := urlIntParam(w, r, "id")
	if !ok {
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
			job, err := repository.JobGet(r.Context(), h.db, jobID)
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
