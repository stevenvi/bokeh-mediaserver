package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/indexer"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
)

type adminHandler struct {
	db       *pgxpool.Pool
	pool     *jobs.Pool
	dataPath string
}

// POST /api/v1/admin/collections
func (h *adminHandler) createCollection(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name     string  `json:"name"`
		Type     string  `json:"type"`
		RootPath string  `json:"root_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	
	body.Name = strings.TrimSpace(body.Name)
	body.Type = strings.TrimSpace(body.Type)
	body.RootPath = strings.TrimSpace(body.RootPath)
	if body.Name == "" || body.Type == "" || body.RootPath == "" {
		writeError(w, http.StatusBadRequest, "name, type, and root_path are required")
		return
	}

	var id int
	err := h.db.QueryRow(r.Context(),
		`INSERT INTO collections (name, type, root_path)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		body.Name, body.Type, body.RootPath,
	).Scan(&id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create collection: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]int{"id": id})
}

// GET /api/v1/admin/collections
func (h *adminHandler) listCollections(w http.ResponseWriter, r *http.Request) {
	rows, err := h.db.Query(r.Context(),
		`SELECT id, name, type, root_path,
		        is_enabled, last_scanned_at, created_at
		 FROM collections ORDER BY name`,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	defer rows.Close()

	var collections []models.Collection
	for rows.Next() {
		var c models.Collection
		if err := rows.Scan(
			&c.ID, &c.Name, &c.Type, &c.RootPath,
			&c.IsEnabled, &c.LastScannedAt, &c.CreatedAt,
		); err != nil {
			slog.Warn("Row scan error", "error", err)
			continue
		}
		collections = append(collections, c)
	}

	writeJSON(w, http.StatusOK, collections)
}

// POST /api/v1/admin/collections/:id/scan
func (h *adminHandler) triggerScan(w http.ResponseWriter, r *http.Request) {
	collectionID, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	// Fetch collection to get root_path
	var rootPath string
	var isEnabled bool
	err = h.db.QueryRow(r.Context(),
		`SELECT COALESCE(root_path, ''), is_enabled FROM collections WHERE id = $1`,
		collectionID,
	).Scan(&rootPath, &isEnabled)
	if err != nil {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}
	if !isEnabled {
		writeError(w, http.StatusBadRequest, "collection is disabled")
		return
	}

	// Prevent duplicate scans
	active, err := jobs.IsActive(r.Context(), h.db, "library_scan", collectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "job check failed")
		return
	}
	if active {
		writeError(w, http.StatusConflict, "a scan is already queued or running for this collection")
		return
	}

	// Create the job row
	relatedType := "collection"
	jobID, err := jobs.Create(r.Context(), h.db, "library_scan", &collectionID, &relatedType)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	// Dispatch to worker pool — returns immediately.
	// Use context.Background() so the scan outlives the HTTP request context.
	scanCtx := context.Background()
	dataPath := h.dataPath
	pool := h.pool
	db := h.db
	h.pool.Submit(func() {
		indexer.RunScan(scanCtx, db, pool, jobID, collectionID, rootPath, dataPath)
	})

	writeJSON(w, http.StatusAccepted, map[string]int{"job_id": jobID})
}

// GET /api/v1/admin/jobs/:id
func (h *adminHandler) getJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	job, err := jobs.GetByID(r.Context(), h.db, jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// GET /api/v1/admin/jobs/:id/events — SSE progress stream
// TODO: We need to actually properly send down progress updates while jobs are in progress
func (h *adminHandler) jobEvents(w http.ResponseWriter, r *http.Request) {
	jobID, err := strconv.Atoi(chi.URLParam(r, "id"))
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
			job, err := jobs.GetByID(r.Context(), h.db, jobID)
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
