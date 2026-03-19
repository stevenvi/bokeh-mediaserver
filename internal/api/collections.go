package api

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
)

type collectionsHandler struct {
	db *pgxpool.Pool
}

// GET /api/v1/collections — top-level enabled collections the user has access to.
// Users see only those in collection_access.
func (h *collectionsHandler) list(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, err := strconv.ParseInt(claims.Subject, 10, 64)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "invalid token subject")
		return
	}

	rows, err := h.db.Query(r.Context(), 
		`SELECT c.id, c.name, c.type
		 FROM collections c
		 JOIN collection_access ca ON ca.collection_id = c.id AND ca.user_id = $1
		 WHERE c.parent_collection_id IS NULL AND c.is_enabled = true
		 ORDER BY c.name`, 
		 userID,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	defer rows.Close()

	type collectionSummary struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
		Type string `json:"type"`
	}

	var collections []collectionSummary
	for rows.Next() {
		var c collectionSummary
		if err := rows.Scan(&c.ID, &c.Name, &c.Type); err != nil {
			slog.Warn("Row scan error", "error", err)
			continue
		}
		collections = append(collections, c)
	}

	writeJSON(w, http.StatusOK, collections)
}

// GET /api/v1/collections/:id
func (h *collectionsHandler) get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	var c models.Collection
	err = h.db.QueryRow(r.Context(),
		`SELECT id, parent_collection_id, name, type
		 FROM collections WHERE id = $1 and is_enabled = true`,
		id,
	).Scan(&c.ID, &c.ParentCollectionID, &c.Name, &c.Type)
	if err != nil {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	writeJSON(w, http.StatusOK, c)
}

// GET /api/v1/collections/:id/collections — direct children
func (h *collectionsHandler) listChildren(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	rows, err := h.db.Query(r.Context(),
		`SELECT id, parent_collection_id, name, type
		 FROM collections
		 WHERE parent_collection_id = $1 AND is_enabled = true
		 ORDER BY name`,
		id,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	defer rows.Close()

	var collections []models.Collection
	for rows.Next() {
		var c models.Collection
		if err := rows.Scan(&c.ID, &c.ParentCollectionID, &c.Name, &c.Type); err != nil {
			slog.Warn("Row scan error", "error", err)
			continue
		}
		collections = append(collections, c)
	}

	writeJSON(w, http.StatusOK, collections)
}

// GET /api/v1/collections/:id/items — paginated media items
func (h *collectionsHandler) listItems(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	page := queryInt(r, "page", 1)
	pageSize := queryInt(r, "page_size", 50)
	if pageSize > 200 {
		pageSize = 200
	}
	offset := (page - 1) * pageSize

	rows, err := h.db.Query(r.Context(),
		`SELECT id, collection_id, title, mime_type, ordinal
		 FROM media_items
		 WHERE collection_id = $1 AND missing_since IS NULL
		 ORDER BY ordinal ASC NULLS LAST, title ASC
		 LIMIT $2 OFFSET $3`,
		id, pageSize, offset,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	defer rows.Close()

	var items []models.MediaItem
	for rows.Next() {
		var item models.MediaItem
		if err := rows.Scan(&item.ID, &item.CollectionID, &item.Title, &item.MimeType, &item.Ordinal); err != nil {
			slog.Warn("Row scan error", "error", err)
			continue
		}
		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items":     items,
		"page":      page,
		"page_size": pageSize,
	})
}

// GET /api/v1/collections/:id/slideshow
// Returns all descendant photo items via recursive CTE, ordered by taken_at.
// TODO: Allow random order as well
// TODO: Allow pagination of slideshow if possible
// TODO: What is this payload size going to look like if you have a huge collection?
func (h *collectionsHandler) slideshow(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	rows, err := h.db.Query(r.Context(),
		`WITH RECURSIVE collection_tree AS (
		     SELECT id FROM collections WHERE id = $1
		     UNION ALL
		     SELECT c.id FROM collections c
		     INNER JOIN collection_tree ct ON c.parent_collection_id = ct.id
		 )
		 SELECT
		     mi.id,
		     mi.title,
		     mi.mime_type,
		     pm.taken_at,
		     pm.placeholder,
		     pm.width_px,
		     pm.height_px
		 FROM media_items mi
		 JOIN photo_metadata pm ON pm.media_item_id = mi.id
		 WHERE mi.collection_id = ANY(SELECT id FROM collection_tree)
		   AND mi.missing_since IS NULL
		 ORDER BY pm.taken_at ASC NULLS LAST, mi.id ASC`,
		id,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	defer rows.Close()

	type slideshowItem struct {
		ID          int64  `json:"id"`
		Title       string `json:"title"`
		MimeType    string `json:"mime_type"`
		TakenAt     any    `json:"taken_at"`
		Placeholder any    `json:"placeholder"`
		WidthPx     any    `json:"width_px"`
		HeightPx    any    `json:"height_px"`
	}

	var items []slideshowItem
	for rows.Next() {
		var item slideshowItem
		if err := rows.Scan(
			&item.ID, &item.Title, &item.MimeType,
			&item.TakenAt, &item.Placeholder, &item.WidthPx, &item.HeightPx,
		); err != nil {
			slog.Warn("Row scan error", "error", err)
			continue
		}
		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, items)
}

func queryInt(r *http.Request, key string, fallback int) int {
	v := r.URL.Query().Get(key)
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return fallback
	}
	return n
}
