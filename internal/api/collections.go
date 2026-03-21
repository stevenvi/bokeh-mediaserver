package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
)

type collectionsHandler struct {
	collections *repository.CollectionRepository
	media       *repository.MediaItemRepository
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

	collections, err := h.collections.ListAccessibleByUser(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
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

	userID, err := strconv.ParseInt(ClaimsFromContext(r.Context()).Subject, 10, 64)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "invalid token subject")
		return
	}

	c, err := h.collections.GetByIDForUser(r.Context(), id, userID)
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

	exists, err := h.collections.ExistsEnabled(r.Context(), id)
	if err != nil || !exists {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	userID, _ := strconv.ParseInt(ClaimsFromContext(r.Context()).Subject, 10, 64)
	if ok, _ := h.collections.HasAccessToCollection(r.Context(), id, userID); !ok {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	collections, err := h.collections.ListChildren(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
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

	exists, err := h.collections.ExistsEnabled(r.Context(), id)
	if err != nil || !exists {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	page := queryInt(r, "page", 1)
	pageSize := queryInt(r, "page_size", 50)
	if pageSize > 200 {
		pageSize = 200
	}
	offset := (page - 1) * pageSize

	userID, _ := strconv.ParseInt(ClaimsFromContext(r.Context()).Subject, 10, 64)
	items, err := h.media.ListByCollectionPaginated(r.Context(), id, userID, pageSize, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items":     items,
		"page":      page,
		"page_size": pageSize,
	})
}

// slideshowCursor encodes the position of the last item returned in a slideshow page.
type slideshowCursor struct {
	CreatedAt *time.Time `json:"t,omitempty"`
	ID        int64      `json:"i"`
}

func encodeSlideshowCursor(item models.SlideshowItem) string {
	b, _ := json.Marshal(slideshowCursor{CreatedAt: item.CreatedAt, ID: item.ID})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeSlideshowCursor(s string) (slideshowCursor, bool) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return slideshowCursor{}, false
	}
	var c slideshowCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return slideshowCursor{}, false
	}
	return c, true
}

// GET /api/v1/collections/:id/slideshow
// Returns a paginated keyset view of all descendant photo items ordered by created_at.
// Query params:
//
//	order     = asc (default) | desc
//	page_size = 1–200, default 50
//	cursor    = opaque token from previous response's next_cursor field
func (h *collectionsHandler) slideshow(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	exists, err := h.collections.ExistsEnabled(r.Context(), id)
	if err != nil || !exists {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	userID, _ := strconv.ParseInt(ClaimsFromContext(r.Context()).Subject, 10, 64)
	if ok, _ := h.collections.HasAccessToCollection(r.Context(), id, userID); !ok {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	orderParam := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("order")))
	ascending := orderParam != "desc"

	pageSize := queryInt(r, "page_size", 50)
	if pageSize > 200 {
		pageSize = 200
	}

	var hasCursor bool
	var cursor slideshowCursor
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		cursor, hasCursor = decodeSlideshowCursor(raw)
		if !hasCursor {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
	}

	// Repository fetches pageSize+1; the extra row signals that a next page exists.
	rows, err := h.collections.GetSlideshowItems(r.Context(), id, pageSize, ascending, hasCursor, cursor.CreatedAt, cursor.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	var nextCursor *string
	if len(rows) > pageSize {
		rows = rows[:pageSize]
		s := encodeSlideshowCursor(rows[pageSize-1])
		nextCursor = &s
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items":       rows,
		"next_cursor": nextCursor,
		"page_size":   pageSize,
	})
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
