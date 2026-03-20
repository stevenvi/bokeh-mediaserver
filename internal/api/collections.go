package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
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

	items, err := h.collections.GetSlideshowItems(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
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
