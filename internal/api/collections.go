package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type collectionsHandler struct {
	db utils.DBTX
}

// GET /api/v1/collections — top-level enabled collections the user has access to.
// Users see only those in collection_access.
func (h *collectionsHandler) list(w http.ResponseWriter, r *http.Request) {
	collections, err := repository.CollectionsListAccessibleByUser(r.Context(), h.db, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	writeJSON(w, http.StatusOK, collections)
}

// GET /api/v1/collections/:id
func (h *collectionsHandler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	c, err := repository.CollectionGetForUser(r.Context(), h.db, id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	writeJSON(w, http.StatusOK, c)
}

// requireAccess checks that the collection exists, is enabled, and the requesting user
// has access. Returns false (and writes the error response) if any check fails.
func (h *collectionsHandler) requireAccess(w http.ResponseWriter, r *http.Request, collectionID int64) bool {
	ok, err := repository.CollectionExistsAndAccessible(r.Context(), h.db, collectionID, userIDFromRequest(r))
	if err != nil || !ok {
		writeError(w, http.StatusNotFound, "collection not found")
		return false
	}
	return true
}

// GET /api/v1/collections/:id/collections — direct children
func (h *collectionsHandler) listChildren(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	if !h.requireAccess(w, r, id) {
		return
	}

	collections, err := repository.CollectionGetChildCollections(r.Context(), h.db, id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	// Extract date prefixes from collection names
	for i := range collections {
		strippedName, date := utils.ExtractDatePrefix(collections[i].Name)
		if date != nil {
			collections[i].Name = strippedName
			collections[i].Date = date
		}
	}

	writeJSON(w, http.StatusOK, collections)
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
