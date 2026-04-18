package api

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
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
		strippedName, date, _ := utils.ExtractDatePrefix(collections[i].Name)
		if date != nil {
			collections[i].Name = strippedName
			collections[i].Date = date
		}
	}

	writeJSON(w, http.StatusOK, collections)
}

// GET /api/v1/collections/:id/items — paginated media items
func (h *collectionsHandler) listItems(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	userID := userIDFromRequest(r)

	// Fetch collection metadata (also enforces access)
	col, err := repository.CollectionGetForUser(r.Context(), h.db, id, userID)
	if err != nil {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	page := queryInt(r, "page", 1)
	pageSize := queryInt(r, "page_size", 200)
	if pageSize > 1000 {
		pageSize = 1000
	}
	offset := (page - 1) * pageSize

	var items []models.MediaItemView

	if col.Type == constants.CollectionTypeMovie || col.Type == constants.CollectionTypeHomeMovie {
		// Video collections use a dedicated query with JOIN to video_metadata and video_bookmarks.
		items, err = repository.MediaItemVideosByCollection(r.Context(), h.db, id, userID, col.Type, pageSize+1, offset)
	} else {
		// Fetch one extra row to determine whether there's a next page.
		items, err = repository.MediaItemsByCollectionPaginated(r.Context(), h.db, id, userID, pageSize+1, offset)
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	var nextPage *int
	if len(items) > pageSize {
		items = items[:pageSize]
		np := page + 1
		nextPage = &np
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items":     items,
		"page":      page,
		"next_page": nextPage,
		"page_size": pageSize,
	})
}

// slideshowCursor encodes the position for keyset pagination in a slideshow.
type slideshowCursor struct {
	CreatedAt *time.Time `json:"t,omitempty"`
	Direction string     `json:"d"` // "f" (forward) or "b" (backward)
	ID        int64      `json:"i"`
}

func encodeSlideshowCursor(item models.SlideshowItem, direction string) string {
	b, _ := json.Marshal(slideshowCursor{Direction: direction, CreatedAt: item.CreatedAt, ID: item.ID})
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
	if c.Direction != "f" && c.Direction != "b" {
		return slideshowCursor{}, false
	}
	return c, true
}

// GET /api/v1/collections/:id/slideshow
// Returns a paginated keyset view of photo items ordered by created_at.
// Query params:
//
//	order     = asc (default) | desc
//	recursive = true (default) | false
//	start     = media item ID to begin at (mutually exclusive with cursor)
//	page_size = 1–200, default 50
//	cursor    = opaque token from previous response's next_cursor or prev_cursor field
func (h *collectionsHandler) slideshow(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	if !h.requireAccess(w, r, id) {
		return
	}

	orderParam := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("order")))
	ascending := orderParam != "desc"

	recursiveParam := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("recursive")))
	recursive := recursiveParam != "false"

	pageSize := queryInt(r, "page_size", 200)
	if pageSize > 1000 {
		pageSize = 1000
	}

	rawCursor := r.URL.Query().Get("cursor")
	rawStart := r.URL.Query().Get("start")
	rawStartDate := r.URL.Query().Get("start_date")
	// These three are mutually exclusive. This logic is goofy but works well enough.
	exclusiveCount := 0
	if rawCursor != "" {
		exclusiveCount++
	}
	if rawStart != "" {
		exclusiveCount++
	}
	if rawStartDate != "" {
		exclusiveCount++
	}
	if exclusiveCount > 1 {
		writeError(w, http.StatusBadRequest, "cursor, start, and start_date are mutually exclusive")
		return
	}

	var hasCursor bool
	var cursor slideshowCursor
	var includeCursor bool // value cursor points at should be included in results (inclusive vs exclusive)
	backward := false

	if rawCursor != "" {
		cursor, hasCursor = decodeSlideshowCursor(rawCursor)
		if !hasCursor {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		backward = cursor.Direction == "b"
	} else if rawStart != "" {
		startID, err := strconv.ParseInt(rawStart, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid start")
			return
		}
		pos, err := repository.SlideshowItemPosition(r.Context(), h.db, startID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "db error")
			return
		}
		if pos == nil {
			writeError(w, http.StatusNotFound, "start item not found")
			return
		}
		// Validate the start item belongs to this collection (or a descendant if recursive)
		if recursive {
			isDesc, err := repository.CollectionIsDescendantOf(r.Context(), h.db, id, pos.CollectionID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "db error")
				return
			}
			if !isDesc {
				writeError(w, http.StatusBadRequest, "start item not in this collection")
				return
			}
		} else if pos.CollectionID != id {
			writeError(w, http.StatusBadRequest, "start item not in this collection")
			return
		}
		cursor = slideshowCursor{Direction: "f", CreatedAt: pos.CreatedAt, ID: pos.ID}
		hasCursor = true
		includeCursor = true
	} else if rawStartDate != "" {
		t, err := time.Parse("2006-01", rawStartDate)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid start_date (expected YYYY-MM)")
			return
		}
		if ascending {
			cursor = slideshowCursor{Direction: "f", CreatedAt: &t, ID: 0}
			includeCursor = true
		} else {
			// First instant of the next month with max ID — strict < gives all items in the target month and earlier.
			nextMonth := t.AddDate(0, 1, 0)
			cursor = slideshowCursor{Direction: "f", CreatedAt: &nextMonth, ID: math.MaxInt64}
		}
		hasCursor = true
	}

	// For backward cursors, flip the sort direction for the query.
	queryAscending := ascending
	if backward {
		queryAscending = !ascending
	}

	rows, err := repository.SlideshowItems(r.Context(), h.db, repository.SlideshowQuery{
		CollectionID:  id,
		PageSize:      pageSize,
		Ascending:     queryAscending,
		Recursive:     recursive,
		HasCursor:     hasCursor,
		CursorTime:    cursor.CreatedAt,
		CursorID:      cursor.ID,
		IncludeCursor: includeCursor,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	hasMore := len(rows) > pageSize
	if hasMore {
		rows = rows[:pageSize]
	}

	// For backward cursors, reverse the results so they're in the original sort order.
	if backward {
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
	}

	var nextCursor, prevCursor *string
	if len(rows) > 0 {
		if backward {
			// We fetched backward: hasMore means there are earlier items (more prev pages).
			// The reversed first item is the earliest — next_cursor always points forward from the last item.
			nc := encodeSlideshowCursor(rows[len(rows)-1], "f")
			nextCursor = &nc
			if hasMore {
				pc := encodeSlideshowCursor(rows[0], "b")
				prevCursor = &pc
			}
		} else {
			// We fetched forward: hasMore means there are later items (more next pages).
			if hasMore {
				nc := encodeSlideshowCursor(rows[len(rows)-1], "f")
				nextCursor = &nc
			}
			// prev_cursor: if we used a cursor (not the very first page), there are previous items.
			if hasCursor && !includeCursor {
				pc := encodeSlideshowCursor(rows[0], "b")
				prevCursor = &pc
			}
			// When start is used, also emit prev_cursor so client can seek backward from start.
			if includeCursor && len(rows) > 0 {
				pc := encodeSlideshowCursor(rows[0], "b")
				prevCursor = &pc
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items":       rows,
		"next_cursor": nextCursor,
		"prev_cursor": prevCursor,
		"page_size":   pageSize,
	})
}

// GET /api/v1/collections/:id/slideshow/metadata
// Returns per-month item counts for the slideshow year scrollbar.
// Query params:
//
//	recursive = true (default) | false
func (h *collectionsHandler) slideshowMetadata(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	if !h.requireAccess(w, r, id) {
		return
	}

	recursiveParam := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("recursive")))
	recursive := recursiveParam != "false"

	counts, err := repository.SlideshowMetadata(r.Context(), h.db, id, recursive)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"months": counts,
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
