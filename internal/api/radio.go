package api

import (
	"net/http"

	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type radioHandler struct {
	db        utils.DBTX
	dataPath  string
	mediaPath string
}

// GET /api/v1/collections/{collectionId}/shows
func (h *radioHandler) listShows(w http.ResponseWriter, r *http.Request) {
	collectionID, ok := urlIntParam(w, r, "collectionId")
	if !ok {
		return
	}
	userID := userIDFromRequest(r)

	shows, err := repository.ShowsInCollection(r.Context(), h.db, collectionID, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list shows")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"shows": shows})
}

// GET /api/v1/collections/{collectionId}/shows/{artistId}/episodes
func (h *radioHandler) listShowEpisodes(w http.ResponseWriter, r *http.Request) {
	collectionID, ok := urlIntParam(w, r, "collectionId")
	if !ok {
		return
	}
	artistID, ok := urlIntParam(w, r, "artistId")
	if !ok {
		return
	}
	userID := userIDFromRequest(r)

	show, err := repository.ArtistGet(r.Context(), h.db, artistID)
	if err != nil {
		writeError(w, http.StatusNotFound, "show not found")
		return
	}

	episodes, err := repository.ShowEpisodesByArtist(r.Context(), h.db, artistID, collectionID, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list episodes")
		return
	}

	bookmark, err := repository.AudioShowBookmarkGet(r.Context(), h.db, userID, artistID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load bookmark")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"show":     show,
		"episodes": episodes,
		"bookmark": bookmark,
	})
}

// PUT /api/v1/audio-shows/{artistId}/bookmark
func (h *radioHandler) upsertShowBookmark(w http.ResponseWriter, r *http.Request) {
	artistID, ok := urlIntParam(w, r, "artistId")
	if !ok {
		return
	}
	userID := userIDFromRequest(r)

	var body struct {
		MediaItemID     int64 `json:"media_item_id"`
		PositionSeconds int   `json:"position_seconds"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}

	if err := repository.AudioShowBookmarkUpsert(r.Context(), h.db, userID, artistID, body.MediaItemID, body.PositionSeconds); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to save bookmark")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
