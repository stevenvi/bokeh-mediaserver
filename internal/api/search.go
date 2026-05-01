package api

import (
	"net/http"
	"strings"

	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type searchHandler struct {
	db utils.DBTX
}

const (
	searchDefaultLimit = 50
	searchMaxLimit     = 200
)

// parseSearchParams reads q (required), offset (default 0), limit (default 50,
// max 200) from the request. Writes a 400 and returns ok=false on missing/empty q.
func parseSearchParams(w http.ResponseWriter, r *http.Request) (repository.SearchParams, bool) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		writeError(w, http.StatusBadRequest, "missing required query parameter: q")
		return repository.SearchParams{}, false
	}
	offset := queryInt(r, "offset", 0)
	if offset < 0 {
		offset = 0
	}
	limit := queryInt(r, "limit", searchDefaultLimit)
	if limit <= 0 {
		limit = searchDefaultLimit
	}
	if limit > searchMaxLimit {
		limit = searchMaxLimit
	}
	return repository.SearchParams{Query: q, Offset: offset, Limit: limit}, true
}

// GET /api/v1/search/videos
func (h *searchHandler) searchVideos(w http.ResponseWriter, r *http.Request) {
	p, ok := parseSearchParams(w, r)
	if !ok {
		return
	}
	movies, homeMovies, err := repository.SearchVideos(r.Context(), h.db, userIDFromRequest(r), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"video:movie":      movies,
		"video:home_movie": homeMovies,
		"offset":           p.Offset,
		"limit":            p.Limit,
	})
}

// GET /api/v1/search/photos
func (h *searchHandler) searchPhotos(w http.ResponseWriter, r *http.Request) {
	p, ok := parseSearchParams(w, r)
	if !ok {
		return
	}
	items, err := repository.SearchPhotos(r.Context(), h.db, userIDFromRequest(r), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	for i := range items {
		items[i].Ordinal = int64(p.Offset + i)
		items[i].RemapCameraModel()
		items[i].RemapLensModel()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"offset": p.Offset,
		"limit":  p.Limit,
	})
}

// GET /api/v1/search/audio/artists
func (h *searchHandler) searchAudioArtists(w http.ResponseWriter, r *http.Request) {
	p, ok := parseSearchParams(w, r)
	if !ok {
		return
	}
	artists, shows, err := repository.SearchAudioArtists(r.Context(), h.db, userIDFromRequest(r), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"artists": artists,
		"shows":   shows,
		"offset":  p.Offset,
		"limit":   p.Limit,
	})
}

// GET /api/v1/search/audio/albums
func (h *searchHandler) searchAudioAlbums(w http.ResponseWriter, r *http.Request) {
	p, ok := parseSearchParams(w, r)
	if !ok {
		return
	}
	albums, err := repository.SearchAudioAlbums(r.Context(), h.db, userIDFromRequest(r), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"albums": albums,
		"offset": p.Offset,
		"limit":  p.Limit,
	})
}

// GET /api/v1/search/audio/tracks
func (h *searchHandler) searchAudioTracks(w http.ResponseWriter, r *http.Request) {
	p, ok := parseSearchParams(w, r)
	if !ok {
		return
	}
	tracks, err := repository.SearchAudioTracks(r.Context(), h.db, userIDFromRequest(r), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tracks": tracks,
		"offset": p.Offset,
		"limit":  p.Limit,
	})
}
