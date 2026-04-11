package api

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type musicHandler struct {
	db        utils.DBTX
	dataPath  string
	mediaPath string
}

// GET /api/v1/collections/{collectionId}/artists
func (h *musicHandler) listArtists(w http.ResponseWriter, r *http.Request) {
	collectionID, ok := urlIntParam(w, r, "collectionId")
	if !ok {
		return
	}

	page, pageSize := parsePagination(r, 100, 500)
	offset := (page - 1) * pageSize
	search := r.URL.Query().Get("search")

	artists, total, err := repository.ArtistsInCollection(r.Context(), h.db, collectionID, pageSize, offset, search)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list artists")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"artists":     artists,
		"total_count": total,
		"page":        page,
		"page_size":   pageSize,
	})
}

// GET /api/v1/collections/{collectionId}/artists/{artistId}/albums
func (h *musicHandler) listArtistAlbums(w http.ResponseWriter, r *http.Request) {
	collectionID, ok := urlIntParam(w, r, "collectionId")
	if !ok {
		return
	}

	artistID, ok := urlIntParam(w, r, "artistId")
	if !ok {
		return
	}

	artist, err := repository.ArtistGet(r.Context(), h.db, artistID)
	if err != nil {
		writeError(w, http.StatusNotFound, "artist not found")
		return
	}

	albums, err := repository.ArtistGetAlbums(r.Context(), h.db, artistID, collectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list albums")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"artist": artist,
		"albums": albums,
	})
}

// GET /api/v1/collections/{collectionId}/albums/{albumId}/tracks
func (h *musicHandler) listAlbumTracks(w http.ResponseWriter, r *http.Request) {
	albumID, ok := urlIntParam(w, r, "albumId")
	if !ok {
		return
	}
	userID := userIDFromRequest(r)

	album, err := repository.AlbumGet(r.Context(), h.db, albumID)
	if err != nil {
		writeError(w, http.StatusNotFound, "album not found")
		return
	}

	tracks, err := repository.AudioTracksByAlbum(r.Context(), h.db, albumID, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list tracks")
		return
	}

	var totalDuration float64
	var discCount int16
	for _, t := range tracks {
		if t.DurationSeconds != nil {
			totalDuration += *t.DurationSeconds
		}
		if t.DiscNumber != nil && *t.DiscNumber > discCount {
			discCount = *t.DiscNumber
		}
	}
	if discCount == 0 {
		discCount = 1
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"album":          album,
		"tracks":         tracks,
		"total_duration": totalDuration,
		"disc_count":     discCount,
	})
}

// GET /audio/{id}/stream
func (h *musicHandler) stream(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}
	userID := userIDFromRequest(r)

	relativePath, mimeType, err := repository.MediaItemGetAudioStreamInfo(r.Context(), h.db, id, userID)
	if err != nil {
		writeError(w, http.StatusNotFound, "track not found")
		return
	}

	w.Header().Set("Content-Type", mimeType)
	http.ServeFile(w, r, filepath.Join(h.mediaPath, relativePath))
}

// GET /images/artists/{id}/cover
func (h *musicHandler) serveArtistImage(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	// ?format= query param overrides Accept header negotiation.
	var accept string
	switch r.URL.Query().Get("format") {
	case "webp":
		accept = "image/webp"
	case "jpeg", "jpg":
		accept = "image/jpeg"
	default:
		accept = r.Header.Get("Accept")
	}
	acceptsAVIF := strings.Contains(accept, "image/avif")

	if acceptsAVIF {
		avifPath := imaging.ArtistThumbnailPath(h.dataPath, id, "avif")
		if fileExists(avifPath) {
			w.Header().Set("Content-Type", "image/avif")
			http.ServeFile(w, r, avifPath)
			return
		}
	}

	webpPath := imaging.ArtistThumbnailPath(h.dataPath, id, "webp")
	if fileExists(webpPath) {
		w.Header().Set("Content-Type", "image/webp")
		http.ServeFile(w, r, webpPath)
		return
	}

	avifPath := imaging.ArtistThumbnailPath(h.dataPath, id, "avif")
	if fileExists(avifPath) {
		w.Header().Set("Content-Type", "image/avif")
		http.ServeFile(w, r, avifPath)
		return
	}

	writeError(w, http.StatusNotFound, "artist image not found")
}

// GET /images/albums/{albumId}/thumb
func (h *musicHandler) serveAlbumThumbnail(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "albumId")
	if !ok {
		return
	}

	// ?format= query param overrides Accept header negotiation.
	var accept string
	switch r.URL.Query().Get("format") {
	case "webp":
		accept = "image/webp"
	case "jpeg", "jpg":
		accept = "image/jpeg"
	default:
		accept = r.Header.Get("Accept")
	}
	acceptsAVIF := strings.Contains(accept, "image/avif")

	if acceptsAVIF {
		avifPath := imaging.AlbumThumbnailPath(h.dataPath, id, "avif")
		if fileExists(avifPath) {
			w.Header().Set("Content-Type", "image/avif")
			http.ServeFile(w, r, avifPath)
			return
		}
	}

	webpPath := imaging.AlbumThumbnailPath(h.dataPath, id, "webp")
	if fileExists(webpPath) {
		w.Header().Set("Content-Type", "image/webp")
		http.ServeFile(w, r, webpPath)
		return
	}

	avifPath := imaging.AlbumThumbnailPath(h.dataPath, id, "avif")
	if fileExists(avifPath) {
		w.Header().Set("Content-Type", "image/avif")
		http.ServeFile(w, r, avifPath)
		return
	}

	writeError(w, http.StatusNotFound, "album thumbnail not found")
}

// GET /images/albums/{albumId}/cover
// Serves the 1280px album cover. Stored as AVIF; converted on-the-fly for clients
// that don't accept AVIF.
func (h *musicHandler) serveAlbumCover(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "albumId")
	if !ok {
		return
	}

	avifPath := imaging.AlbumCoverPath(h.dataPath, id, "avif")
	if !fileExists(avifPath) {
		writeError(w, http.StatusNotFound, "album cover not found")
		return
	}

	// ?format= query param overrides Accept header negotiation.
	var accept string
	switch r.URL.Query().Get("format") {
	case "webp":
		accept = "image/webp"
	case "jpeg", "jpg":
		accept = "image/jpeg"
	default:
		accept = r.Header.Get("Accept")
	}

	if strings.Contains(accept, "image/avif") {
		w.Header().Set("Content-Type", "image/avif")
		http.ServeFile(w, r, avifPath)
		return
	}

	if strings.Contains(accept, "image/webp") {
		webpBytes, err := imaging.GenerateWebP(avifPath)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "image conversion failed")
			return
		}
		w.Header().Set("Content-Type", "image/webp")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(webpBytes)
		return
	}

	jpegBytes, err := imaging.GenerateJPEG(avifPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "image conversion failed")
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(jpegBytes)
}

// POST /api/v1/admin/artists/{id}/image
func (h *musicHandler) uploadArtistImage(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	// Verify artist exists
	_, err := repository.ArtistGet(r.Context(), h.db, id)
	if err != nil {
		writeError(w, http.StatusNotFound, "artist not found")
		return
	}

	if err := r.ParseMultipartForm(20 << 20); err != nil { // 20 MB limit
		writeError(w, http.StatusBadRequest, "invalid multipart form")
		return
	}

	file, _, err := r.FormFile("image")
	if err != nil {
		writeError(w, http.StatusBadRequest, "image file required")
		return
	}
	defer file.Close()

	tmp, err := os.CreateTemp("", "bokeh-artist-upload-*.tmp")
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

	if err := imaging.GenerateArtistThumbnailFromUpload(tmp.Name(), h.dataPath, id); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to process image")
		return
	}

	if err := repository.ArtistSetManualThumbnail(r.Context(), h.db, id, true); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update artist")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// DELETE /api/v1/admin/artists/{id}/image
func (h *musicHandler) deleteArtistImage(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	// Remove files
	for _, ext := range []string{"avif", "webp"} {
		path := imaging.ArtistThumbnailPath(h.dataPath, id, ext)
		_ = os.Remove(path)
	}

	if err := repository.ArtistSetManualThumbnail(r.Context(), h.db, id, false); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update artist")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// POST /api/v1/admin/albums/{albumId}/cover
func (h *musicHandler) uploadAlbumCover(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "albumId")
	if !ok {
		return
	}

	_, err := repository.AlbumGet(r.Context(), h.db, id)
	if err != nil {
		writeError(w, http.StatusNotFound, "album not found")
		return
	}

	if err := r.ParseMultipartForm(20 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart form")
		return
	}

	file, _, err := r.FormFile("image")
	if err != nil {
		writeError(w, http.StatusBadRequest, "image file required")
		return
	}
	defer file.Close()

	tmp, err := os.CreateTemp("", "bokeh-album-upload-*.tmp")
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

	if err := imaging.GenerateAlbumThumbnailFromUpload(tmp.Name(), h.dataPath, id); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to process image")
		return
	}
	if err := imaging.GenerateAlbumCoverFromUpload(tmp.Name(), h.dataPath, id); err != nil {
		slog.Warn("generate album cover (1280px)", "album_id", id, "err", err)
	}

	if err := repository.AlbumSetManualCover(r.Context(), h.db, id, true); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update album")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// DELETE /api/v1/admin/albums/{albumId}/cover
func (h *musicHandler) deleteAlbumCover(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "albumId")
	if !ok {
		return
	}

	for _, ext := range []string{"avif", "webp"} {
		_ = os.Remove(imaging.AlbumThumbnailPath(h.dataPath, id, ext))
	}
	_ = os.Remove(imaging.AlbumCoverPath(h.dataPath, id, "avif"))

	if err := repository.AlbumSetManualCover(r.Context(), h.db, id, false); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update album")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func parsePagination(r *http.Request, defaultSize, maxSize int) (page, pageSize int) {
	page = 1
	pageSize = defaultSize
	if p, err := strconv.ParseInt(r.URL.Query().Get("page"), 10, 64); err == nil && p > 0 {
		page = int(p)
	}
	if ps, err := strconv.ParseInt(r.URL.Query().Get("page_size"), 10, 64); err == nil && ps > 0 {
		pageSize = int(ps)
	}
	if pageSize > maxSize {
		pageSize = maxSize
	}
	return
}
