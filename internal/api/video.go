package api

import (
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/config"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/streaming"
)

type videoHandler struct {
	media      *repository.MediaItemRepository
	dataPath   string
	mediaPath  string
	cfg        *config.Config
	dispatcher *jobs.Dispatcher
}

// isLocalRequest returns true if the request originates from an RFC 1918 or
// loopback address. X-Forwarded-For is checked first (leftmost hop); if absent
// the direct RemoteAddr is used.
func isLocalRequest(r *http.Request) bool {
	raw := r.Header.Get("X-Forwarded-For")
	if raw != "" {
		// Leftmost IP is the original client
		raw = strings.TrimSpace(strings.SplitN(raw, ",", 2)[0])
	} else {
		raw, _, _ = net.SplitHostPort(r.RemoteAddr)
		if raw == "" {
			raw = r.RemoteAddr
		}
	}

	ip := net.ParseIP(raw)
	if ip == nil {
		return false
	}
	return isPrivateOrLoopback(ip)
}

var privateRanges = func() []*net.IPNet {
	cidrs := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"127.0.0.0/8",
		"::1/128",
		"fc00::/7",
	}
	var nets []*net.IPNet
	for _, cidr := range cidrs {
		_, n, _ := net.ParseCIDR(cidr)
		nets = append(nets, n)
	}
	return nets
}()

func isPrivateOrLoopback(ip net.IP) bool {
	if ip.IsLoopback() {
		return true
	}
	for _, n := range privateRanges {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// GET /videos/{id}/stream
// Redirects to raw (local), stored HLS (transcoded remote), or live HLS.
func (h *videoHandler) stream(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	if isLocalRequest(r) {
		http.Redirect(w, r, "/videos/"+chi.URLParam(r, "id")+"/raw", http.StatusFound)
		return
	}

	// Check if stored transcode exists
	_, _, fileHash, err := h.media.GetVideoStreamInfo(r.Context(), id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}

	if fileExists(imaging.VideoHLSManifest(h.dataPath, fileHash)) {
		http.Redirect(w, r, "/videos/"+chi.URLParam(r, "id")+"/hls/manifest.m3u8", http.StatusFound)
		return
	}

	http.Redirect(w, r, "/videos/"+chi.URLParam(r, "id")+"/live/manifest.m3u8", http.StatusFound)
}

// GET /videos/{id}/raw
// Serves the original video file with byte-range support via http.ServeFile.
func (h *videoHandler) raw(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	relativePath, mimeType, _, err := h.media.GetVideoStreamInfo(r.Context(), id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}

	w.Header().Set("Content-Type", mimeType)
	http.ServeFile(w, r, filepath.Join(h.mediaPath, relativePath))
}

// GET /videos/{id}/hls/manifest.m3u8
// Serves the stored HLS transcode manifest.
func (h *videoHandler) hlsManifest(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	_, _, fileHash, err := h.media.GetVideoStreamInfo(r.Context(), id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}

	manifestPath := imaging.VideoHLSManifest(h.dataPath, fileHash)
	if !fileExists(manifestPath) {
		writeError(w, http.StatusNotFound, "transcode not available")
		return
	}

	http.ServeFile(w, r, manifestPath)
}

// GET /videos/{id}/hls/{segment}
// Serves a segment from the stored HLS transcode.
func (h *videoHandler) hlsSegment(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	_, _, fileHash, err := h.media.GetVideoStreamInfo(r.Context(), id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}

	segName := chi.URLParam(r, "segment")
	hlsDir := imaging.VideoHLSDir(h.dataPath, fileHash)
	segPath := filepath.Join(hlsDir, filepath.Clean(segName))

	// Prevent path traversal outside the HLS directory
	if !strings.HasPrefix(segPath, hlsDir+string(filepath.Separator)) {
		writeError(w, http.StatusBadRequest, "invalid segment path")
		return
	}

	if !fileExists(segPath) {
		writeError(w, http.StatusNotFound, "segment not found")
		return
	}

	http.ServeFile(w, r, segPath)
}

// GET /videos/{id}/live/manifest.m3u8
// Creates (or reuses) an on-the-fly HLS session and serves the manifest.
func (h *videoHandler) liveManifest(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	userID := userIDFromRequest(r)
	relativePath, _, _, err := h.media.GetVideoStreamInfo(r.Context(), id, userID)
	if err != nil {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}

	meta, err := h.media.GetVideoMetadataWithBookmark(r.Context(), id, userID)
	if err != nil {
		// metadata may not exist yet; treat bitrate as unknown
		meta = nil
	}

	fsPath := filepath.Join(h.mediaPath, relativePath)

	var bitrateKbps *int
	if meta != nil {
		bitrateKbps = meta.BitrateKbps
	}

	session, err := streaming.GetOrCreateSession(id, bitrateKbps, fsPath, h.cfg.TranscodeBitrateKbps, h.dispatcher)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to start stream")
		return
	}

	streaming.ServeManifest(w, r, session)
}

// GET /videos/{id}/live/{segment}
// Serves a segment from an active on-the-fly HLS session.
func (h *videoHandler) liveSegment(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	session := streaming.GetSession(id)
	if session == nil {
		writeError(w, http.StatusNotFound, "no active stream session")
		return
	}

	segName := chi.URLParam(r, "segment")
	streaming.ServeSegment(w, r, session, segName)
}

// GET /images/videos/{id}/cover
// Content-negotiated: serves AVIF if accepted, else WebP. 404 if neither exists.
func (h *videoHandler) cover(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	_, _, fileHash, err := h.media.GetVideoStreamInfo(r.Context(), id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}

	accept := r.Header.Get("Accept")
	acceptsAVIF := strings.Contains(accept, "image/avif")

	if acceptsAVIF {
		avifPath := imaging.VariantPath(h.dataPath, fileHash, "cover", "avif")
		if fileExists(avifPath) {
			w.Header().Set("Content-Type", "image/avif")
			http.ServeFile(w, r, avifPath)
			return
		}
	}

	webpPath := imaging.VariantPath(h.dataPath, fileHash, "cover", "webp")
	if fileExists(webpPath) {
		w.Header().Set("Content-Type", "image/webp")
		http.ServeFile(w, r, webpPath)
		return
	}

	avifPath := imaging.VariantPath(h.dataPath, fileHash, "cover", "avif")
	if fileExists(avifPath) {
		w.Header().Set("Content-Type", "image/avif")
		http.ServeFile(w, r, avifPath)
		return
	}

	writeError(w, http.StatusNotFound, "cover not found")
}

// PUT /api/v1/media/{id}/bookmark
func (h *videoHandler) upsertBookmark(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	var body struct {
		PositionSeconds int `json:"position_seconds"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}

	userID := userIDFromRequest(r)
	if err := h.media.UpsertVideoBookmark(r.Context(), userID, id, body.PositionSeconds); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to save bookmark")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// DELETE /api/v1/media/{id}/bookmark
func (h *videoHandler) deleteBookmark(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	userID := userIDFromRequest(r)
	if err := h.media.DeleteVideoBookmark(r.Context(), userID, id); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete bookmark")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/admin/media/{id}/cover
// Admin endpoint to upload a manual cover image for a video item.
func (h *videoHandler) uploadCover(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	if err := r.ParseMultipartForm(20 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart form")
		return
	}

	file, _, err := r.FormFile("cover")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing cover file")
		return
	}
	defer file.Close()

	imageBytes, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read upload")
		return
	}

	_, _, fileHash, err := h.media.GetVideoStreamInfo(r.Context(), id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}

	collType, _ := h.media.GetRootCollectionType(r.Context(), id)
	widthRatio, heightRatio := 3, 4
	if collType == "video:movie" {
		widthRatio, heightRatio = 2, 3
	}

	if err := imaging.GenerateVideoCoverFromBytes(imageBytes, h.dataPath, fileHash, widthRatio, heightRatio); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to process cover image")
		return
	}

	if err := h.media.SetVideoManualCover(r.Context(), id, true); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update video metadata")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

