package api

import (
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

func userIDFromRequest(r *http.Request) int64 {
	claims := ClaimsFromContext(r.Context())
	id, _ := strconv.ParseInt(claims.Subject, 10, 64)
	return id
}

type photosHandler struct {
	db        utils.DBTX
	dataPath  string
	mediaPath string
}

// GET /api/v1/collections/{id}/photos
func (h *photosHandler) listPhotos(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	userID := userIDFromRequest(r)
	accessible, err := repository.CollectionExistsAndAccessible(r.Context(), h.db, id, userID)
	if err != nil || !accessible {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	sortOrder := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort_order")))
	ascending := sortOrder != "desc"

	recursiveParam := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("recursive")))
	recursive := recursiveParam == "true"

	offset := queryInt(r, "offset", 0)
	limit := queryInt(r, "limit", 200)
	if limit > 1000 {
		limit = 1000
	}

	items, err := repository.PhotoItems(r.Context(), h.db, repository.PhotoQuery{
		CollectionID: id,
		Offset:       offset,
		Limit:        limit,
		Ascending:    ascending,
		Recursive:    recursive,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	for i := range items {
		items[i].Ordinal = int64(offset + i)
		items[i].RemapCameraModel()
		items[i].RemapLensModel()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"offset": offset,
		"limit":  limit,
	})
}

// GET /api/v1/collections/{id}/photos/stats
func (h *photosHandler) photoStats(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	userID := userIDFromRequest(r)
	accessible, err := repository.CollectionExistsAndAccessible(r.Context(), h.db, id, userID)
	if err != nil || !accessible {
		writeError(w, http.StatusNotFound, "collection not found")
		return
	}

	recursiveParam := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("recursive")))
	recursive := recursiveParam == "true"

	counts, err := repository.PhotoStatistics(r.Context(), h.db, id, recursive)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	total := 0
	for _, c := range counts {
		total += c.Count
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"total":  total,
		"months": counts,
	})
}

// GET /images/:id/exif
func (h *photosHandler) getExif(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	raw, err := repository.PhotoExifRaw(r.Context(), h.db, id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "EXIF data not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

// getItemHashAndPath fetches the content hash and full filesystem path for a media item
// in a single DB roundtrip.
func (h *photosHandler) getItemHashAndPath(id int64, r *http.Request) (hash, fsPath string, err error) {
	hash, relativePath, err := repository.MediaItemFileHashAndPath(r.Context(), h.db, id, userIDFromRequest(r))
	if err != nil {
		return "", "", err
	}
	return hash, filepath.Join(h.mediaPath, relativePath), nil
}

// GET /images/:id/:variant
// Serves WebP variants directly. Falls back to JPEG on-the-fly for legacy clients.
func (h *photosHandler) serveVariant(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}
	variant := chi.URLParam(r, "variant")

	switch variant {
	case imaging.VariantThumb, imaging.VariantSmall, imaging.VariantPreview:
	default:
		writeError(w, http.StatusBadRequest, "invalid variant")
		return
	}

	hash, fsPath, err := h.getItemHashAndPath(id, r)
	if err != nil {
		writeError(w, http.StatusNotFound, "media item not found")
		return
	}

	// ?format= query param overrides Accept header negotiation (used by Roku Poster nodes).
	accept := resolveAccept(r)

	// Walk the fallback chain until we find a variant that exists on disk.
	// Variants are ordered largest to smallest — the requested variant is
	// tried first, then each smaller one, then the source file as last resort.
	fallbackChain := variantFallbackChain(variant)

	for _, v := range fallbackChain {
		if v == variantOriginal {
			http.ServeFile(w, r, fsPath)
			return
		}

		webpPath := imaging.VariantPath(h.dataPath, hash, v, "webp")
		if !fileExists(webpPath) {
			continue
		}

		// Serve WebP directly unless client only accepts JPEG.
		if !strings.Contains(accept, "image/jpeg") || strings.Contains(accept, "image/webp") {
			http.ServeFile(w, r, webpPath)
			return
		}

		// JPEG on-the-fly fallback for legacy clients.
		jpegBytes, err := imaging.GenerateJPEG(webpPath)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "image conversion failed")
			return
		}
		w.Header().Set("Content-Type", "image/jpeg")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(jpegBytes)
		return
	}

	writeError(w, http.StatusNotFound, "variant not found")
}

// GET /images/:id/tiles/image.dzi
// Generates DZI tiles on-demand if they don't already exist.
func (h *photosHandler) serveDZIManifest(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	hash, fsPath, err := h.getItemHashAndPath(id, r)
	if err != nil {
		writeError(w, http.StatusNotFound, "media item not found")
		return
	}

	generated, err := imaging.GenerateDZIIfNotPresent(fsPath, h.dataPath, hash)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate DZI tiles")
		return
	}

	if generated {
		w.Header().Set("X-DZI-Generated", "true")
	} else {
		w.Header().Set("X-DZI-Generated", "false")
	}

	dziPath := filepath.Join(imaging.TilesPath(h.dataPath, hash), "image.dzi")
	http.ServeFile(w, r, dziPath)
}

// GET /images/:id/tiles/*
// Serves DZI tile files. The wildcard captures the level/col_row.webp path.
func (h *photosHandler) serveDZITile(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	hash, _, err := h.getItemHashAndPath(id, r)
	if err != nil {
		writeError(w, http.StatusNotFound, "media item not found")
		return
	}

	// chi.URLParam with wildcard gives us everything after /tiles/
	tilePath := chi.URLParam(r, "*")
	// Sanitize: prevent directory traversal
	tilePath = filepath.Clean("/" + tilePath)

	fullPath := filepath.Join(imaging.TilesPath(h.dataPath, hash), tilePath)

	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		writeError(w, http.StatusNotFound, "tile not found")
		return
	}

	http.ServeFile(w, r, fullPath)
}

// GET /images/collections/{id}/cover
func (h *photosHandler) serveCollectionCover(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	serveStoredImage(w, r,
		imaging.CollectionThumbnailPath(h.dataPath, id, "webp"),
		"cover not found",
	)
}

// resolveAccept returns the effective Accept value for image format negotiation.
// The ?format= query parameter takes precedence over the Accept header, allowing
// clients that cannot set headers (e.g. Roku Poster nodes) to request a specific format.
func resolveAccept(r *http.Request) string {
	switch r.URL.Query().Get("format") {
	case "webp":
		return "image/webp"
	case "jpeg", "jpg":
		return "image/jpeg"
	default:
		return r.Header.Get("Accept")
	}
}

// serveStoredImage serves a WebP image file. Falls back to JPEG on-the-fly
// for legacy clients that explicitly request JPEG and don't accept WebP.
// Returns 404 if the file doesn't exist.
func serveStoredImage(w http.ResponseWriter, r *http.Request, webpPath, notFoundMsg string) {
	if !fileExists(webpPath) {
		writeError(w, http.StatusNotFound, notFoundMsg)
		return
	}

	accept := resolveAccept(r)

	// Serve JPEG on-the-fly only when client explicitly requests JPEG and doesn't accept WebP.
	if strings.Contains(accept, "image/jpeg") && !strings.Contains(accept, "image/webp") {
		jpegBytes, err := imaging.GenerateJPEG(webpPath)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "image conversion failed")
			return
		}
		w.Header().Set("Content-Type", "image/jpeg")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(jpegBytes)
		return
	}

	http.ServeFile(w, r, webpPath)
}

// variantOriginal is a sentinel value in a fallback chain representing the source file.
const variantOriginal = "original"

// variantFallbackChain returns the variant, the source file sentinel, and all smaller
// variants in order. The source file is placed immediately after the requested variant
// so that a too-small-to-derive source is preferred over downsampled smaller variants.
func variantFallbackChain(variant string) []string {
	order := []string{
		imaging.VariantPreview,
		imaging.VariantSmall,
		imaging.VariantThumb,
	}
	for i, v := range order {
		if v == variant {
			chain := make([]string, 0, len(order)-i+1)
			chain = append(chain, v, variantOriginal)
			chain = append(chain, order[i+1:]...)
			return chain
		}
	}
	return []string{imaging.VariantThumb, variantOriginal}
}

// fileExists returns true if the path exists and is a regular file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
