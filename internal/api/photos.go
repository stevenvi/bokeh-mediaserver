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

// GET /api/v1/media/:id
func (h *photosHandler) getItem(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	item, err := repository.MediaItemGet(r.Context(), h.db, id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "media item not found")
		return
	}

	writeJSON(w, http.StatusOK, item)
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
// Content-negotiated: serves AVIF natively, WebP or JPEG on-the-fly from AVIF.
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

	accept := r.Header.Get("Accept")
	acceptsAVIF := strings.Contains(accept, "image/avif")
	acceptsWebP := strings.Contains(accept, "image/webp")

	// Walk the fallback chain until we find a variant that exists on disk.
	// Variants are ordered largest to smallest — the requested variant is
	// tried first, then each smaller one, then the source file as last resort.
	fallbackChain := variantFallbackChain(variant)

	if acceptsAVIF {
		for _, v := range fallbackChain {
			path := imaging.VariantPath(h.dataPath, hash, v, "avif")
			if fileExists(path) {
				http.ServeFile(w, r, path)
				return
			}
		}

		// Last resort: serve source file directly
		http.ServeFile(w, r, fsPath)
		return
	}

	// Non-AVIF path: transcode from the best available AVIF on disk.
	// Check for pre-generated JPEG thumb first (always exists on disk).
	for _, v := range fallbackChain {
		jpegPath := imaging.VariantPath(h.dataPath, hash, v, "jpg")
		if fileExists(jpegPath) {
			http.ServeFile(w, r, jpegPath)
			return
		}

		avifPath := imaging.VariantPath(h.dataPath, hash, v, "avif")
		if !fileExists(avifPath) {
			continue
		}

		if acceptsWebP {
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
		return
	}

	// Couldn't get any image data, fail
	writeError(w, http.StatusNotFound, "variant not found")
}

// GET /images/:id/tiles/image.dzi
func (h *photosHandler) serveDZIManifest(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	hash, _, err := h.getItemHashAndPath(id, r)
	if err != nil {
		writeError(w, http.StatusNotFound, "media item not found")
		return
	}

	dziPath := filepath.Join(imaging.TilesPath(h.dataPath, hash), "image.dzi")
	http.ServeFile(w, r, dziPath)
}

// GET /images/:id/tiles/*
// Serves DZI tile files. The wildcard captures the level/col_row.avif path.
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
// Content-negotiated: serves AVIF if accepted, else WebP.
func (h *photosHandler) serveCollectionCover(w http.ResponseWriter, r *http.Request) {
	id, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	accept := r.Header.Get("Accept")
	acceptsAVIF := strings.Contains(accept, "image/avif")

	if acceptsAVIF {
		avifPath := imaging.CollectionThumbnailPath(h.dataPath, id, "avif")
		if fileExists(avifPath) {
			w.Header().Set("Content-Type", "image/avif")
			http.ServeFile(w, r, avifPath)
			return
		}
	}

	webpPath := imaging.CollectionThumbnailPath(h.dataPath, id, "webp")
	if fileExists(webpPath) {
		w.Header().Set("Content-Type", "image/webp")
		http.ServeFile(w, r, webpPath)
		return
	}

	// AVIF fallback if WebP doesn't exist (shouldn't happen but be safe)
	avifPath := imaging.CollectionThumbnailPath(h.dataPath, id, "avif")
	if fileExists(avifPath) {
		w.Header().Set("Content-Type", "image/avif")
		http.ServeFile(w, r, avifPath)
		return
	}

	writeError(w, http.StatusNotFound, "cover not found")
}

// variantFallbackChain returns the variant and all smaller variants in order.
// The requested variant is always first — it's tried before falling back.
func variantFallbackChain(variant string) []string {
	order := []string{
		imaging.VariantPreview,
		imaging.VariantSmall,
		imaging.VariantThumb,
	}
	for i, v := range order {
		if v == variant {
			return order[i:]
		}
	}
	return []string{imaging.VariantThumb}
}

// fileExists returns true if the path exists and is a regular file.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
