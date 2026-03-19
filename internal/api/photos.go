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
)

func userIDFromRequest(r *http.Request) int64 {
	claims := ClaimsFromContext(r.Context())
	id, _ := strconv.ParseInt(claims.Subject, 10, 64)
	return id
}

type photosHandler struct {
	media     *repository.MediaItemRepository
	dataPath  string
	mediaPath string
}

// GET /api/v1/media/:id
func (h *photosHandler) getItem(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	item, err := h.media.GetByID(r.Context(), id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "media item not found")
		return
	}

	photo, err := h.media.GetPhotoMetadata(r.Context(), id)
	if err == nil {
		item.Photo = photo
	}

	writeJSON(w, http.StatusOK, item)
}

// GET /images/:id/exif
func (h *photosHandler) getExif(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	raw, err := h.media.GetExifRaw(r.Context(), id, userIDFromRequest(r))
	if err != nil {
		writeError(w, http.StatusNotFound, "EXIF data not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

// TODO: Are having both of these functions that query the DB for the same media item inefficient? Should we combine them into one query and pass the data down?
func (h *photosHandler) getItemFsPath(id int64, r *http.Request) (string, error) {
	relativePath, err := h.media.GetRelativePath(r.Context(), id)
	if err != nil {
		return "", err
	}
	return filepath.Join(h.mediaPath, relativePath), nil
}

func (h *photosHandler) getItemHash(id int64, r *http.Request) (string, error) {
	return h.media.GetFileHash(r.Context(), id)
}

// GET /images/:id/:variant
// Serves AVIF to clients that send Accept: image/avif, JPEG otherwise.
func (h *photosHandler) serveVariant(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	variant := chi.URLParam(r, "variant")

	switch variant {
	case imaging.VariantThumb, imaging.VariantSmall, imaging.VariantPreview:
	default:
		writeError(w, http.StatusBadRequest, "invalid variant")
		return
	}

	hash, err := h.getItemHash(id, r)
	if err != nil {
		writeError(w, http.StatusNotFound, "media item not found")
		return
	}

	acceptsAVIF := strings.Contains(r.Header.Get("Accept"), "image/avif")

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
		fsPath, err := h.getItemFsPath(id, r)
		if err != nil {
			writeError(w, http.StatusNotFound, "media item not found")
			return
		}
		http.ServeFile(w, r, fsPath)
		return
	}

	// JPEG: check if jpeg exists, otherwise do on-the-fly from the best available AVIF
	for _, v := range fallbackChain {
		jpegPath := imaging.VariantPath(h.dataPath, hash, v, "jpg")
		avifPath := imaging.VariantPath(h.dataPath, hash, v, "avif")
		if fileExists(jpegPath) {
			http.ServeFile(w, r, jpegPath)
			return
		}
		if fileExists(avifPath) {
			jpegBytes, err := imaging.GenerateJPEGOnTheFly(avifPath)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "image conversion failed")
				return
			}
			w.Header().Set("Content-Type", "image/jpeg")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(jpegBytes)
			return
		}
	}

	// Couldn't get any image data, fail
	writeError(w, http.StatusNotFound, "variant not found")
}

// GET /images/:id/tiles/image.dzi
func (h *photosHandler) serveDZIManifest(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	hash, err := h.getItemHash(id, r)
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
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	hash, err := h.getItemHash(id, r)
	if err != nil {
		writeError(w, http.StatusNotFound, "media item not found")
		return
	}

	// chi.URLParam with wildcard gives us everything after /tiles/
	tilePath := chi.URLParam(r, "*")
	// Sanitize: prevent directory traversal
	tilePath = filepath.Clean("/" + tilePath)

	fullPath := filepath.Join(
		imaging.TilesPath(h.dataPath, hash),
		"image_files",
		tilePath,
	)

	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		writeError(w, http.StatusNotFound, "tile not found")
		return
	}

	http.ServeFile(w, r, fullPath)
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
