package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/go-chi/chi/v5"
)

// urlIntParam extracts a chi URL parameter and parses it as int64.
// On failure it writes a 400 error and returns 0, false.
func urlIntParam(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, name), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid %s", name))
		return 0, false
	}
	return id, true
}

// decodeJSON reads and JSON-decodes the request body into v.
// On failure it writes a 400 error and returns false.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	return true
}

// parseUploadToTemp parses a multipart form, extracts the named field, and writes
// it to a temp file. Returns the temp file path and true on success. On failure it
// writes the appropriate HTTP error and returns ("", false). The caller must
// defer os.Remove(path) to clean up the temp file.
func parseUploadToTemp(w http.ResponseWriter, r *http.Request, fieldName string, maxBytes int64, prefix string) (string, bool) {
	if err := r.ParseMultipartForm(maxBytes); err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart form")
		return "", false
	}
	file, _, err := r.FormFile(fieldName)
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing "+fieldName+" file")
		return "", false
	}
	defer file.Close()

	tmp, err := os.CreateTemp("", prefix)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create temp file")
		return "", false
	}

	if _, err := io.Copy(tmp, file); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		writeError(w, http.StatusInternalServerError, "failed to read upload")
		return "", false
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		writeError(w, http.StatusInternalServerError, "failed to write temp file")
		return "", false
	}
	return tmp.Name(), true
}
