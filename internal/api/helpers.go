package api

import (
	"encoding/json"
	"fmt"
	"net/http"
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
