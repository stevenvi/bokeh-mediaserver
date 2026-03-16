package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/auth"
)

type authHandler struct {
	db      *pgxpool.Pool
	plugins map[string]auth.Plugin
	secret  string
}

func newAuthHandler(db *pgxpool.Pool, secret string) *authHandler {
	h := &authHandler{
		db:      db,
		plugins: make(map[string]auth.Plugin),
		secret:  secret,
	}
	// Register the default local plugin
	local := auth.LocalPlugin{}
	h.plugins[local.Name()] = local
	return h
}

// GET /api/v1/auth/providers
func (h *authHandler) listProviders(w http.ResponseWriter, r *http.Request) {
	type provider struct {
		Name string `json:"name"`
	}
	providers := make([]provider, 0, len(h.plugins))
	for name := range h.plugins {
		providers = append(providers, provider{Name: name})
	}
	writeJSON(w, http.StatusOK, providers)
}

// POST /api/v1/auth/login
func (h *authHandler) login(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Provider    string          `json:"provider"`
		Credentials json.RawMessage `json:"credentials"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Provider == "" {
		body.Provider = "local"
	}

	plugin, ok := h.plugins[body.Provider]
	if !ok {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown auth provider: %s", body.Provider))
		return
	}

	userID, err := plugin.Authenticate(r.Context(), h.db, body.Credentials)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	// Fetch is_admin for the token payload
	var isAdmin bool
	if err := h.db.QueryRow(r.Context(),
		`SELECT is_admin FROM users WHERE id = $1`, userID,
	).Scan(&isAdmin); err != nil {
		writeError(w, http.StatusInternalServerError, "user lookup failed")
		return
	}

	token, err := auth.IssueToken(userID, isAdmin, h.secret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	// TODO: Return refresh token as well, and implement token refreshing endpoint!!
	//       Can it be integrated into a single db call above??
	writeJSON(w, http.StatusOK, map[string]string{"access_token": token})
}

// GET /api/v1/auth/me
func (h *authHandler) me(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.Atoi(claims.Subject)

	var name string
	var isAdmin bool
	if err := h.db.QueryRow(r.Context(),
		`SELECT name, is_admin FROM users WHERE id = $1`, userID,
	).Scan(&name, &isAdmin); err != nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":       userID,
		"name":     name,
		"is_admin": isAdmin,
	})
}
