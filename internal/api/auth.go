package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/auth"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type authHandler struct {
	db       utils.DBTX
	users    *repository.UserRepository
	sessions *repository.SessionRepository
	plugins  map[string]auth.Plugin
	secret   string
}

func newAuthHandler(db utils.DBTX, users *repository.UserRepository, sessions *repository.SessionRepository, secret string, plugins map[string]auth.Plugin) *authHandler {
	return &authHandler{db: db, users: users, sessions: sessions, secret: secret, plugins: plugins}
}

// clientIP returns the request's remote IP, stripping the port if present.
// middleware.RealIP has already resolved X-Forwarded-For / X-Real-IP by this point.
func clientIP(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
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
		DeviceName  string          `json:"device_name"`
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

	isAdmin, err := h.users.GetAdminStatus(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "user lookup failed")
		return
	}

	rawRefresh, hashRefresh, err := auth.GenerateRefreshToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	sessionID, err := h.sessions.Create(r.Context(), userID, hashRefresh, time.Now().Add(auth.RefreshTokenTTL), nullableString(body.DeviceName), clientIP(r))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "session creation failed")
		return
	}

	h.users.TouchLastSeen(r.Context(), userID)

	accessToken, err := auth.IssueToken(userID, isAdmin, h.secret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":             accessToken,
		"access_token_expires_in":  int(auth.AccessTokenTTL.Seconds()),
		"refresh_token":            rawRefresh,
		"refresh_token_expires_in": int(auth.RefreshTokenTTL.Seconds()),
		"session_id":               sessionID,
	})
}

// POST /api/v1/auth/refresh
func (h *authHandler) refresh(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RefreshToken == "" {
		writeError(w, http.StatusBadRequest, "refresh_token is required")
		return
	}

	hash := auth.HashRefreshToken(body.RefreshToken)
	ip := clientIP(r)

	// Check if this is a replayed (already-rotated) token — indicates theft.
	stolenUserID, found, _ := h.sessions.FindByPreviousTokenHash(r.Context(), hash)
	if found {
		_ = h.sessions.DeleteAllForUser(r.Context(), stolenUserID)
		slog.Warn("refresh token reuse detected — all sessions revoked",
			"user_id", stolenUserID, "ip", ip)
		writeError(w, http.StatusUnauthorized, "refresh token reuse detected; all sessions have been revoked")
		return
	}

	// Normal path: find the active session.
	sessionID, userID, isAdmin, expiresAt, err := h.sessions.FindByTokenHash(r.Context(), hash)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid or expired refresh token")
		return
	}
	if time.Now().After(expiresAt) {
		_, _ = h.sessions.Delete(r.Context(), sessionID, userID)
		writeError(w, http.StatusUnauthorized, "refresh token expired")
		return
	}

	// Rotate: store new token, keep the old hash as previous for theft detection.
	rawRefresh, hashRefresh, err := auth.GenerateRefreshToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	if err := h.sessions.RotateToken(r.Context(), sessionID, hashRefresh, hash, time.Now().Add(auth.RefreshTokenTTL), ip); err != nil {
		writeError(w, http.StatusInternalServerError, "token rotation failed")
		return
	}

	h.users.TouchLastSeen(r.Context(), userID)

	accessToken, err := auth.IssueToken(userID, isAdmin, h.secret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":             accessToken,
		"access_token_expires_in":  int(auth.AccessTokenTTL.Seconds()),
		"refresh_token":            rawRefresh,
		"refresh_token_expires_in": int(auth.RefreshTokenTTL.Seconds()),
		"session_id":               sessionID,
	})
}

// GET /api/v1/auth/sessions
func (h *authHandler) listSessions(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	h.writeSessionList(w, r, userID)
}

// DELETE /api/v1/auth/sessions/:id
func (h *authHandler) revokeSession(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	sessionID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid session id")
		return
	}

	affected, err := h.sessions.Delete(r.Context(), sessionID, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if affected == 0 {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/auth/credentials
func (h *authHandler) changeCredentials(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	providerName, err := h.users.GetAuthProvider(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	plugin, ok := h.plugins[providerName]
	if !ok {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("auth provider %q does not support credential updates", providerName))
		return
	}

	var body struct {
		Credentials json.RawMessage `json:"credentials"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := plugin.UpdateCredentials(r.Context(), h.db, userID, body.Credentials); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/auth/me
func (h *authHandler) me(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	user, err := h.users.FindByID(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":       user.ID,
		"name":     user.Name,
		"is_admin": user.IsAdmin,
	})
}

// writeSessionList is shared between the user and admin session-listing handlers.
func (h *authHandler) writeSessionList(w http.ResponseWriter, r *http.Request, userID int64) {
	sessions, err := h.sessions.ListForUser(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, sessions)
}

// nullableString returns nil for empty strings (maps to SQL NULL).
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
