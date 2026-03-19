package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/auth"
)

type authHandler struct {
	db      *pgxpool.Pool
	plugins map[string]auth.Plugin
	secret  string
}

func newAuthHandler(db *pgxpool.Pool, secret string, plugins map[string]auth.Plugin) *authHandler {
	return &authHandler{db: db, secret: secret, plugins: plugins}
}

// deleteAllSessions removes every active session for the given user.
func (h *authHandler) deleteAllSessions(ctx context.Context, userID int64) error {
	_, err := h.db.Exec(ctx, `DELETE FROM user_sessions WHERE user_id = $1`, userID)
	return err
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

	var isAdmin bool
	if err := h.db.QueryRow(r.Context(),
		`SELECT is_admin FROM users WHERE id = $1`, userID,
	).Scan(&isAdmin); err != nil {
		writeError(w, http.StatusInternalServerError, "user lookup failed")
		return
	}

	rawRefresh, hashRefresh, err := auth.GenerateRefreshToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	var sessionID int64
	err = h.db.QueryRow(r.Context(),
		`INSERT INTO user_sessions (user_id, token_hash, expires_at, device_name, ip_address)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		userID, hashRefresh, time.Now().Add(auth.RefreshTokenTTL),
		nullableString(body.DeviceName), clientIP(r),
	).Scan(&sessionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "session creation failed")
		return
	}

	_, _ = h.db.Exec(r.Context(), `UPDATE users SET last_seen_at = now() WHERE id = $1`, userID)

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
	var stolenUserID int64
	err := h.db.QueryRow(r.Context(),
		`SELECT user_id FROM user_sessions WHERE previous_token_hash = $1`, hash,
	).Scan(&stolenUserID)
	if err == nil {
		// A previously rotated token was presented again. Revoke all sessions for this user.
		_ = h.deleteAllSessions(r.Context(), int64(stolenUserID))
		slog.Warn("refresh token reuse detected — all sessions revoked",
			"user_id", stolenUserID, "ip", ip)
		writeError(w, http.StatusUnauthorized, "refresh token reuse detected; all sessions have been revoked")
		return
	}

	// Normal path: find the active session.
	var sessionID int64
	var userID int64
	var isAdmin bool
	var expiresAt time.Time
	err = h.db.QueryRow(r.Context(),
		`SELECT s.id, s.user_id, u.is_admin, s.expires_at
		 FROM user_sessions s
		 JOIN users u ON u.id = s.user_id
		 WHERE s.token_hash = $1`,
		hash,
	).Scan(&sessionID, &userID, &isAdmin, &expiresAt)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid or expired refresh token")
		return
	}
	if time.Now().After(expiresAt) {
		_, _ = h.db.Exec(r.Context(), `DELETE FROM user_sessions WHERE id = $1`, sessionID)
		writeError(w, http.StatusUnauthorized, "refresh token expired")
		return
	}

	// Rotate: store new token, keep the old hash as previous for theft detection.
	rawRefresh, hashRefresh, err := auth.GenerateRefreshToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	_, err = h.db.Exec(r.Context(),
		`UPDATE user_sessions
		 SET token_hash = $2, previous_token_hash = $3,
		     expires_at = $4, ip_address = $5, last_used_at = now()
		 WHERE id = $1`,
		sessionID, hashRefresh, hash, time.Now().Add(auth.RefreshTokenTTL), ip,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token rotation failed")
		return
	}

	_, _ = h.db.Exec(r.Context(), `UPDATE users SET last_seen_at = now() WHERE id = $1`, userID)

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

	// user_id scoping ensures users can only revoke their own sessions.
	tag, err := h.db.Exec(r.Context(),
		`DELETE FROM user_sessions WHERE id = $1 AND user_id = $2`,
		sessionID, userID,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if tag.RowsAffected() == 0 {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/auth/credentials
func (h *authHandler) changeCredentials(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	var providerName string
	if err := h.db.QueryRow(r.Context(),
		`SELECT auth_provider FROM users WHERE id = $1`, userID,
	).Scan(&providerName); err != nil {
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

// writeSessionList is shared between the user and admin session-listing handlers.
func (h *authHandler) writeSessionList(w http.ResponseWriter, r *http.Request, userID int64) {
	rows, err := h.db.Query(r.Context(),
		`SELECT id, device_name, ip_address, last_used_at, created_at, expires_at
		 FROM user_sessions
		 WHERE user_id = $1
		 ORDER BY last_used_at DESC`,
		userID,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	defer rows.Close()

	type sessionView struct {
		ID          int64      `json:"id"`
		DeviceName  *string    `json:"device_name"`
		IPAddress   *string    `json:"ip_address"`
		LastUsedAt  time.Time  `json:"last_used_at"`
		CreatedAt   time.Time  `json:"created_at"`
		ExpiresAt   time.Time  `json:"expires_at"`
	}

	var sessions []sessionView
	for rows.Next() {
		var s sessionView
		if err := rows.Scan(&s.ID, &s.DeviceName, &s.IPAddress, &s.LastUsedAt, &s.CreatedAt, &s.ExpiresAt); err != nil {
			continue
		}
		sessions = append(sessions, s)
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
