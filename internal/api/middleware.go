package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/stevenvi/bokeh-mediaserver/internal/auth"
)

type contextKey string

const claimsKey contextKey = "claims"

// ClaimsFromContext retrieves JWT claims stored by the auth middleware.
func ClaimsFromContext(ctx context.Context) *auth.Claims {
	v, _ := ctx.Value(claimsKey).(*auth.Claims)
	return v
}

// requireJWT validates the Bearer token, checks the device guard, injects
// claims into the request context, and optionally enforces the admin claim.
func requireJWT(secret string, guard *DeviceGuard, adminOnly bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			if !strings.HasPrefix(header, "Bearer ") {
				writeError(w, http.StatusUnauthorized, "missing or invalid authorization header")
				return
			}
			claims, err := auth.ParseToken(strings.TrimPrefix(header, "Bearer "), secret)
			if err != nil {
				writeError(w, http.StatusUnauthorized, "invalid or expired token")
				return
			}
			if guard.IsBlocked(claims.DeviceID) {
				writeError(w, http.StatusForbidden, "device has been revoked or banned")
				return
			}
			if adminOnly && !claims.IsAdmin {
				writeError(w, http.StatusForbidden, "admin access required")
				return
			}
			ctx := context.WithValue(r.Context(), claimsKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireAuth validates the Bearer token, checks the device guard, and injects
// claims into the request context.
func RequireAuth(secret string, guard *DeviceGuard) func(http.Handler) http.Handler {
	return requireJWT(secret, guard, false)
}

// RequireAdmin extends RequireAuth — additionally checks the adm claim.
func RequireAdmin(secret string, guard *DeviceGuard) func(http.Handler) http.Handler {
	return requireJWT(secret, guard, true)
}

// writeError writes a consistent JSON error response.
func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":      http.StatusText(status),
		"message":    message,
		"statusCode": status,
	})
}

// writeJSON writes a value as a JSON response.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writeJSON encode", "err", err)
	}
}

// statusRecorder wraps an http.ResponseWriter to capture the response status code.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

// requestLogger is a simple structured request logging middleware.
func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		slog.Debug(fmt.Sprintf("[%d] %s %s", rec.status, r.Method, r.URL.Path))
	})
}
