package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/auth"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type authHandler struct {
	db         utils.DBTX
	guard      *DeviceGuard
	rl         *loginRateLimiter
	plugins    map[string]auth.Plugin
	secret     string
	production bool
}

func newAuthHandler(db utils.DBTX, guard *DeviceGuard, rl *loginRateLimiter, secret string, plugins map[string]auth.Plugin, production bool) *authHandler {
	return &authHandler{db: db, guard: guard, rl: rl, secret: secret, plugins: plugins, production: production}
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

// setAuthCookies sets httpOnly auth cookies for both the access and refresh tokens.
// Cookies are marked Secure only in production to allow http:// in development.
func setAuthCookies(w http.ResponseWriter, accessToken, refreshToken string, production bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     "access_token",
		Value:    accessToken,
		Path:     "/",
		MaxAge:   int(auth.AccessTokenTTL.Seconds()),
		HttpOnly: true,
		Secure:   production,
		SameSite: http.SameSiteStrictMode,
	})
	http.SetCookie(w, &http.Cookie{
		Name:     "refresh_token",
		Value:    refreshToken,
		Path:     "/api/v1/auth/refresh",
		MaxAge:   int(auth.RefreshTokenTTL.Seconds()),
		HttpOnly: true,
		Secure:   production,
		SameSite: http.SameSiteStrictMode,
	})
}

// clearAuthCookies overwrites both auth cookies with empty expired values.
func clearAuthCookies(w http.ResponseWriter, production bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     "access_token",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   production,
		SameSite: http.SameSiteStrictMode,
	})
	http.SetCookie(w, &http.Cookie{
		Name:     "refresh_token",
		Value:    "",
		Path:     "/api/v1/auth/refresh",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   production,
		SameSite: http.SameSiteStrictMode,
	})
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
		DeviceUUID  string          `json:"device_uuid"`
		DeviceName  string          `json:"device_name"`
		Credentials json.RawMessage `json:"credentials"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.DeviceUUID) == "" {
		writeError(w, http.StatusBadRequest, "device_uuid is required")
		return
	}
	if body.Provider == "" {
		body.Provider = "local"
	}

	ip := clientIP(r)

	if h.rl.IsLocked(ip) {
		writeError(w, http.StatusTooManyRequests, "try again later")
		return
	}

	plugin, ok := h.plugins[body.Provider]
	if !ok {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown auth provider: %s", body.Provider))
		return
	}

	userID, err := plugin.Authenticate(r.Context(), h.db, body.Credentials)
	if err != nil {
		h.rl.Record(ip)
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	// TODO: Stop all the one-property query madness here!!
	localAccessOnly, err := repository.UserIsLocalOnly(r.Context(), h.db, userID)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "user lookup failed")
		return
	}
	if localAccessOnly && !isLocalRequest(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	isAdmin, err := repository.UserIsAdmin(r.Context(), h.db, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "user lookup failed")
		return
	}

	// Look up existing device for this (userID, device_uuid).
	existing, err := repository.DeviceFindByUserAndUUID(r.Context(), h.db, userID, body.DeviceUUID)
	if err != nil && err != repository.ErrNotFound {
		writeError(w, http.StatusInternalServerError, "device lookup failed")
		return
	}

	if existing != nil && existing.BannedAt != nil {
		writeError(w, http.StatusForbidden, "this device has been banned")
		return
	}

	rawRefresh, hashRefresh, err := auth.GenerateRefreshToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	expiresAt := time.Now().Add(auth.RefreshTokenTTL)
	entry := models.AccessHistoryEntry{
		IP:       ip,
		Agent:    r.Header.Get("User-Agent"),
		LastSeen: time.Now(),
	}

	var deviceID int64
	if existing == nil {
		// New device — check count and possibly evict LRU.
		count, err := repository.DevicesCountActiveForUser(r.Context(), h.db, userID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "device count failed")
			return
		}
		if count >= repository.DevicesMaxPerUser {
			evictedID, err := repository.DeviceEvictLRU(r.Context(), h.db, userID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "device eviction failed")
				return
			}
			if evictedID != 0 {
				h.guard.Revoke(evictedID, auth.AccessTokenTTL)
			}
		}
		deviceID, err = repository.DeviceCreate(r.Context(), h.db, userID, body.DeviceUUID, body.DeviceName, hashRefresh, expiresAt, entry)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "device creation failed")
			return
		}
	} else {
		deviceID = existing.ID
		if err := repository.DeviceUpdateSession(r.Context(), h.db, deviceID, hashRefresh, "", expiresAt, entry, body.DeviceName); err != nil {
			writeError(w, http.StatusInternalServerError, "session update failed")
			return
		}
	}

	repository.UserTouchLastSeen(r.Context(), h.db, userID)

	accessToken, err := auth.IssueToken(userID, deviceID, isAdmin, h.secret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	// Set httpOnly cookies before writing the response body.
	setAuthCookies(w, accessToken, rawRefresh, h.production)

	// Also include tokens in the JSON body for convenience
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":             accessToken,
		"access_token_expires_in":  int(auth.AccessTokenTTL.Seconds()),
		"refresh_token":            rawRefresh,
		"refresh_token_expires_in": int(auth.RefreshTokenTTL.Seconds()),
		"device_id":                deviceID,
	})
}

// POST /api/v1/auth/refresh
func (h *authHandler) refresh(w http.ResponseWriter, r *http.Request) {
	// Try cookie first, fall back to JSON body for each field.
	var refreshToken = ""

	if cookie, err := r.Cookie("refresh_token"); err == nil {
		refreshToken = cookie.Value
	}

	var body struct {
		RefreshToken string `json:"refresh_token"`
		DeviceUUID   string `json:"device_uuid"`
	}

	if !decodeJSON(w, r, &body) {
		return
	}

	deviceUUID := body.DeviceUUID
	if refreshToken == "" {
		refreshToken = body.RefreshToken
	}

	if strings.TrimSpace(refreshToken) == "" {
		writeError(w, http.StatusBadRequest, "refresh_token is required")
		return
	}
	if strings.TrimSpace(deviceUUID) == "" {
		writeError(w, http.StatusBadRequest, "device_uuid is required")
		return
	}

	hash := auth.HashRefreshToken(refreshToken)
	ip := clientIP(r)

	// Check for replayed (already-rotated) token — indicates theft.
	stolenUserID, _, found, err := repository.DeviceFindByPreviousRefreshTokenHash(r.Context(), h.db, hash)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if found {
		// TODO: Deleted data provides no audit log!
		ids, _ := repository.DevicesDeleteForUser(r.Context(), h.db, stolenUserID)
		h.guard.RevokeMany(ids, auth.AccessTokenTTL)
		slog.Warn("refresh token reuse detected — all devices revoked",
			"user_id", stolenUserID, "ip", ip)
		clearAuthCookies(w, h.production)
		writeError(w, http.StatusUnauthorized, "refresh token reuse detected; all devices have been revoked")
		return
	}

	// Normal path: find the active device.
	device, err := repository.DeviceFindByRefreshTokenHash(r.Context(), h.db, hash)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid or expired refresh token")
		return
	}

	// Verify device_uuid matches — mismatch indicates stolen token.
	// This is a little heavy-handed, maybe we should just revoke the offending device instead of all devices?
	if device.DeviceUUID != deviceUUID {
		ids, _ := repository.DevicesDeleteForUser(r.Context(), h.db, device.UserID)
		h.guard.RevokeMany(ids, auth.AccessTokenTTL)
		slog.Warn("SECURITY: device_uuid mismatch on refresh — all devices revoked",
			"user_id", device.UserID, "expected_uuid", device.DeviceUUID,
			"received_uuid", deviceUUID, "revoked_devices", len(ids), "ip", ip)
		clearAuthCookies(w, h.production)
		writeError(w, http.StatusUnauthorized, "device mismatch; all devices have been revoked")
		return
	}

	if device.ExpiresAt != nil && time.Now().After(*device.ExpiresAt) {
		writeError(w, http.StatusUnauthorized, "refresh token expired")
		return
	}

	localAccessOnly, err := repository.UserIsLocalOnly(r.Context(), h.db, device.UserID)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "user lookup failed")
		return
	}
	if localAccessOnly && !isLocalRequest(r) {
		clearAuthCookies(w, h.production)
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	isAdmin, err := repository.UserIsAdmin(r.Context(), h.db, device.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "user lookup failed")
		return
	}

	rawRefresh, hashRefresh, err := auth.GenerateRefreshToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	entry := models.AccessHistoryEntry{
		IP:       ip,
		Agent:    r.Header.Get("User-Agent"),
		LastSeen: time.Now(),
	}
	if err := repository.DeviceUpdateSession(r.Context(), h.db, device.ID, hashRefresh, hash, time.Now().Add(auth.RefreshTokenTTL), entry, ""); err != nil {
		writeError(w, http.StatusInternalServerError, "token rotation failed")
		return
	}

	repository.UserTouchLastSeen(r.Context(), h.db, device.UserID)

	accessToken, err := auth.IssueToken(device.UserID, device.ID, isAdmin, h.secret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	setAuthCookies(w, accessToken, rawRefresh, h.production)

	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":             accessToken,
		"access_token_expires_in":  int(auth.AccessTokenTTL.Seconds()),
		"refresh_token":            rawRefresh,
		"refresh_token_expires_in": int(auth.RefreshTokenTTL.Seconds()),
		"device_id":                device.ID,
	})
}

// POST /api/v1/auth/logout
func (h *authHandler) logout(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	// Delete the device row so its refresh token becomes invalid immediately.
	_, _ = repository.DeviceDelete(r.Context(), h.db, claims.DeviceID, userID)
	// Revoke the access token for its remaining TTL via the in-memory guard.
	h.guard.Revoke(claims.DeviceID, auth.AccessTokenTTL)

	clearAuthCookies(w, h.production)
	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/auth/devices
func (h *authHandler) listDevices(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	h.writeDeviceList(w, r, userID)
}

// DELETE /api/v1/auth/devices/{id}
func (h *authHandler) deleteDevice(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	deviceID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	if deviceID == claims.DeviceID {
		writeError(w, http.StatusForbidden, "cannot delete your own active device")
		return
	}

	affected, err := repository.DeviceDelete(r.Context(), h.db, deviceID, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if affected == 0 {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	h.guard.Revoke(deviceID, auth.AccessTokenTTL)
	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/auth/devices/{id}/ban
func (h *authHandler) banDevice(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	deviceID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	if deviceID == claims.DeviceID {
		writeError(w, http.StatusForbidden, "cannot ban your own device")
		return
	}

	if err := repository.DeviceBan(r.Context(), h.db, deviceID, userID); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	h.guard.Ban(deviceID)
	w.WriteHeader(http.StatusNoContent)
}

// DELETE /api/v1/auth/devices/{id}/ban
func (h *authHandler) unbanDevice(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	deviceID, ok := urlIntParam(w, r, "id")
	if !ok {
		return
	}

	if err := repository.DeviceUnban(r.Context(), h.db, deviceID, userID); err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	h.guard.Unban(deviceID)
	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/auth/credentials
func (h *authHandler) changeCredentials(w http.ResponseWriter, r *http.Request) {
	claims := ClaimsFromContext(r.Context())
	userID, _ := strconv.ParseInt(claims.Subject, 10, 64)

	providerName, err := repository.UserAuthProvider(r.Context(), h.db, userID)
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
	if !decodeJSON(w, r, &body) {
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

	user, err := repository.UserGet(r.Context(), h.db, userID)
	if err != nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":        user.ID,
		"name":      user.Name,
		"is_admin":  user.IsAdmin,
		"device_id": claims.DeviceID,
	})
}

// writeDeviceList is shared between the user and admin device-listing handlers.
func (h *authHandler) writeDeviceList(w http.ResponseWriter, r *http.Request, userID int64) {
	devices, err := repository.DevicesGetForUser(r.Context(), h.db, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, devices)
}
