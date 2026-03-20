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

	"github.com/go-chi/chi/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/auth"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type authHandler struct {
	db      utils.DBTX
	users   *repository.UserRepository
	devices *repository.DeviceRepository
	guard   *DeviceGuard
	rl      *loginRateLimiter
	plugins map[string]auth.Plugin
	secret  string
}

func newAuthHandler(db utils.DBTX, users *repository.UserRepository, devices *repository.DeviceRepository, guard *DeviceGuard, rl *loginRateLimiter, secret string, plugins map[string]auth.Plugin) *authHandler {
	return &authHandler{db: db, users: users, devices: devices, guard: guard, rl: rl, secret: secret, plugins: plugins}
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
		DeviceUUID  string          `json:"device_uuid"`
		DeviceName  string          `json:"device_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
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
		writeError(w, http.StatusTooManyRequests, "too many login attempts; try again later")
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

	isAdmin, err := h.users.GetAdminStatus(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "user lookup failed")
		return
	}

	// Look up existing device for this (userID, device_uuid).
	existing, err := h.devices.FindByUserAndUUID(r.Context(), userID, body.DeviceUUID)
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
		count, err := h.devices.CountActiveForUser(r.Context(), userID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "device count failed")
			return
		}
		if count >= repository.MaxDevicesPerUser {
			evictedID, err := h.devices.EvictLRU(r.Context(), userID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "device eviction failed")
				return
			}
			if evictedID != 0 {
				h.guard.Revoke(evictedID, auth.AccessTokenTTL)
			}
		}
		deviceID, err = h.devices.Create(r.Context(), userID, body.DeviceUUID, body.DeviceName, hashRefresh, expiresAt, entry)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "device creation failed")
			return
		}
	} else {
		deviceID = existing.ID
		if err := h.devices.UpdateSession(r.Context(), deviceID, hashRefresh, "", expiresAt, entry); err != nil {
			writeError(w, http.StatusInternalServerError, "session update failed")
			return
		}
	}

	h.users.TouchLastSeen(r.Context(), userID)

	accessToken, err := auth.IssueToken(userID, deviceID, isAdmin, h.secret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

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
	var body struct {
		RefreshToken string `json:"refresh_token"`
		DeviceUUID   string `json:"device_uuid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RefreshToken == "" {
		writeError(w, http.StatusBadRequest, "refresh_token is required")
		return
	}
	if strings.TrimSpace(body.DeviceUUID) == "" {
		writeError(w, http.StatusBadRequest, "device_uuid is required")
		return
	}

	hash := auth.HashRefreshToken(body.RefreshToken)
	ip := clientIP(r)

	// Check for replayed (already-rotated) token — indicates theft.
	stolenUserID, _, found, err := h.devices.FindByPreviousRefreshTokenHash(r.Context(), hash)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if found {
		// TODO: Deleted data provides no audit log!
		ids, _ := h.devices.DeleteAllForUser(r.Context(), stolenUserID)
		h.guard.RevokeMany(ids, auth.AccessTokenTTL)
		slog.Warn("refresh token reuse detected — all devices revoked",
			"user_id", stolenUserID, "ip", ip)
		writeError(w, http.StatusUnauthorized, "refresh token reuse detected; all devices have been revoked")
		return
	}

	// Normal path: find the active device.
	device, err := h.devices.FindByRefreshTokenHash(r.Context(), hash)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid or expired refresh token")
		return
	}

	// Verify device_uuid matches — mismatch indicates stolen token.
	// This is a little heavy-handed, maybe we should just revoke the offending device instead of all devices?
	// TODO: Deleted data provides no audit log!
	if device.DeviceUUID != body.DeviceUUID {
		ids, _ := h.devices.DeleteAllForUser(r.Context(), device.UserID)
		h.guard.RevokeMany(ids, auth.AccessTokenTTL)
		slog.Warn("refresh device_uuid mismatch — all devices revoked",
			"user_id", device.UserID, "ip", ip)
		writeError(w, http.StatusUnauthorized, "device mismatch; all devices have been revoked")
		return
	}

	if device.ExpiresAt != nil && time.Now().After(*device.ExpiresAt) {
		writeError(w, http.StatusUnauthorized, "refresh token expired")
		return
	}

	isAdmin, err := h.users.GetAdminStatus(r.Context(), device.UserID)
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
	if err := h.devices.UpdateSession(r.Context(), device.ID, hashRefresh, hash, time.Now().Add(auth.RefreshTokenTTL), entry); err != nil {
		writeError(w, http.StatusInternalServerError, "token rotation failed")
		return
	}

	h.users.TouchLastSeen(r.Context(), device.UserID)

	accessToken, err := auth.IssueToken(device.UserID, device.ID, isAdmin, h.secret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":             accessToken,
		"access_token_expires_in":  int(auth.AccessTokenTTL.Seconds()),
		"refresh_token":            rawRefresh,
		"refresh_token_expires_in": int(auth.RefreshTokenTTL.Seconds()),
		"device_id":                device.ID,
	})
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

	deviceID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid device id")
		return
	}

	affected, err := h.devices.Delete(r.Context(), deviceID, userID)
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

	deviceID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid device id")
		return
	}

	if deviceID == claims.DeviceID {
		writeError(w, http.StatusForbidden, "cannot ban your own device")
		return
	}

	if err := h.devices.Ban(r.Context(), deviceID, userID); err != nil {
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

	deviceID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid device id")
		return
	}

	if err := h.devices.Unban(r.Context(), deviceID, userID); err != nil {
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

// writeDeviceList is shared between the user and admin device-listing handlers.
func (h *authHandler) writeDeviceList(w http.ResponseWriter, r *http.Request, userID int64) {
	devices, err := h.devices.ListForUser(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, devices)
}

// nullableString returns nil for empty strings (maps to SQL NULL).
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
