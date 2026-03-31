package api

import (
	"context"
	"sync"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// DeviceGuard provides O(1) in-memory checks for revoked and banned device IDs.
// Revoked entries are pruned lazily when their TTL expires.
// Banned entries persist until explicitly unbanned.
// TODO: An attacker could use this to cause memory overflow by spamming attempts from random IPs. Should use a bounded LRU cache.
type DeviceGuard struct {
	revoked map[int64]time.Time // device_id → access token expiry time
	banned  map[int64]struct{}  // device_id → permanently blocked until unbanned
	mutex   sync.RWMutex
}

// NewDeviceGuard creates a new DeviceGuard.
func NewDeviceGuard() *DeviceGuard {
	return &DeviceGuard{
		revoked: make(map[int64]time.Time),
		banned:  make(map[int64]struct{}),
	}
}

// LoadBanned populates the banned set from the database at startup.
func (this *DeviceGuard) LoadBanned(ctx context.Context, db utils.DBTX) error {
	ids, err := repository.DeviceGetBannedIDs(ctx, db)
	if err != nil {
		return err
	}
	this.mutex.Lock()
	defer this.mutex.Unlock()
	for _, id := range ids {
		this.banned[id] = struct{}{}
	}
	return nil
}

// Revoke marks a device as revoked until the given TTL expires.
// Used when a device is deleted so existing access tokens are blocked.
func (this *DeviceGuard) Revoke(id int64, ttl time.Duration) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.revoked[id] = time.Now().Add(ttl)
}

// RevokeMany calls Revoke for each ID.
func (this *DeviceGuard) RevokeMany(ids []int64, ttl time.Duration) {
	expiry := time.Now().Add(ttl)
	this.mutex.Lock()
	defer this.mutex.Unlock()
	for _, id := range ids {
		this.revoked[id] = expiry
	}
}

// Ban permanently blocks a device until Unban is called.
func (this *DeviceGuard) Ban(id int64) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.banned[id] = struct{}{}
	delete(this.revoked, id) // ban supersedes revoke
}

// Unban removes a device from the banned set.
func (this *DeviceGuard) Unban(id int64) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	delete(this.banned, id)
}

// IsBlocked returns true if the device is banned or has an active revocation entry.
// Expired revocation entries are pruned lazily on read.
func (this *DeviceGuard) IsBlocked(id int64) bool {
	this.mutex.RLock()
	_, banned := this.banned[id]
	if banned {
		this.mutex.RUnlock()
		return true
	}
	expiry, revoked := this.revoked[id]
	this.mutex.RUnlock()

	if !revoked {
		return false
	}
	if time.Now().Before(expiry) {
		return true
	}
	// Entry expired — prune it.
	this.mutex.Lock()
	if exp, ok := this.revoked[id]; ok && !time.Now().Before(exp) {
		delete(this.revoked, id)
	}
	this.mutex.Unlock()
	return false
}
