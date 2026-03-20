package api

import (
	"sync"
	"time"
)

const (
	maxAttempts    = 5
	attemptWindow  = time.Minute
	lockDuration   = 30 * time.Minute
)

// loginRateLimiter tracks failed login attempts per IP and temporarily locks
// IPs that exceed the threshold.
// TODO: An attacker could use this to cause memory overflow by spamming attempts with random IPs. Should use a sensible LRU cache.
type loginRateLimiter struct {
	mutex    sync.Mutex
	attempts map[string][]time.Time
	locked   map[string]time.Time
}

func newLoginRateLimiter() *loginRateLimiter {
	return &loginRateLimiter{
		attempts: make(map[string][]time.Time),
		locked:   make(map[string]time.Time),
	}
}

// IsLocked returns true if the IP is currently rate-limited.
func (this *loginRateLimiter) IsLocked(ip string) bool {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	until, ok := this.locked[ip]
	if !ok {
		return false
	}
	if time.Now().Before(until) {
		return true
	}
	delete(this.locked, ip)
	return false
}

// Record records a failed login attempt for an IP.
// Returns true if this attempt caused the IP to become locked.
func (this *loginRateLimiter) Record(ip string) bool {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	now := time.Now()
	cutoff := now.Add(-attemptWindow)

	// Keep only attempts within the window.
	existing := this.attempts[ip]
	filtered := existing[:0]
	for _, t := range existing {
		if t.After(cutoff) {
			filtered = append(filtered, t)
		}
	}
	filtered = append(filtered, now)
	this.attempts[ip] = filtered

	if len(filtered) >= maxAttempts {
		this.locked[ip] = now.Add(lockDuration)
		delete(this.attempts, ip)
		return true
	}
	return false
}
