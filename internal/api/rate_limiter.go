package api

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	maxAttempts         = 5
	attemptWindow       = time.Minute
	lockDuration        = 30 * time.Minute
	rateLimiterCapacity = 10_000 // bounds memory to ~2 MB across both caches
)

// loginRateLimiter tracks failed login attempts per IP and temporarily locks
// IPs that exceed the threshold. Both caches are bounded LRU to prevent memory
// exhaustion via random IPs.
type loginRateLimiter struct {
	attempts *lru.Cache[string, []time.Time]
	locked   *lru.Cache[string, time.Time]
	mutex    sync.Mutex
}

func newLoginRateLimiter() *loginRateLimiter {
	attempts, _ := lru.New[string, []time.Time](rateLimiterCapacity)
	locked, _ := lru.New[string, time.Time](rateLimiterCapacity)
	return &loginRateLimiter{
		attempts: attempts,
		locked:   locked,
	}
}

// IsLocked returns true if the IP is currently rate-limited.
func (this *loginRateLimiter) IsLocked(ip string) bool {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	until, ok := this.locked.Get(ip)
	if !ok {
		return false
	}
	if time.Now().Before(until) {
		return true
	}
	this.locked.Remove(ip)
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
	existing, _ := this.attempts.Get(ip)
	filtered := existing[:0]
	for _, t := range existing {
		if t.After(cutoff) {
			filtered = append(filtered, t)
		}
	}
	filtered = append(filtered, now)
	this.attempts.Add(ip, filtered)

	if len(filtered) >= maxAttempts {
		this.locked.Add(ip, now.Add(lockDuration))
		this.attempts.Remove(ip)
		return true
	}
	return false
}
