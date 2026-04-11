package jobs_test

import (
	"context"
	"testing"
	"time"

	db_test_utils "github.com/stevenvi/bokeh-mediaserver/internal/db/utils"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stretchr/testify/assert"
)

// TestScheduler_StartStop verifies the scheduler can start and stop cleanly.
func TestScheduler_StartStop(t *testing.T) {
	pool := db_test_utils.TestPool
	if pool == nil {
		t.Skip("no test database available")
	}

	dispatcher := jobs.NewDispatcher(pool)
	s := jobs.NewScheduler(pool, dispatcher)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.Start(ctx)
	s.NotifyReload()
	s.Stop()
}

// TestScheduler_NotifyReload verifies NotifyReload doesn't block when called multiple times.
func TestScheduler_NotifyReload(t *testing.T) {
	pool := db_test_utils.TestPool
	if pool == nil {
		t.Skip("no test database available")
	}

	dispatcher := jobs.NewDispatcher(pool)
	s := jobs.NewScheduler(pool, dispatcher)

	// NotifyReload before Start should not block (buffered channel).
	for i := 0; i < 10; i++ {
		s.NotifyReload()
	}
	assert.NotNil(t, s)
}
