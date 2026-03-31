package repository_test

import (
	"testing"

	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// server_config always has a single seeded row (id=1) created by migrations.
// These tests read from that row; they do not exercise an "empty" path.

func TestServerConfigTranscodeBitrate(t *testing.T) {
	t.Run("returns_default_bitrate", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		kbps, err := repository.ServerConfigTranscodeBitrate(bg(), db)
		require.NoError(t, err)
		assert.Greater(t, kbps, 0)
	})
}

func TestServerConfigSchedules(t *testing.T) {
	t.Run("returns_schedule_map", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		schedules, err := repository.ServerConfigSchedules(bg(), db)
		require.NoError(t, err)
		assert.Contains(t, schedules, "scan_schedule")
		assert.Contains(t, schedules, "integrity_schedule")
		assert.Contains(t, schedules, "device_cleanup_schedule")
		assert.Contains(t, schedules, "cover_cycle_schedule")
	})
}
