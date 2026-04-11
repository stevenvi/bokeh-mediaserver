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

