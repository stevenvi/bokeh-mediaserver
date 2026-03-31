package repository_test

import (
	"testing"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeviceFindByUserAndUUID(t *testing.T) {
	t.Run("finds_existing_device", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		_, uuid, _ := createDevice(t, db, userID)

		d, err := repository.DeviceFindByUserAndUUID(bg(), db, userID, uuid)
		require.NoError(t, err)
		assert.Equal(t, uuid, d.DeviceUUID)
		assert.Equal(t, userID, d.UserID)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		_, err := repository.DeviceFindByUserAndUUID(bg(), db, userID, "no-such-uuid")
		assert.ErrorIs(t, err, repository.ErrNotFound)
	})
}

func TestDeviceFindByRefreshTokenHash(t *testing.T) {
	t.Run("finds_device_by_token", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, _, tokenHash := createDevice(t, db, userID)

		d, err := repository.DeviceFindByRefreshTokenHash(bg(), db, tokenHash)
		require.NoError(t, err)
		assert.Equal(t, id, d.ID)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.DeviceFindByRefreshTokenHash(bg(), db, "no-such-hash")
		assert.ErrorIs(t, err, repository.ErrNotFound)
	})
}

func TestDeviceFindByPreviousRefreshTokenHash(t *testing.T) {
	t.Run("finds_device_by_previous_hash", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, _, oldHash := createDevice(t, db, userID)

		// Rotate the token so previous_refresh_token_hash is populated.
		err := repository.DeviceUpdateSession(bg(), db, id, "newhash", oldHash,
			time.Now().Add(90*24*time.Hour), testAccessEntry(), "")
		require.NoError(t, err)

		foundUserID, foundDeviceID, found, err := repository.DeviceFindByPreviousRefreshTokenHash(bg(), db, oldHash)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, userID, foundUserID)
		assert.Equal(t, id, foundDeviceID)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, _, found, err := repository.DeviceFindByPreviousRefreshTokenHash(bg(), db, "no-prev-hash")
		require.NoError(t, err)
		assert.False(t, found)
	})
}

func TestDeviceCreate(t *testing.T) {
	t.Run("creates_device", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, uuid, _ := createDevice(t, db, userID)
		assert.Greater(t, id, int64(0))

		d, err := repository.DeviceFindByUserAndUUID(bg(), db, userID, uuid)
		require.NoError(t, err)
		assert.Equal(t, "Test Device", d.DeviceName)
		assert.Nil(t, d.BannedAt)
	})
}

func TestDeviceUpdateSession(t *testing.T) {
	t.Run("rotates_token_and_keeps_old_as_previous", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, uuid, oldHash := createDevice(t, db, userID)

		err := repository.DeviceUpdateSession(bg(), db, id, "newhash", oldHash,
			time.Now().Add(90*24*time.Hour), testAccessEntry(), "")
		require.NoError(t, err)

		d, err := repository.DeviceFindByUserAndUUID(bg(), db, userID, uuid)
		require.NoError(t, err)
		require.NotNil(t, d.RefreshTokenHash)
		assert.Equal(t, "newhash", *d.RefreshTokenHash)
		require.NotNil(t, d.PreviousRefreshTokenHash)
		assert.Equal(t, oldHash, *d.PreviousRefreshTokenHash)
	})

	t.Run("updates_device_name_when_provided", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, uuid, oldHash := createDevice(t, db, userID)

		err := repository.DeviceUpdateSession(bg(), db, id, "newhash2", oldHash,
			time.Now().Add(90*24*time.Hour), testAccessEntry(), "My MacBook")
		require.NoError(t, err)

		d, err := repository.DeviceFindByUserAndUUID(bg(), db, userID, uuid)
		require.NoError(t, err)
		assert.Equal(t, "My MacBook", d.DeviceName)
	})
}

func TestDevicesCountActiveForUser(t *testing.T) {
	t.Run("counts_non_banned_devices", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id1, _, _ := createDevice(t, db, userID)
		_, _, _ = createDevice(t, db, userID)
		require.NoError(t, repository.DeviceBan(bg(), db, id1, userID))

		count, err := repository.DevicesCountActiveForUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("zero_for_user_with_no_devices", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		count, err := repository.DevicesCountActiveForUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestDeviceEvictLRU(t *testing.T) {
	t.Run("evicts_oldest_non_banned_device", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id1, _, _ := createDevice(t, db, userID)
		_, _, _ = createDevice(t, db, userID)
		testutil.MustExec(t, db,
			`UPDATE devices SET last_seen_at = now() - interval '1 hour' WHERE id = $1`, id1)

		evictedID, err := repository.DeviceEvictLRU(bg(), db, userID)
		require.NoError(t, err)
		assert.Equal(t, id1, evictedID)
	})

	t.Run("returns_zero_when_no_devices", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, err := repository.DeviceEvictLRU(bg(), db, userID)
		require.NoError(t, err)
		assert.Equal(t, int64(0), id)
	})
}

func TestDevicesGetForUser(t *testing.T) {
	t.Run("returns_all_devices_including_banned", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id1, _, _ := createDevice(t, db, userID)
		id2, _, _ := createDevice(t, db, userID)
		require.NoError(t, repository.DeviceBan(bg(), db, id1, userID))

		devices, err := repository.DevicesGetForUser(bg(), db, userID)
		require.NoError(t, err)
		require.Len(t, devices, 2)
		ids := []int64{devices[0].ID, devices[1].ID}
		assert.Contains(t, ids, id1)
		assert.Contains(t, ids, id2)
	})

	t.Run("empty_for_user_with_no_devices", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		devices, err := repository.DevicesGetForUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Empty(t, devices)
	})
}

func TestDeviceDelete(t *testing.T) {
	t.Run("deletes_device", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, _, _ := createDevice(t, db, userID)

		rows, err := repository.DeviceDelete(bg(), db, id, userID)
		require.NoError(t, err)
		assert.Equal(t, int64(1), rows)

		count, err := repository.DevicesCountActiveForUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("zero_rows_for_wrong_user", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		u1 := createUser(t, db)
		u2 := createUser(t, db)
		id, _, _ := createDevice(t, db, u1)

		rows, err := repository.DeviceDelete(bg(), db, id, u2)
		require.NoError(t, err)
		assert.Equal(t, int64(0), rows)
	})
}

func TestDevicesDeleteForUser(t *testing.T) {
	t.Run("deletes_all_devices_and_returns_ids", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id1, _, _ := createDevice(t, db, userID)
		id2, _, _ := createDevice(t, db, userID)

		ids, err := repository.DevicesDeleteForUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Contains(t, ids, id1)
		assert.Contains(t, ids, id2)

		count, err := repository.DevicesCountActiveForUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("empty_when_no_devices", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		ids, err := repository.DevicesDeleteForUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Empty(t, ids)
	})
}

func TestDeviceBan(t *testing.T) {
	t.Run("sets_banned_at_and_clears_token", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, uuid, _ := createDevice(t, db, userID)

		require.NoError(t, repository.DeviceBan(bg(), db, id, userID))

		d, err := repository.DeviceFindByUserAndUUID(bg(), db, userID, uuid)
		require.NoError(t, err)
		assert.NotNil(t, d.BannedAt)
		assert.Nil(t, d.RefreshTokenHash)
	})

	t.Run("banned_device_not_counted_as_active", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, _, _ := createDevice(t, db, userID)
		require.NoError(t, repository.DeviceBan(bg(), db, id, userID))

		count, err := repository.DevicesCountActiveForUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestDeviceUnban(t *testing.T) {
	t.Run("clears_banned_at", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, uuid, _ := createDevice(t, db, userID)
		require.NoError(t, repository.DeviceBan(bg(), db, id, userID))
		require.NoError(t, repository.DeviceUnban(bg(), db, id, userID))

		d, err := repository.DeviceFindByUserAndUUID(bg(), db, userID, uuid)
		require.NoError(t, err)
		assert.Nil(t, d.BannedAt)
	})

	t.Run("noop_for_non_banned_device", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, _, _ := createDevice(t, db, userID)
		require.NoError(t, repository.DeviceUnban(bg(), db, id, userID))
	})
}

func TestDeviceGetBannedIDs(t *testing.T) {
	t.Run("includes_banned_device", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, _, _ := createDevice(t, db, userID)
		require.NoError(t, repository.DeviceBan(bg(), db, id, userID))

		ids, err := repository.DeviceGetBannedIDs(bg(), db)
		require.NoError(t, err)
		assert.Contains(t, ids, id)
	})

	t.Run("excludes_active_devices", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, _, _ := createDevice(t, db, userID)

		ids, err := repository.DeviceGetBannedIDs(bg(), db)
		require.NoError(t, err)
		assert.NotContains(t, ids, id)
	})
}

func TestDevicesStaleNonBanned(t *testing.T) {
	t.Run("returns_old_devices", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, _, _ := createDevice(t, db, userID)
		testutil.MustExec(t, db,
			`UPDATE devices SET last_seen_at = now() - interval '400 days' WHERE id = $1`, id)

		cutoff := time.Now().Add(-30 * 24 * time.Hour)
		ids, err := repository.DevicesStaleNonBanned(bg(), db, cutoff)
		require.NoError(t, err)
		assert.Contains(t, ids, id)
	})

	t.Run("excludes_recently_seen_devices", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id, _, _ := createDevice(t, db, userID)

		cutoff := time.Now().Add(-30 * 24 * time.Hour)
		ids, err := repository.DevicesStaleNonBanned(bg(), db, cutoff)
		require.NoError(t, err)
		assert.NotContains(t, ids, id)
	})
}

func TestDevicesDeleteByID(t *testing.T) {
	t.Run("deletes_specified_devices", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		id1, _, _ := createDevice(t, db, userID)
		id2, _, _ := createDevice(t, db, userID)

		require.NoError(t, repository.DevicesDeleteByID(bg(), db, []int64{id1, id2}))

		count, err := repository.DevicesCountActiveForUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("noop_for_empty_slice", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		require.NoError(t, repository.DevicesDeleteByID(bg(), db, []int64{}))
	})
}

// ─── Helper ───────────────────────────────────────────────────────────────────

func testAccessEntry() models.AccessHistoryEntry {
	return models.AccessHistoryEntry{
		LastSeen: time.Now(),
		IP:       "127.0.0.1",
		Agent:    "test-agent",
	}
}
