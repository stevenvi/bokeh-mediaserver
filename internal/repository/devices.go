package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// "Fifteen should be more than enough for any user"
const maxDevicesPerUser = 15

// DeviceFindByUserAndUUID returns a device by (userID, device_uuid), or ErrNotFound.
func DeviceFindByUserAndUUID(ctx context.Context, db utils.DBTX, userID int64, uuid string) (*models.Device, error) {
	d := &models.Device{}
	err := db.QueryRow(ctx,
		`SELECT id, device_uuid, user_id, refresh_token_hash, previous_refresh_token_hash,
		        expires_at, device_name, banned_at,
		        access_history, created_at, last_seen_at
		 FROM devices
		 WHERE user_id = $1 AND device_uuid = $2`,
		userID, uuid,
	).Scan(
		&d.ID, &d.DeviceUUID, &d.UserID, &d.RefreshTokenHash, &d.PreviousRefreshTokenHash,
		&d.ExpiresAt, &d.DeviceName, &d.BannedAt,
		&d.AccessHistory, &d.CreatedAt, &d.LastSeenAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return d, err
}

// DeviceFindByRefreshTokenHash looks up a device by its current refresh_token_hash.
func DeviceFindByRefreshTokenHash(ctx context.Context, db utils.DBTX, hash string) (*models.Device, error) {
	d := &models.Device{}
	err := db.QueryRow(ctx,
		`SELECT id, device_uuid, user_id, refresh_token_hash, previous_refresh_token_hash,
		        expires_at, device_name, banned_at,
		        access_history, created_at, last_seen_at
		 FROM devices
		 WHERE refresh_token_hash = $1`,
		hash,
	).Scan(
		&d.ID, &d.DeviceUUID, &d.UserID, &d.RefreshTokenHash, &d.PreviousRefreshTokenHash,
		&d.ExpiresAt, &d.DeviceName, &d.BannedAt,
		&d.AccessHistory, &d.CreatedAt, &d.LastSeenAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return d, err
}

// DeviceFindByPreviousRefreshTokenHash checks for a replayed (already-rotated) refresh token.
// Returns userID, deviceID, found, error.
func DeviceFindByPreviousRefreshTokenHash(ctx context.Context, db utils.DBTX, hash string) (userID, deviceID int64, found bool, err error) {
	err = db.QueryRow(ctx,
		`SELECT user_id, id FROM devices WHERE previous_refresh_token_hash = $1`, hash,
	).Scan(&userID, &deviceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, err
	}
	return userID, deviceID, true, nil
}

// DeviceCreate inserts a new device and returns its ID.
func DeviceCreate(ctx context.Context, db utils.DBTX, userID int64, uuid, name, tokenHash string, expiresAt time.Time, entry models.AccessHistoryEntry) (int64, error) {
	historyJSON, err := json.Marshal([]models.AccessHistoryEntry{entry})
	if err != nil {
		return 0, fmt.Errorf("marshal access history: %w", err)
	}

	var id int64
	err = db.QueryRow(ctx,
		`INSERT INTO devices (user_id, device_uuid, device_name, refresh_token_hash, expires_at, access_history)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id`,
		userID, uuid, name, tokenHash, expiresAt, historyJSON,
	).Scan(&id)
	return id, err
}

// DeviceUpdateSession rotates the token and updates last_seen_at and access_history for an existing device.
// If deviceName is non-empty, the device's display name is updated as well.
func DeviceUpdateSession(ctx context.Context, db utils.DBTX, deviceID int64, newHash, oldHash string, expiresAt time.Time, entry models.AccessHistoryEntry, deviceName string) error {
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal access history entry: %w", err)
	}

	_, err = db.Exec(ctx,
		`UPDATE devices
		 SET refresh_token_hash          = $2,
		     previous_refresh_token_hash = $3,
		     expires_at                  = $4,
		     last_seen_at                = now(),
		     device_name                 = CASE WHEN $6 != '' THEN $6 ELSE device_name END,
		     access_history              = (
		         SELECT jsonb_agg(e)
		         FROM (
		             SELECT $5::jsonb AS e
		             UNION ALL
		             SELECT elem FROM jsonb_array_elements(access_history) AS elem
		             LIMIT 5
		         ) sub
		     )
		 WHERE id = $1`,
		deviceID, newHash, oldHash, expiresAt, entryJSON, deviceName,
	)
	return err
}

// DevicesCountActiveForUser returns the number of non-banned devices for a user.
func DevicesCountActiveForUser(ctx context.Context, db utils.DBTX, userID int64) (int, error) {
	var count int
	err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM devices WHERE user_id = $1 AND banned_at IS NULL`,
		userID,
	).Scan(&count)
	return count, err
}

// DeviceEvictLRU deletes the oldest non-banned device for a user and returns its ID.
func DeviceEvictLRU(ctx context.Context, db utils.DBTX, userID int64) (int64, error) {
	var id int64
	err := db.QueryRow(ctx,
		`DELETE FROM devices
		 WHERE id = (
		     SELECT id FROM devices
		     WHERE user_id = $1 AND banned_at IS NULL
		     ORDER BY last_seen_at ASC
		     LIMIT 1
		 )
		 RETURNING id`,
		userID,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// DevicesGetForUser returns all devices for a user (including banned), ordered by last_seen_at desc.
func DevicesGetForUser(ctx context.Context, db utils.DBTX, userID int64) ([]models.DeviceView, error) {
	rows, err := db.Query(ctx,
		`SELECT 
			last_seen_at, 
			created_at, 
			banned_at, 
			device_name, 
			access_history,
			id
		 FROM devices
		 WHERE user_id = $1
		 ORDER BY last_seen_at DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.DeviceView])
}

// DeviceDelete removes a device scoped to a user. Returns rows affected.
func DeviceDelete(ctx context.Context, db utils.DBTX, deviceID, userID int64) (int64, error) {
	tag, err := db.Exec(ctx,
		`DELETE FROM devices WHERE id = $1 AND user_id = $2`,
		deviceID, userID,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// DevicesDeleteForUser removes all devices for a user and returns their IDs.
func DevicesDeleteForUser(ctx context.Context, db utils.DBTX, userID int64) ([]int64, error) {
	rows, err := db.Query(ctx,
		`DELETE FROM devices WHERE user_id = $1 RETURNING id`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// DeviceBan sets banned_at and nulls out token fields for a device.
func DeviceBan(ctx context.Context, db utils.DBTX, deviceID, userID int64) error {
	_, err := db.Exec(ctx,
		`UPDATE devices
		 SET banned_at = now(),
		     refresh_token_hash = NULL, previous_refresh_token_hash = NULL, expires_at = NULL
		 WHERE id = $1 AND user_id = $2`,
		deviceID, userID,
	)
	return err
}

// DeviceUnban clears banned_at for a device.
func DeviceUnban(ctx context.Context, db utils.DBTX, deviceID, userID int64) error {
	_, err := db.Exec(ctx,
		`UPDATE devices SET banned_at = NULL WHERE id = $1 AND user_id = $2`,
		deviceID, userID,
	)
	return err
}

// DeviceGetBannedIDs returns all device IDs where banned_at IS NOT NULL.
func DeviceGetBannedIDs(ctx context.Context, db utils.DBTX) ([]int64, error) {
	rows, err := db.Query(ctx,
		`SELECT id FROM devices WHERE banned_at IS NOT NULL`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// DevicesStaleNonBanned returns IDs of non-banned devices not seen since cutoff.
func DevicesStaleNonBanned(ctx context.Context, db utils.DBTX, cutoff time.Time) ([]int64, error) {
	rows, err := db.Query(ctx,
		`SELECT id FROM devices WHERE banned_at IS NULL AND last_seen_at < $1`,
		cutoff,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// DevicesDeleteByID deletes devices with the given IDs.
func DevicesDeleteByID(ctx context.Context, db utils.DBTX, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := db.Exec(ctx, `DELETE FROM devices WHERE id = ANY($1)`, ids)
	return err
}

// DevicesMaxPerUser is the maximum number of non-banned devices per user before LRU eviction.
const DevicesMaxPerUser = maxDevicesPerUser
