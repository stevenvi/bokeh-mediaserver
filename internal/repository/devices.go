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

const maxDevicesPerUser = 15

type DeviceRepository struct {
	db utils.DBTX
}

func NewDeviceRepository(db utils.DBTX) *DeviceRepository {
	return &DeviceRepository{db: db}
}

// FindByUserAndUUID returns a device by (userID, device_uuid), or ErrNotFound.
func (r *DeviceRepository) FindByUserAndUUID(ctx context.Context, userID int64, uuid string) (*models.Device, error) {
	d := &models.Device{}
	err := r.db.QueryRow(ctx,
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

// FindByRefreshTokenHash looks up a device by its current refresh_token_hash.
func (r *DeviceRepository) FindByRefreshTokenHash(ctx context.Context, hash string) (*models.Device, error) {
	d := &models.Device{}
	err := r.db.QueryRow(ctx,
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

// FindByPreviousRefreshTokenHash checks for a replayed (already-rotated) refresh token.
// Returns userID, deviceID, found, error.
func (r *DeviceRepository) FindByPreviousRefreshTokenHash(ctx context.Context, hash string) (userID, deviceID int64, found bool, err error) {
	err = r.db.QueryRow(ctx,
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

// Create inserts a new device and returns its ID.
func (r *DeviceRepository) Create(ctx context.Context, userID int64, uuid, name, tokenHash string, expiresAt time.Time, entry models.AccessHistoryEntry) (int64, error) {
	historyJSON, err := json.Marshal([]models.AccessHistoryEntry{entry})
	if err != nil {
		return 0, fmt.Errorf("marshal access history: %w", err)
	}

	var id int64
	err = r.db.QueryRow(ctx,
		`INSERT INTO devices (user_id, device_uuid, device_name, refresh_token_hash, expires_at, access_history)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id`,
		userID, uuid, name, tokenHash, expiresAt, historyJSON,
	).Scan(&id)
	return id, err
}

// UpdateSession rotates the token and updates last_seen_at and access_history for an existing device.
// If deviceName is non-empty, the device's display name is updated as well.
func (r *DeviceRepository) UpdateSession(ctx context.Context, deviceID int64, newHash, oldHash string, expiresAt time.Time, entry models.AccessHistoryEntry, deviceName string) error {
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal access history entry: %w", err)
	}

	_, err = r.db.Exec(ctx,
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

// CountActiveForUser returns the number of non-banned devices for a user.
func (r *DeviceRepository) CountActiveForUser(ctx context.Context, userID int64) (int, error) {
	var count int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM devices WHERE user_id = $1 AND banned_at IS NULL`,
		userID,
	).Scan(&count)
	return count, err
}

// EvictLRU deletes the oldest non-banned device for a user and returns its ID.
func (r *DeviceRepository) EvictLRU(ctx context.Context, userID int64) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
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

// ListForUser returns all devices for a user (including banned), ordered by last_seen_at desc.
func (r *DeviceRepository) ListForUser(ctx context.Context, userID int64) ([]models.DeviceView, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, device_name, banned_at, last_seen_at, created_at, access_history
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

// Delete removes a device scoped to a user. Returns rows affected.
func (r *DeviceRepository) Delete(ctx context.Context, deviceID, userID int64) (int64, error) {
	tag, err := r.db.Exec(ctx,
		`DELETE FROM devices WHERE id = $1 AND user_id = $2`,
		deviceID, userID,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// DeleteAllForUser removes all devices for a user and returns their IDs.
func (r *DeviceRepository) DeleteAllForUser(ctx context.Context, userID int64) ([]int64, error) {
	rows, err := r.db.Query(ctx,
		`DELETE FROM devices WHERE user_id = $1 RETURNING id`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// Ban sets banned_at and nulls out token fields for a device.
func (r *DeviceRepository) Ban(ctx context.Context, deviceID, userID int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE devices
		 SET banned_at = now(),
		     refresh_token_hash = NULL, previous_refresh_token_hash = NULL, expires_at = NULL
		 WHERE id = $1 AND user_id = $2`,
		deviceID, userID,
	)
	return err
}

// Unban clears banned_at for a device.
func (r *DeviceRepository) Unban(ctx context.Context, deviceID, userID int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE devices SET banned_at = NULL WHERE id = $1 AND user_id = $2`,
		deviceID, userID,
	)
	return err
}

// LoadBannedIDs returns all device IDs where banned_at IS NOT NULL.
func (r *DeviceRepository) LoadBannedIDs(ctx context.Context) ([]int64, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id FROM devices WHERE banned_at IS NOT NULL`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// ListStaleNonBanned returns IDs of non-banned devices not seen since cutoff.
func (r *DeviceRepository) ListStaleNonBanned(ctx context.Context, cutoff time.Time) ([]int64, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id FROM devices WHERE banned_at IS NULL AND last_seen_at < $1`,
		cutoff,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// DeleteByIDs deletes devices with the given IDs.
func (r *DeviceRepository) DeleteByIDs(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.db.Exec(ctx, `DELETE FROM devices WHERE id = ANY($1)`, ids)
	return err
}

// MaxDevicesPerUser is the maximum number of non-banned devices per user before LRU eviction.
const MaxDevicesPerUser = maxDevicesPerUser
