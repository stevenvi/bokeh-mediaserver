package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// AudioShowBookmarkUpsert inserts or updates a user's playback position within an audio show.
// artistID identifies the show (artist = show, albums = groupings within the show).
func AudioShowBookmarkUpsert(ctx context.Context, db utils.DBTX, userID, artistID, itemID int64, positionSeconds int) error {
	_, err := db.Exec(ctx,
		`INSERT INTO audio_show_bookmarks (user_id, artist_id, media_item_id, position_seconds)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (user_id, artist_id) DO UPDATE SET
		     media_item_id    = EXCLUDED.media_item_id,
		     position_seconds = EXCLUDED.position_seconds,
		     last_listened_at = NOW()`,
		userID, artistID, itemID, positionSeconds,
	)
	return err
}

// AudioShowBookmarkGet returns the user's current position within a show, or nil if none exists.
func AudioShowBookmarkGet(ctx context.Context, db utils.DBTX, userID, artistID int64) (*models.ShowBookmark, error) {
	var b models.ShowBookmark
	err := db.QueryRow(ctx,
		`SELECT media_item_id, position_seconds, last_listened_at
		 FROM audio_show_bookmarks
		 WHERE user_id = $1 AND artist_id = $2`,
		userID, artistID,
	).Scan(&b.MediaItemID, &b.PositionSeconds, &b.LastListenedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}
