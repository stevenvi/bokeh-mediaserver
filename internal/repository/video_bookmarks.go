package repository

import (
	"context"

	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// VideoBookmarkUpsert inserts or updates a user's playback position for a video.
func VideoBookmarkUpsert(ctx context.Context, db utils.DBTX, userID, itemID int64, positionSeconds int) error {
	_, err := db.Exec(ctx,
		`INSERT INTO video_bookmarks (user_id, media_item_id, position_seconds)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (user_id, media_item_id) DO UPDATE SET
		     position_seconds = EXCLUDED.position_seconds,
		     last_watched_at  = NOW()`,
		userID, itemID, positionSeconds,
	)
	return err
}

// VideoBookmarkDelete removes a user's bookmark for a video item.
func VideoBookmarkDelete(ctx context.Context, db utils.DBTX, userID, itemID int64) error {
	_, err := db.Exec(ctx,
		`DELETE FROM video_bookmarks WHERE user_id = $1 AND media_item_id = $2`,
		userID, itemID,
	)
	return err
}
