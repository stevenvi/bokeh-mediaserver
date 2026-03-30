package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type VideoMetadataRepository struct {
	db utils.DBTX
}

func NewVideoMetadataRepository(db utils.DBTX) *VideoMetadataRepository {
	return &VideoMetadataRepository{db: db}
}

// UpsertVideoMetadata inserts or updates video metadata for a media item.
// Does NOT reset transcoded_at on re-process — transcoding is expensive and
// must be explicitly re-triggered by clearing the field.
func (r *VideoMetadataRepository) UpsertVideoMetadata(ctx context.Context, itemID int64,
	durationSeconds *int,
	width, height *int,
	bitrateKbps *int,
	videoCodec, audioCodec *string,
	date, endDate *time.Time,
	author *string,
) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO video_metadata
		    (media_item_id, duration_seconds, width, height, bitrate_kbps,
		     video_codec, audio_codec, date, end_date, author)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (media_item_id) DO UPDATE SET
		    duration_seconds = EXCLUDED.duration_seconds,
		    width             = EXCLUDED.width,
		    height            = EXCLUDED.height,
		    bitrate_kbps      = EXCLUDED.bitrate_kbps,
		    video_codec       = EXCLUDED.video_codec,
		    audio_codec       = EXCLUDED.audio_codec,
		    date              = EXCLUDED.date,
		    end_date          = EXCLUDED.end_date,
		    author            = EXCLUDED.author`,
		itemID, durationSeconds, width, height, bitrateKbps,
		videoCodec, audioCodec, date, endDate, author,
	)
	return err
}

// GetVideoMetadataWithBookmark fetches video_metadata for an item, with the
// requesting user's bookmark position populated if one exists.
func (r *VideoMetadataRepository) GetVideoMetadataWithBookmark(ctx context.Context, itemID int64, userID int64) (*models.VideoMetadata, error) {
	var m models.VideoMetadata
	err := r.db.QueryRow(ctx,
		`SELECT vm.duration_seconds, vm.width, vm.height, vm.bitrate_kbps,
		        vm.video_codec, vm.audio_codec, vm.transcoded_at,
		        vm.date, vm.end_date, vm.author, vm.manual_cover,
		        vb.position_seconds
		 FROM video_metadata vm
		 LEFT JOIN video_bookmarks vb
		     ON vb.media_item_id = vm.media_item_id AND vb.user_id = $2
		 WHERE vm.media_item_id = $1`,
		itemID, userID,
	).Scan(
		&m.DurationSeconds, &m.Width, &m.Height, &m.BitrateKbps,
		&m.VideoCodec, &m.AudioCodec, &m.TranscodedAt,
		&m.Date, &m.EndDate, &m.Author, &m.ManualCover,
		&m.BookmarkSeconds,
	)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// ListVideoItemsForIntegrity returns all non-missing video items with their
// file hash, transcoded_at timestamp, and root collection type.
func (r *VideoMetadataRepository) ListVideoItemsForIntegrity(ctx context.Context) ([]VideoIntegrityItem, error) {
	rows, err := r.db.Query(ctx,
		`SELECT vm.media_item_id, mi.file_hash, vm.transcoded_at, c.type AS collection_type
		 FROM video_metadata vm
		 JOIN media_items mi ON mi.id = vm.media_item_id
		 JOIN collections c ON c.id = mi.collection_id
		 WHERE mi.missing_since IS NULL`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[VideoIntegrityItem])
}

// ClearTranscodedAt sets transcoded_at to NULL for a video item, allowing it
// to be re-transcoded.
func (r *VideoMetadataRepository) ClearTranscodedAt(ctx context.Context, itemID int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE video_metadata SET transcoded_at = NULL WHERE media_item_id = $1`,
		itemID,
	)
	return err
}

// IsVideoManualCover returns true if manual_cover is false for the item,
// meaning auto-generated cover art is appropriate. Returns false (not manual)
// if no row exists yet (first processing run).
func (r *VideoMetadataRepository) IsVideoManualCover(ctx context.Context, itemID int64) (bool, error) {
	var manualCover bool
	err := r.db.QueryRow(ctx,
		`SELECT manual_cover FROM video_metadata WHERE media_item_id = $1`,
		itemID,
	).Scan(&manualCover)
	if err != nil {
		// Row may not exist yet (first run); treat as not manual
		return false, nil
	}
	return !manualCover, nil
}

// VideoNeedsTranscode returns true if the item has not yet been transcoded
// (transcoded_at IS NULL in video_metadata).
func (r *VideoMetadataRepository) VideoNeedsTranscode(ctx context.Context, itemID int64) (bool, error) {
	var count int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM video_metadata
		 WHERE media_item_id = $1 AND transcoded_at IS NULL`,
		itemID,
	).Scan(&count)
	return count > 0, err
}

// GetVideoMetaForTranscode returns the bitrate and transcoded_at fields for a
// media item's video_metadata. Used by the transcoder to decide whether to proceed.
func (r *VideoMetadataRepository) GetVideoMetaForTranscode(ctx context.Context, itemID int64) (*models.VideoMetadata, error) {
	var m models.VideoMetadata
	err := r.db.QueryRow(ctx,
		`SELECT bitrate_kbps, transcoded_at FROM video_metadata WHERE media_item_id = $1`,
		itemID,
	).Scan(&m.BitrateKbps, &m.TranscodedAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// SetTranscodedAt records the time a transcode completed for a media item.
func (r *VideoMetadataRepository) SetTranscodedAt(ctx context.Context, itemID int64, t time.Time) error {
	_, err := r.db.Exec(ctx,
		`UPDATE video_metadata SET transcoded_at = $2 WHERE media_item_id = $1`,
		itemID, t,
	)
	return err
}

// SetVideoManualCover sets the manual_cover flag on a video_metadata row.
func (r *VideoMetadataRepository) SetVideoManualCover(ctx context.Context, itemID int64, manual bool) error {
	_, err := r.db.Exec(ctx,
		`UPDATE video_metadata SET manual_cover = $2 WHERE media_item_id = $1`,
		itemID, manual,
	)
	return err
}
