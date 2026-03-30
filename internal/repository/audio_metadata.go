package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type AudioMetadataRepository struct {
	db utils.DBTX
}

func NewAudioMetadataRepository(db utils.DBTX) *AudioMetadataRepository {
	return &AudioMetadataRepository{db: db}
}

// UpsertAudioMetadata inserts or updates audio metadata for a media item.
func (r *AudioMetadataRepository) UpsertAudioMetadata(ctx context.Context, itemID int64,
	artistID, albumArtistID, albumID *int64,
	title *string,
	trackNumber, discNumber *int16,
	durationSeconds *float64,
	genre *string,
	year *int16,
	replayGainDB *float64,
	hasEmbeddedArt bool,
) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO audio_metadata
		     (media_item_id, artist_id, album_artist_id, album_id, title,
		      track_number, disc_number, duration_seconds,
		      genre, year, replay_gain_db, has_embedded_art, processed_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now())
		 ON CONFLICT (media_item_id) DO UPDATE SET
		     artist_id        = EXCLUDED.artist_id,
		     album_artist_id  = EXCLUDED.album_artist_id,
		     album_id         = EXCLUDED.album_id,
		     title            = EXCLUDED.title,
		     track_number     = EXCLUDED.track_number,
		     disc_number      = EXCLUDED.disc_number,
		     duration_seconds = EXCLUDED.duration_seconds,
		     genre            = EXCLUDED.genre,
		     year             = EXCLUDED.year,
		     replay_gain_db   = EXCLUDED.replay_gain_db,
		     has_embedded_art = EXCLUDED.has_embedded_art,
		     processed_at     = now()`,
		itemID, artistID, albumArtistID, albumID, title,
		trackNumber, discNumber, durationSeconds,
		genre, year, replayGainDB, hasEmbeddedArt,
	)
	return err
}

// ListTracksByAlbum returns all tracks for an album ordered by disc and track number.
// Access is verified against the album's root_collection_id.
func (r *AudioMetadataRepository) ListTracksByAlbum(ctx context.Context, albumID, userID int64) ([]models.TrackView, error) {
	rows, err := r.db.Query(ctx,
		`SELECT m.id, m.title, am.track_number, am.disc_number,
		        am.duration_seconds, a.name, m.mime_type
		 FROM audio_metadata am
		 JOIN media_items m ON m.id = am.media_item_id
		 LEFT JOIN artists a ON a.id = am.artist_id
		 JOIN audio_albums al ON al.id = am.album_id
		 WHERE am.album_id = $1
		   AND m.missing_since IS NULL AND m.hidden_at IS NULL
		   AND EXISTS (
		       SELECT 1 FROM collection_access ca
		       WHERE ca.collection_id = al.root_collection_id
		         AND ca.user_id = $2
		   )
		 ORDER BY am.disc_number ASC NULLS LAST, am.track_number ASC NULLS LAST, m.title ASC`,
		albumID, userID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.TrackView])
}
