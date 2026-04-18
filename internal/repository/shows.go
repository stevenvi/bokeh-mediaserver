package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// ShowsInCollection returns all shows in an audio:show collection.
// Each show maps to an artist; albums within that artist are groupings (seasons/volumes/etc.).
// Access is verified against collection_access for the requesting user.
func ShowsInCollection(ctx context.Context, db utils.DBTX, collectionID, userID int64) ([]models.ShowSummary, error) {
	rows, err := db.Query(ctx,
		`SELECT DISTINCT a.id, a.name, a.manual_thumbnail
		 FROM artists a
		 JOIN audio_albums aa ON aa.artist_id = a.id
		 JOIN audio_metadata am ON am.album_id = aa.id
		 JOIN media_items mi ON mi.id = am.media_item_id
		 WHERE aa.root_collection_id = $1
		   AND mi.missing_since IS NULL AND mi.hidden_at IS NULL
		   AND EXISTS (
		       SELECT 1 FROM collection_access ca
		       WHERE ca.collection_id = $1 AND ca.user_id = $2
		   )
		 ORDER BY a.name ASC`,
		collectionID, userID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.ShowSummary])
}

// ShowEpisodesByArtist returns all episodes for a show (identified by artist) across all
// grouping albums, ordered with natural (numeric-aware) album sorting, then disc, then track.
// The album name is included in each row so the client can render multi-level groupings.
func ShowEpisodesByArtist(ctx context.Context, db utils.DBTX, artistID, collectionID, userID int64) ([]models.EpisodeView, error) {
	rows, err := db.Query(ctx,
		`SELECT
		     am.track_number,
		     am.disc_number,
		     am.duration_seconds,
		     ep_artist.name   AS artist_name,
		     m.title,
		     m.mime_type,
		     m.id,
		     aa.name          AS album_name
		 FROM audio_metadata am
		 JOIN media_items m ON m.id = am.media_item_id
		 LEFT JOIN artists ep_artist ON ep_artist.id = am.artist_id
		 JOIN audio_albums aa ON aa.id = am.album_id
		 WHERE aa.artist_id = $1
		   AND aa.root_collection_id = $2
		   AND m.missing_since IS NULL AND m.hidden_at IS NULL
		   AND EXISTS (
		       SELECT 1 FROM collection_access ca
		       WHERE ca.collection_id = $2 AND ca.user_id = $3
		   )
		 ORDER BY
		     -- Natural sort: text before the first digit, then the first numeric run as int
		     regexp_replace(aa.name, '\d+.*', '') ASC,
		     COALESCE((regexp_match(aa.name, '\d+'))[1]::int, 0) ASC,
		     am.disc_number ASC NULLS LAST,
		     am.track_number ASC NULLS LAST,
		     m.title ASC`,
		artistID, collectionID, userID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.EpisodeView])
}
