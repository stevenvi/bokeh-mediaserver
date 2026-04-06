package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// ArtistUpsert inserts a new artist or returns the existing ID if the name already exists.
func ArtistUpsert(ctx context.Context, db utils.DBTX, name string) (int64, error) {
	sortName := utils.GenerateSortName(name)
	var id int64
	err := db.QueryRow(ctx,
		`INSERT INTO artists (name, sort_name)
		 VALUES ($1, $2)
		 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name  -- No-op update to make RETURNING work on conflict
		 RETURNING id`,
		name, sortName,
	).Scan(&id)
	return id, err
}

// ArtistGet returns an artist by ID.
func ArtistGet(ctx context.Context, db utils.DBTX, id int64) (*models.Artist, error) {
	var artist models.Artist
	err := db.QueryRow(ctx,
		`SELECT id, name, sort_name, manual_image, created_at
		 FROM artists WHERE id = $1`,
		id,
	).Scan(&artist.ID, &artist.Name, &artist.SortName, &artist.ManualImage, &artist.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &artist, nil
}

// ArtistsInCollection returns artists who own at least one album in the given root
// collection. "Various Artists" appears here when compilation albums exist.
// Results are ordered by sort_name.
// TODO: Is this efficient? Investigate that.
func ArtistsInCollection(ctx context.Context, db utils.DBTX, collectionID int64, limit, offset int, search string) ([]models.ArtistSummary, int, error) {
	baseQuery := `
		WITH artist_ids AS (
			SELECT DISTINCT al.artist_id AS aid
			FROM audio_albums al
			WHERE al.root_collection_id = $1
			  AND al.artist_id IS NOT NULL
		)`

	// Count and list queries have different parameter positions for search ($2 vs $4),
	// so they require separate args slices.
	var countSearchClause, listSearchClause string
	countArgs := []any{collectionID}
	listArgs := []any{collectionID, limit, offset}

	if search != "" {
		countSearchClause = ` AND a.search_vector @@ plainto_tsquery('simple', $2)`
		countArgs = append(countArgs, search)
		listSearchClause = ` AND a.search_vector @@ plainto_tsquery('simple', $4)`
		listArgs = append(listArgs, search)
	}

	// Get total count
	countQuery := baseQuery + `
		SELECT COUNT(*) FROM artist_ids s
		JOIN artists a ON a.id = s.aid
		WHERE s.aid IS NOT NULL` + countSearchClause

	var total int
	err := db.QueryRow(ctx, countQuery, countArgs...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Get page
	listQuery := baseQuery + `
		SELECT a.name, a.sort_name, a.id
		FROM artist_ids s
		JOIN artists a ON a.id = s.aid
		WHERE s.aid IS NOT NULL` + listSearchClause + `
		ORDER BY a.sort_name ASC
		LIMIT $2 OFFSET $3`

	rows, err := db.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, 0, err
	}
	artists, err := pgx.CollectRows(rows, pgx.RowToStructByPos[models.ArtistSummary])
	if err != nil {
		return nil, 0, err
	}
	return artists, total, nil
}

// ArtistGetAlbums returns album summaries for a given artist within a collection, ordered by year.
// Includes albums the artist owns (al.artist_id = artistID) and any album they appear on as a
// track artist (e.g. compilation albums), so a band's page shows compilations they contributed to.
func ArtistGetAlbums(ctx context.Context, db utils.DBTX, artistID, collectionID int64) ([]models.AlbumSummary, error) {
	rows, err := db.Query(ctx,
		`SELECT
			al.year,
			al.name,
			al.id,
			COUNT(am.media_item_id),
			COALESCE(SUM(am.duration_seconds), 0)
		FROM audio_albums al
		JOIN audio_metadata am ON am.album_id = al.id
		JOIN media_items mi ON mi.id = am.media_item_id
		WHERE al.root_collection_id = $2
		  AND mi.missing_since IS NULL AND mi.hidden_at IS NULL
		  AND (
		      al.artist_id = $1
		      OR EXISTS (
		          SELECT 1 FROM audio_metadata am2
		          WHERE am2.album_id = al.id AND am2.artist_id = $1
		      )
		  )
		GROUP BY al.id, al.name, al.year
		ORDER BY al.year ASC NULLS LAST, al.name ASC`,
		artistID, collectionID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.AlbumSummary])
}

// ArtistSetManualImage marks an artist as having a manually uploaded image.
func ArtistSetManualImage(ctx context.Context, db utils.DBTX, id int64, manual bool) error {
	_, err := db.Exec(ctx,
		`UPDATE artists SET manual_image = $2 WHERE id = $1`,
		id, manual,
	)
	return err
}

// ArtistsWithNoTracks returns IDs of artists that have no remaining albums
// and no track references (as either track artist or album artist). 
// Safe to call after empty albums have been removed.
func ArtistsWithNoTracks(ctx context.Context, db utils.DBTX) ([]int64, error) {
	rows, err := db.Query(ctx,
		`SELECT a.id FROM artists a
		 WHERE NOT EXISTS (
		     SELECT 1 FROM audio_albums al WHERE al.artist_id = a.id
		 )
		 AND NOT EXISTS (
		     SELECT 1 FROM audio_metadata am
		     JOIN media_items mi ON mi.id = am.media_item_id
		     WHERE (am.artist_id = a.id OR am.album_artist_id = a.id)
		 )`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// ArtistDelete removes an artist record by ID.
func ArtistDelete(ctx context.Context, db utils.DBTX, artistID int64) error {
	_, err := db.Exec(ctx, `DELETE FROM artists WHERE id = $1`, artistID)
	return err
}

// ArtistsWithoutManualImage returns IDs of artists that have manual_image = false
// and have at least one non-compilation album where they are the album artist.
// Artists who only appear on compilation albums are excluded.
func ArtistsWithoutManualImage(ctx context.Context, db utils.DBTX) ([]int64, error) {
	rows, err := db.Query(ctx,
		`SELECT DISTINCT a.id
		 FROM artists a
		 JOIN audio_albums al ON al.artist_id = a.id
		 WHERE a.manual_image = false
		   AND al.is_compilation = false`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}
