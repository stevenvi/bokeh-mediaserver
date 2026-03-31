package repository

import (
	"context"

	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// AlbumUpsert inserts a new album or returns the existing ID if one with the
// same (name, artist_id, root_collection_id) already exists.
// year and genre are only written on insert; they are not updated on conflict to
// avoid flapping when individual tracks have inconsistent tag data.
func AlbumUpsert(
	ctx context.Context,
	db utils.DBTX,
	name string,
	artistID *int64,
	year *int16,
	genre *string,
	rootCollectionID int64,
	isCompilation bool,
) (int64, error) {
	var id int64
	err := db.QueryRow(ctx,
		`INSERT INTO audio_albums (name, artist_id, year, genre, root_collection_id, is_compilation)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (name, COALESCE(artist_id, 0), root_collection_id)
		 DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
		name, artistID, year, genre, rootCollectionID, isCompilation,
	).Scan(&id)
	return id, err
}

// AlbumGet returns an audio album by ID.
func AlbumGet(ctx context.Context, db utils.DBTX, id int64) (*models.AudioAlbum, error) {
	var a models.AudioAlbum
	err := db.QueryRow(ctx,
		`SELECT id, name, artist_id, year, genre, root_collection_id, is_compilation, manual_cover, created_at
		 FROM audio_albums WHERE id = $1`,
		id,
	).Scan(&a.ID, &a.Name, &a.ArtistID, &a.Year, &a.Genre,
		&a.RootCollectionID, &a.IsCompilation, &a.ManualCover, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// AlbumSetManualCover marks an album as having a manually uploaded cover.
func AlbumSetManualCover(ctx context.Context, db utils.DBTX, id int64, manual bool) error {
	_, err := db.Exec(ctx,
		`UPDATE audio_albums SET manual_cover = $2 WHERE id = $1`, id, manual)
	return err
}

// AlbumGetRandomNonCompilationIDByArtist returns a random non-compilation album ID
// for the given artist. Returns pgx.ErrNoRows if none exist.
// TODO: This should be limited to albums in the same collection, but it looks like we don't have that data at the moment...
func AlbumGetRandomNonCompilationIDByArtist(ctx context.Context, db utils.DBTX, artistID int64) (int64, error) {
	var id int64
	err := db.QueryRow(ctx,
		`SELECT id FROM audio_albums
		 WHERE artist_id = $1
		   AND is_compilation = false
		 ORDER BY RANDOM()
		 LIMIT 1`,
		artistID,
	).Scan(&id)
	return id, err
}
