package repository_test

import (
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlbumUpsert(t *testing.T) {
	t.Run("inserts_album", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		artistID := createArtist(t, db)

		id, err := repository.AlbumUpsert(bg(), db, "Drone EP", &artistID, int16Ptr(2004), nil, collID, false)
		require.NoError(t, err)
		assert.Greater(t, id, int64(0))

		album, err := repository.AlbumGet(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, "Drone EP", album.Name)
		require.NotNil(t, album.Year)
		assert.Equal(t, int16(2004), *album.Year)
	})

	t.Run("returns_existing_id_on_conflict", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		artistID := createArtist(t, db)

		id1, err := repository.AlbumUpsert(bg(), db, "Century Child", &artistID, nil, nil, collID, false)
		require.NoError(t, err)
		id2, err := repository.AlbumUpsert(bg(), db, "Century Child", &artistID, nil, nil, collID, false)
		require.NoError(t, err)
		assert.Equal(t, id1, id2)
	})
}

func TestAlbumGet(t *testing.T) {
	t.Run("returns_album", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		albumID := createAlbum(t, db, nil, collID)

		album, err := repository.AlbumGet(bg(), db, albumID)
		require.NoError(t, err)
		assert.Equal(t, albumID, album.ID)
		assert.Equal(t, collID, album.RootCollectionID)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.AlbumGet(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestAlbumSetManualCover(t *testing.T) {
	t.Run("sets_manual_cover", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		albumID := createAlbum(t, db, nil, collID)

		require.NoError(t, repository.AlbumSetManualCover(bg(), db, albumID, false))
		require.NoError(t, repository.AlbumSetManualCover(bg(), db, albumID, true))

		album, err := repository.AlbumGet(bg(), db, albumID)
		require.NoError(t, err)
		assert.True(t, album.ManualCover)
	})

	t.Run("clears_manual_cover", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		albumID := createAlbum(t, db, nil, collID)

		require.NoError(t, repository.AlbumSetManualCover(bg(), db, albumID, true))
		require.NoError(t, repository.AlbumSetManualCover(bg(), db, albumID, false))

		album, err := repository.AlbumGet(bg(), db, albumID)
		require.NoError(t, err)
		assert.False(t, album.ManualCover)
	})
}

func TestAlbumGetRandomNonCompilationIDByArtist(t *testing.T) {
	t.Run("returns_an_album_id", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		artistID := createArtist(t, db)
		albumID := createAlbum(t, db, &artistID, collID)

		got, err := repository.AlbumGetRandomNonCompilationIDByArtist(bg(), db, artistID)
		require.NoError(t, err)
		assert.Equal(t, albumID, got)
	})

	t.Run("returns_an_album_id_multiple_albums", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		artistID := createArtist(t, db)
		albumID1 := createAlbum(t, db, &artistID, collID)
		albumID2 := createAlbum(t, db, &artistID, collID)

		got, err := repository.AlbumGetRandomNonCompilationIDByArtist(bg(), db, artistID)
		require.NoError(t, err)
		assert.True(t, slices.Contains([]int64{albumID1, albumID2}, got))
	})

	t.Run("no_rows_when_only_compilations_exist", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		artistID := createArtist(t, db)

		// Insert a compilation album for this artist.
		_, err := repository.AlbumUpsert(bg(), db, "Hits Vol. 1", &artistID, nil, nil, collID, true)
		require.NoError(t, err)

		_, err = repository.AlbumGetRandomNonCompilationIDByArtist(bg(), db, artistID)
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	})

	t.Run("no_rows_when_no_albums_exist", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_ = createCollection(t, db, constants.CollectionTypeMusic)
		artistID := createArtist(t, db)

		_, err := repository.AlbumGetRandomNonCompilationIDByArtist(bg(), db, artistID)
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	})
}
