package repository_test

import (
	"testing"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArtistUpsert(t *testing.T) {
	t.Run("inserts_artist", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id, err := repository.ArtistUpsert(bg(), db, "The Beatles")
		require.NoError(t, err)
		assert.Greater(t, id, int64(0))

		artist, err := repository.ArtistGet(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, "The Beatles", artist.Name)
		assert.NotEmpty(t, artist.SortName)
	})

	t.Run("returns_same_id_on_conflict", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id1, err := repository.ArtistUpsert(bg(), db, "Sabaton")
		require.NoError(t, err)
		id2, err := repository.ArtistUpsert(bg(), db, "Sabaton")
		require.NoError(t, err)
		assert.Equal(t, id1, id2)
	})
}

func TestArtistGet(t *testing.T) {
	t.Run("returns_artist", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createArtist(t, db)
		artist, err := repository.ArtistGet(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, id, artist.ID)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.ArtistGet(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestArtistsInCollection(t *testing.T) {
	t.Run("returns_artists_with_albums_in_collection", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID, artistID, _, _ := setupAudioData(t, db)

		artists, total, err := repository.ArtistsInCollection(bg(), db, collID, 10, 0, "")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, 1)
		found := false
		for _, a := range artists {
			if a.ID == artistID {
				found = true
				break
			}
		}
		assert.True(t, found)
	})

	t.Run("returns_artists_with_albums_in_collection_matching_search", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID, artistID, _, _ := setupAudioData(t, db)

		artists, total, err := repository.ArtistsInCollection(bg(), db, collID, 10, 0, "tESt") // case-insensitive
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, 1)
		found := false
		for _, a := range artists {
			if a.ID == artistID {
				found = true
				break
			}
		}
		assert.True(t, found)
	})

	t.Run("empty_for_collection_with_no_albums", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)

		artists, total, err := repository.ArtistsInCollection(bg(), db, collID, 10, 0, "")
		require.NoError(t, err)
		assert.Equal(t, 0, total)
		assert.Empty(t, artists)
	})

	t.Run("empty_when_no_artists_match_search", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID, _, _, _ := setupAudioData(t, db)

		artists, total, err := repository.ArtistsInCollection(bg(), db, collID, 10, 0, "asdf")
		require.NoError(t, err)
		assert.Equal(t, 0, total)
		assert.Empty(t, artists)
	})
}

func TestArtistGetAlbums(t *testing.T) {
	t.Run("returns_albums_for_artist", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID, artistID, albumID, _ := setupAudioData(t, db)

		albums, err := repository.ArtistGetAlbums(bg(), db, artistID, collID)
		require.NoError(t, err)
		require.Len(t, albums, 1)
		assert.Equal(t, albumID, albums[0].AlbumID)
	})

	t.Run("empty_for_artist_with_no_albums", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		artistID := createArtist(t, db)

		albums, err := repository.ArtistGetAlbums(bg(), db, artistID, collID)
		require.NoError(t, err)
		assert.Empty(t, albums)
	})
}

func TestArtistSetManualThumbnail(t *testing.T) {
	t.Run("sets_manual_image", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createArtist(t, db)
		require.NoError(t, repository.ArtistSetManualThumbnail(bg(), db, id, false))
		require.NoError(t, repository.ArtistSetManualThumbnail(bg(), db, id, true))

		artist, err := repository.ArtistGet(bg(), db, id)
		require.NoError(t, err)
		assert.True(t, artist.ManualThumbnail)
	})

	t.Run("clears_manual_image", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createArtist(t, db)
		require.NoError(t, repository.ArtistSetManualThumbnail(bg(), db, id, true))
		require.NoError(t, repository.ArtistSetManualThumbnail(bg(), db, id, false))

		artist, err := repository.ArtistGet(bg(), db, id)
		require.NoError(t, err)
		assert.False(t, artist.ManualThumbnail)
	})
}

func TestArtistsWithoutManualThumbnail(t *testing.T) {
	t.Run("includes_artist_with_non_compilation_album", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		artistID := createArtist(t, db)
		createAlbum(t, db, &artistID, collID) // non-compilation by default

		ids, err := repository.ArtistsWithoutManualThumbnail(bg(), db)
		require.NoError(t, err)
		assert.Contains(t, ids, artistID)
	})

	t.Run("excludes_artist_with_manual_image_set", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		artistID := createArtist(t, db)
		createAlbum(t, db, &artistID, collID)
		require.NoError(t, repository.ArtistSetManualThumbnail(bg(), db, artistID, true))

		ids, err := repository.ArtistsWithoutManualThumbnail(bg(), db)
		require.NoError(t, err)
		assert.NotContains(t, ids, artistID)
	})
}
