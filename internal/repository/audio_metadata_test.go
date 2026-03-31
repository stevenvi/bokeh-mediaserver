package repository_test

import (
	"testing"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAudioTrackUpsert(t *testing.T) {
	t.Run("minimal_fields_all_null", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		itemID := createAudioMediaItem(t, db, collID)

		err := repository.AudioTrackUpsert(bg(), db, itemID,
			nil, nil, nil, nil,
			nil, nil, nil, nil, nil, nil, false,
		)
		require.NoError(t, err)

		var artistID, albumArtistID, albumID *int64
		var title *string
		var trackNumber, discNumber, year *int16
		var durationSeconds, replayGainDB *float64
		var genre *string
		var hasEmbeddedArt bool
		err = db.QueryRow(bg(),
			`SELECT artist_id, album_artist_id, album_id, title,
			        track_number, disc_number, duration_seconds,
			        genre, year, replay_gain_db, has_embedded_art
			 FROM audio_metadata WHERE media_item_id = $1`, itemID,
		).Scan(&artistID, &albumArtistID, &albumID, &title,
			&trackNumber, &discNumber, &durationSeconds,
			&genre, &year, &replayGainDB, &hasEmbeddedArt)
		require.NoError(t, err)
		assert.Nil(t, artistID)
		assert.Nil(t, albumArtistID)
		assert.Nil(t, albumID)
		assert.Nil(t, title)
		assert.Nil(t, trackNumber)
		assert.Nil(t, discNumber)
		assert.Nil(t, durationSeconds)
		assert.Nil(t, genre)
		assert.Nil(t, year)
		assert.Nil(t, replayGainDB)
		assert.False(t, hasEmbeddedArt)
	})

	t.Run("all_fields_stored_correctly", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		artistID := createArtist(t, db)
		albumArtistID := createArtist(t, db)
		albumID := createAlbum(t, db, &artistID, collID)
		itemID := createAudioMediaItem(t, db, collID)

		title := "Ghost Division"
		trackNum := int16(3)
		discNum := int16(1)
		duration := float64(213.5)
		genre := "Power Metal"
		year := int16(2008)
		replayGain := float64(-8.3)
		err := repository.AudioTrackUpsert(bg(), db, itemID,
			&artistID, &albumArtistID, &albumID, &title,
			&trackNum, &discNum, &duration, &genre, &year, &replayGain, true,
		)
		require.NoError(t, err)

		var gotArtistID, gotAlbumArtistID, gotAlbumID *int64
		var gotTitle *string
		var gotTrackNum, gotDiscNum, gotYear *int16
		var gotDuration, gotReplayGain *float64
		var gotGenre *string
		var gotHasArt bool
		err = db.QueryRow(bg(),
			`SELECT artist_id, album_artist_id, album_id, title,
			        track_number, disc_number, duration_seconds,
			        genre, year, replay_gain_db, has_embedded_art
			 FROM audio_metadata WHERE media_item_id = $1`, itemID,
		).Scan(&gotArtistID, &gotAlbumArtistID, &gotAlbumID, &gotTitle,
			&gotTrackNum, &gotDiscNum, &gotDuration,
			&gotGenre, &gotYear, &gotReplayGain, &gotHasArt)
		require.NoError(t, err)

		require.NotNil(t, gotArtistID)
		assert.Equal(t, artistID, *gotArtistID)
		require.NotNil(t, gotAlbumArtistID)
		assert.Equal(t, albumArtistID, *gotAlbumArtistID)
		require.NotNil(t, gotAlbumID)
		assert.Equal(t, albumID, *gotAlbumID)
		require.NotNil(t, gotTitle)
		assert.Equal(t, "Ghost Division", *gotTitle)
		require.NotNil(t, gotTrackNum)
		assert.Equal(t, int16(3), *gotTrackNum)
		require.NotNil(t, gotDiscNum)
		assert.Equal(t, int16(1), *gotDiscNum)
		require.NotNil(t, gotDuration)
		assert.InDelta(t, 213.5, *gotDuration, 0.001)
		require.NotNil(t, gotGenre)
		assert.Equal(t, "Power Metal", *gotGenre)
		require.NotNil(t, gotYear)
		assert.Equal(t, int16(2008), *gotYear)
		require.NotNil(t, gotReplayGain)
		assert.InDelta(t, -8.3, *gotReplayGain, 0.001)
		assert.True(t, gotHasArt)
	})

	t.Run("upsert_updates_existing_row", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, artistID, albumID, itemID := setupAudioData(t, db)

		newTitle := "Updated Title"
		trackNum := int16(7)
		discNum := int16(2)
		duration := float64(304.8)
		genre := "Folk"
		year := int16(2019)
		replayGain := float64(-6.1)
		err := repository.AudioTrackUpsert(bg(), db, itemID,
			&artistID, nil, &albumID, &newTitle,
			&trackNum, &discNum, &duration, &genre, &year, &replayGain, false,
		)
		require.NoError(t, err)

		var gotTitle *string
		var gotTrackNum, gotDiscNum *int16
		var gotDuration *float64
		var gotGenre *string
		var gotYear *int16
		var gotReplayGain *float64
		var gotHasArt bool
		err = db.QueryRow(bg(),
			`SELECT title, track_number, disc_number, duration_seconds,
			        genre, year, replay_gain_db, has_embedded_art
			 FROM audio_metadata WHERE media_item_id = $1`, itemID,
		).Scan(&gotTitle, &gotTrackNum, &gotDiscNum, &gotDuration,
			&gotGenre, &gotYear, &gotReplayGain, &gotHasArt)
		require.NoError(t, err)

		require.NotNil(t, gotTitle)
		assert.Equal(t, "Updated Title", *gotTitle)
		require.NotNil(t, gotTrackNum)
		assert.Equal(t, int16(7), *gotTrackNum)
		require.NotNil(t, gotDiscNum)
		assert.Equal(t, int16(2), *gotDiscNum)
		require.NotNil(t, gotDuration)
		assert.InDelta(t, 304.8, *gotDuration, 0.001)
		require.NotNil(t, gotGenre)
		assert.Equal(t, "Folk", *gotGenre)
		require.NotNil(t, gotYear)
		assert.Equal(t, int16(2019), *gotYear)
		require.NotNil(t, gotReplayGain)
		assert.InDelta(t, -6.1, *gotReplayGain, 0.001)
		assert.False(t, gotHasArt)
	})
}

func TestAudioTracksByAlbum(t *testing.T) {
	t.Run("returns_tracks_for_album", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID, _, albumID, itemID := setupAudioData(t, db)
		userID := createUser(t, db)
		grantAccess(t, db, userID, collID)

		tracks, err := repository.AudioTracksByAlbum(bg(), db, albumID, userID)
		require.NoError(t, err)
		require.Len(t, tracks, 1)
		assert.Equal(t, itemID, tracks[0].ID)
	})

	t.Run("empty_when_no_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, _, albumID, _ := setupAudioData(t, db)
		userID := createUser(t, db)
		// No access granted — access check in query should exclude all rows.

		tracks, err := repository.AudioTracksByAlbum(bg(), db, albumID, userID)
		require.NoError(t, err)
		assert.Empty(t, tracks)
	})

	t.Run("empty_for_album_with_no_tracks", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID, artistID, _, _ := setupAudioData(t, db)
		userID := createUser(t, db)
		grantAccess(t, db, userID, collID)

		// Create a separate album with no tracks.
		emptyAlbumID := createAlbum(t, db, &artistID, collID)

		tracks, err := repository.AudioTracksByAlbum(bg(), db, emptyAlbumID, userID)
		require.NoError(t, err)
		assert.Empty(t, tracks)
	})
}
