package repository_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Search-specific helpers ────────────────────────────────────────────────
// These allow controlled titles/metadata, unlike the generic setup_test.go
// helpers that auto-generate unique names.

var searchItemCounter int64

// addPhoto inserts a media item with the given title plus a photo_metadata row
// carrying the given description and keywords. Used to drive the FTS indexes.
func addPhoto(t *testing.T, db utils.DBTX, collID int64, title, description string, keywords []string) int64 {
	t.Helper()
	n := atomic.AddInt64(&searchItemCounter, 1)
	itemID, _, err := repository.MediaItemUpsert(bg(), db, collID, title,
		fmt.Sprintf("search/photo-%d.jpg", n), 1024,
		fmt.Sprintf("search-phash-%d", n), "image/jpeg", time.Time{})
	require.NoError(t, err)

	w, h := 1920, 1080
	var descPtr *string
	if description != "" {
		descPtr = &description
	}
	err = repository.PhotoUpsert(bg(), db, itemID,
		&w, &h, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, descPtr, keywords, nil,
	)
	require.NoError(t, err)
	return itemID
}

// addVideo inserts a video media item with the given title in the given
// collection.
func addVideo(t *testing.T, db utils.DBTX, collID int64, title string) int64 {
	t.Helper()
	n := atomic.AddInt64(&searchItemCounter, 1)
	itemID, _, err := repository.MediaItemUpsert(bg(), db, collID, title,
		fmt.Sprintf("search/video-%d.mp4", n), 1024,
		fmt.Sprintf("search-vhash-%d", n), "video/mp4", time.Time{})
	require.NoError(t, err)
	return itemID
}

// addAudioTrack inserts an audio media item + audio_metadata row.
func addAudioTrack(t *testing.T, db utils.DBTX, collID int64, title string, artistID, albumID *int64) int64 {
	t.Helper()
	n := atomic.AddInt64(&searchItemCounter, 1)
	itemID, _, err := repository.MediaItemUpsert(bg(), db, collID, title,
		fmt.Sprintf("search/track-%d.mp3", n), 1024,
		fmt.Sprintf("search-ahash-%d", n), "audio/mpeg", time.Time{})
	require.NoError(t, err)

	dur := float64(180)
	err = repository.AudioTrackUpsert(bg(), db, itemID,
		artistID, nil, albumID,
		nil, nil, &dur, nil, nil, nil, false,
	)
	require.NoError(t, err)
	return itemID
}

// hidePhoto sets hidden_at on a media item to exclude it from search.
func hideItem(t *testing.T, db utils.DBTX, itemID int64) {
	t.Helper()
	_, err := db.Exec(bg(), `UPDATE media_items SET hidden_at = now() WHERE id = $1`, itemID)
	require.NoError(t, err)
}

func markItemMissing(t *testing.T, db utils.DBTX, itemID int64) {
	t.Helper()
	_, err := db.Exec(bg(), `UPDATE media_items SET missing_since = now() WHERE id = $1`, itemID)
	require.NoError(t, err)
}

// ─── Photo search ───────────────────────────────────────────────────────────

func TestSearchPhotos(t *testing.T) {
	t.Run("matches_title_description_keywords_and_excludes_others", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)

		// A: title-only match for "sunset"
		idA := addPhoto(t, db, collID, "Sunset Beach", "", nil)
		// B: description match for "sunset"
		idB := addPhoto(t, db, collID, "IMG_0001", "sunset over mountains", nil)
		// C: keyword match for "hiking"
		idC := addPhoto(t, db, collID, "IMG_0002", "", []string{"travel", "hiking"})
		// D: title+keywords match for "mountain" (via description "mountains" stem)
		idD := addPhoto(t, db, collID, "Mountain View", "", []string{"indoor"})

		// "sunset" → A + B; not C, D
		hits, err := repository.SearchPhotos(bg(), db, userID, repository.SearchParams{Query: "sunset", Limit: 50})
		require.NoError(t, err)
		got := photoIDs(hits)
		assert.Contains(t, got, idA)
		assert.Contains(t, got, idB)
		assert.NotContains(t, got, idC)
		assert.NotContains(t, got, idD)

		// "hiking" → C only
		hits, err = repository.SearchPhotos(bg(), db, userID, repository.SearchParams{Query: "hiking", Limit: 50})
		require.NoError(t, err)
		got = photoIDs(hits)
		assert.Equal(t, []int64{idC}, got)

		// "mountain" → B (description "mountains") + D (title "Mountain"); not A, C
		hits, err = repository.SearchPhotos(bg(), db, userID, repository.SearchParams{Query: "mountain", Limit: 50})
		require.NoError(t, err)
		got = photoIDs(hits)
		assert.Contains(t, got, idB)
		assert.Contains(t, got, idD)
		assert.NotContains(t, got, idA)
		assert.NotContains(t, got, idC)
	})

	t.Run("scopes_to_user_collection_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		alice := createUser(t, db)
		bob := createUser(t, db)
		aliceColl := createCollection(t, db, constants.CollectionTypePhoto)
		bobColl := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, alice, aliceColl)
		grantAccess(t, db, bob, bobColl)

		aliceItem := addPhoto(t, db, aliceColl, "Sunset", "", nil)
		bobItem := addPhoto(t, db, bobColl, "Sunset", "", nil)

		// Alice sees her item, not Bob's
		aliceHits, err := repository.SearchPhotos(bg(), db, alice, repository.SearchParams{Query: "sunset", Limit: 50})
		require.NoError(t, err)
		got := photoIDs(aliceHits)
		assert.Equal(t, []int64{aliceItem}, got)
		assert.NotContains(t, got, bobItem)

		// Bob sees his, not Alice's
		bobHits, err := repository.SearchPhotos(bg(), db, bob, repository.SearchParams{Query: "sunset", Limit: 50})
		require.NoError(t, err)
		got = photoIDs(bobHits)
		assert.Equal(t, []int64{bobItem}, got)
		assert.NotContains(t, got, aliceItem)
	})

	t.Run("excludes_hidden_and_missing_items", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)

		visible := addPhoto(t, db, collID, "Sunset visible", "", nil)
		hidden := addPhoto(t, db, collID, "Sunset hidden", "", nil)
		missing := addPhoto(t, db, collID, "Sunset missing", "", nil)
		hideItem(t, db, hidden)
		markItemMissing(t, db, missing)

		hits, err := repository.SearchPhotos(bg(), db, userID, repository.SearchParams{Query: "sunset", Limit: 50})
		require.NoError(t, err)
		got := photoIDs(hits)
		assert.Equal(t, []int64{visible}, got)
		assert.NotContains(t, got, hidden)
		assert.NotContains(t, got, missing)
	})

	t.Run("pagination_is_stable_under_tied_ranks", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)

		// 5 items with identical metadata produce identical ts_rank.
		var ids []int64
		for i := 0; i < 5; i++ {
			ids = append(ids, addPhoto(t, db, collID, fmt.Sprintf("sunset-%d", i), "", nil))
		}

		// Walk page-by-page (limit=1) and assert disjoint, no skips.
		seen := map[int64]bool{}
		for offset := 0; offset < 5; offset++ {
			hits, err := repository.SearchPhotos(bg(), db, userID,
				repository.SearchParams{Query: "sunset", Offset: offset, Limit: 1})
			require.NoError(t, err)
			require.Len(t, hits, 1, "expected exactly one row at offset %d", offset)
			require.False(t, seen[hits[0].ID], "duplicate at offset %d: id %d", offset, hits[0].ID)
			seen[hits[0].ID] = true
		}
		assert.Len(t, seen, 5)
		for _, id := range ids {
			assert.True(t, seen[id], "missing item id %d in paginated walk", id)
		}
	})

	t.Run("does_not_match_non_photo_collection_types", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		photoColl := createCollection(t, db, constants.CollectionTypePhoto)
		movieColl := createCollection(t, db, constants.CollectionTypeMovie)
		grantAccess(t, db, userID, photoColl)
		grantAccess(t, db, userID, movieColl)

		photoID := addPhoto(t, db, photoColl, "Sunset photo", "", nil)
		// A media item with the same title in a movie collection should not
		// surface in photo search.
		_ = addVideo(t, db, movieColl, "Sunset photo")

		hits, err := repository.SearchPhotos(bg(), db, userID, repository.SearchParams{Query: "sunset", Limit: 50})
		require.NoError(t, err)
		got := photoIDs(hits)
		assert.Equal(t, []int64{photoID}, got)
	})
}

func photoIDs(items []models.PhotoItem) []int64 {
	out := make([]int64, len(items))
	for i, it := range items {
		out[i] = it.ID
	}
	return out
}

// ─── Video search ──────────────────────────────────────────────────────────

func TestSearchVideos(t *testing.T) {
	t.Run("partitions_into_movies_and_home_movies", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		movieColl := createCollection(t, db, constants.CollectionTypeMovie)
		homeColl := createCollection(t, db, constants.CollectionTypeHomeMovie)
		grantAccess(t, db, userID, movieColl)
		grantAccess(t, db, userID, homeColl)

		movieID := addVideo(t, db, movieColl, "Storm Front")
		homeID := addVideo(t, db, homeColl, "Storm at the lake")

		movies, homeMovies, err := repository.SearchVideos(bg(), db, userID,
			repository.SearchParams{Query: "storm", Limit: 50})
		require.NoError(t, err)

		movieIDs := videoHitIDs(movies)
		homeIDs := videoHitIDs(homeMovies)

		assert.Equal(t, []int64{movieID}, movieIDs)
		assert.Equal(t, []int64{homeID}, homeIDs)
		// Each excluded from the other bucket
		assert.NotContains(t, movieIDs, homeID)
		assert.NotContains(t, homeIDs, movieID)
	})

	t.Run("scopes_to_user_collection_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		alice := createUser(t, db)
		bob := createUser(t, db)
		aliceColl := createCollection(t, db, constants.CollectionTypeMovie)
		bobColl := createCollection(t, db, constants.CollectionTypeMovie)
		grantAccess(t, db, alice, aliceColl)
		grantAccess(t, db, bob, bobColl)

		aliceItem := addVideo(t, db, aliceColl, "Storm Front")
		bobItem := addVideo(t, db, bobColl, "Storm Front")

		movies, _, err := repository.SearchVideos(bg(), db, alice,
			repository.SearchParams{Query: "storm", Limit: 50})
		require.NoError(t, err)
		got := videoHitIDs(movies)
		assert.Equal(t, []int64{aliceItem}, got)
		assert.NotContains(t, got, bobItem)
	})

	t.Run("excludes_hidden_items", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		grantAccess(t, db, userID, collID)

		visible := addVideo(t, db, collID, "Storm Front")
		hidden := addVideo(t, db, collID, "Storm Front Hidden")
		hideItem(t, db, hidden)

		movies, _, err := repository.SearchVideos(bg(), db, userID,
			repository.SearchParams{Query: "storm", Limit: 50})
		require.NoError(t, err)
		got := videoHitIDs(movies)
		assert.Contains(t, got, visible)
		assert.NotContains(t, got, hidden)
	})
}

func videoHitIDs(hits []repository.VideoSearchHit) []int64 {
	out := make([]int64, len(hits))
	for i, h := range hits {
		out[i] = h.ID
	}
	return out
}

// ─── Audio search ──────────────────────────────────────────────────────────

func TestSearchAudioArtists(t *testing.T) {
	t.Run("partitions_music_artists_vs_show_artists", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		musicColl := createCollection(t, db, constants.CollectionTypeMusic)
		showColl := createCollection(t, db, constants.CollectionTypeAudioShow)
		grantAccess(t, db, userID, musicColl)
		grantAccess(t, db, userID, showColl)

		// Music artist has albums.
		musicArtistID, err := repository.ArtistUpsert(bg(), db, "Aurora Echo")
		require.NoError(t, err)
		albumID := createAlbum(t, db, &musicArtistID, musicColl)
		_ = addAudioTrack(t, db, musicColl, "Track One", &musicArtistID, &albumID)

		// Show artist has tracks in audio:show, no album.
		showArtistID, err := repository.ArtistUpsert(bg(), db, "Aurora Sunrise")
		require.NoError(t, err)
		_ = addAudioTrack(t, db, showColl, "Episode 1", &showArtistID, nil)

		artists, shows, err := repository.SearchAudioArtists(bg(), db, userID,
			repository.SearchParams{Query: "aurora", Limit: 50})
		require.NoError(t, err)

		artistIDs := artistHitIDs(artists)
		showIDs := artistHitIDs(shows)

		assert.Contains(t, artistIDs, musicArtistID)
		assert.NotContains(t, artistIDs, showArtistID)
		assert.Contains(t, showIDs, showArtistID)
		assert.NotContains(t, showIDs, musicArtistID)
	})

	t.Run("excludes_artists_in_inaccessible_collections", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		alice := createUser(t, db)
		aliceColl := createCollection(t, db, constants.CollectionTypeMusic)
		bobColl := createCollection(t, db, constants.CollectionTypeMusic)
		grantAccess(t, db, alice, aliceColl)
		// Bob has his own collection but Alice has no access to it.

		aliceArtist, err := repository.ArtistUpsert(bg(), db, "Sigma Test")
		require.NoError(t, err)
		aliceAlbum := createAlbum(t, db, &aliceArtist, aliceColl)
		_ = addAudioTrack(t, db, aliceColl, "Some track", &aliceArtist, &aliceAlbum)

		bobArtist, err := repository.ArtistUpsert(bg(), db, "Sigma Other")
		require.NoError(t, err)
		bobAlbum := createAlbum(t, db, &bobArtist, bobColl)
		_ = addAudioTrack(t, db, bobColl, "Other track", &bobArtist, &bobAlbum)

		artists, _, err := repository.SearchAudioArtists(bg(), db, alice,
			repository.SearchParams{Query: "sigma", Limit: 50})
		require.NoError(t, err)
		got := artistHitIDs(artists)
		assert.Contains(t, got, aliceArtist)
		assert.NotContains(t, got, bobArtist)
	})
}

func artistHitIDs(hits []repository.AudioArtistSearchHit) []int64 {
	out := make([]int64, len(hits))
	for i, h := range hits {
		out[i] = h.ID
	}
	return out
}

func TestSearchAudioAlbums(t *testing.T) {
	t.Run("matches_album_name_and_excludes_others", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		grantAccess(t, db, userID, collID)

		artistID := createArtist(t, db)
		match, _, err := repository.AlbumUpsert(bg(), db, "Crimson Tide", &artistID, nil, nil, collID, false)
		require.NoError(t, err)
		miss, _, err := repository.AlbumUpsert(bg(), db, "Other Album", &artistID, nil, nil, collID, false)
		require.NoError(t, err)

		hits, err := repository.SearchAudioAlbums(bg(), db, userID,
			repository.SearchParams{Query: "crimson", Limit: 50})
		require.NoError(t, err)

		var got []int64
		for _, h := range hits {
			got = append(got, h.ID)
		}
		assert.Contains(t, got, match)
		assert.NotContains(t, got, miss)
	})

	t.Run("scopes_to_user_collection_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		alice := createUser(t, db)
		aliceColl := createCollection(t, db, constants.CollectionTypeMusic)
		bobColl := createCollection(t, db, constants.CollectionTypeMusic)
		grantAccess(t, db, alice, aliceColl)

		artistID := createArtist(t, db)
		aliceAlbum, _, err := repository.AlbumUpsert(bg(), db, "Crimson Tide", &artistID, nil, nil, aliceColl, false)
		require.NoError(t, err)
		bobAlbum, _, err := repository.AlbumUpsert(bg(), db, "Crimson Tide", &artistID, nil, nil, bobColl, false)
		require.NoError(t, err)

		hits, err := repository.SearchAudioAlbums(bg(), db, alice,
			repository.SearchParams{Query: "crimson", Limit: 50})
		require.NoError(t, err)
		var got []int64
		for _, h := range hits {
			got = append(got, h.ID)
		}
		assert.Contains(t, got, aliceAlbum)
		assert.NotContains(t, got, bobAlbum)
	})
}

func TestSearchAudioTracks(t *testing.T) {
	t.Run("matches_track_title_only_not_artist_or_album_text", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		grantAccess(t, db, userID, collID)

		// Artist name and album name both contain "phoenix" but the track title doesn't.
		artistID, err := repository.ArtistUpsert(bg(), db, "Phoenix Phoenix")
		require.NoError(t, err)
		albumID, _, err := repository.AlbumUpsert(bg(), db, "Phoenix Album", &artistID, nil, nil, collID, false)
		require.NoError(t, err)
		_ = addAudioTrack(t, db, collID, "Cardinal", &artistID, &albumID)

		// A second track whose title actually contains "phoenix".
		matching := addAudioTrack(t, db, collID, "Phoenix Rising", &artistID, &albumID)

		hits, err := repository.SearchAudioTracks(bg(), db, userID,
			repository.SearchParams{Query: "phoenix", Limit: 50})
		require.NoError(t, err)
		var got []int64
		for _, h := range hits {
			got = append(got, h.ID)
		}
		assert.Equal(t, []int64{matching}, got, "track search must not pick up artist/album text")

		// Verify the matching hit carries joined artist/album fields.
		require.Len(t, hits, 1)
		require.NotNil(t, hits[0].ArtistName)
		assert.Equal(t, "Phoenix Phoenix", *hits[0].ArtistName)
		require.NotNil(t, hits[0].AlbumName)
		assert.Equal(t, "Phoenix Album", *hits[0].AlbumName)
		require.NotNil(t, hits[0].AlbumID)
		assert.Equal(t, albumID, *hits[0].AlbumID)
	})

	t.Run("includes_show_episodes_with_collection_type_set", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		showColl := createCollection(t, db, constants.CollectionTypeAudioShow)
		grantAccess(t, db, userID, showColl)

		showArtistID, err := repository.ArtistUpsert(bg(), db, "The Daily Tide")
		require.NoError(t, err)
		episode := addAudioTrack(t, db, showColl, "Storm Episode", &showArtistID, nil)

		hits, err := repository.SearchAudioTracks(bg(), db, userID,
			repository.SearchParams{Query: "storm", Limit: 50})
		require.NoError(t, err)
		require.Len(t, hits, 1)
		assert.Equal(t, episode, hits[0].ID)
		assert.Equal(t, string(constants.CollectionTypeAudioShow), hits[0].CollectionType)
	})

	t.Run("scopes_to_user_collection_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		alice := createUser(t, db)
		aliceColl := createCollection(t, db, constants.CollectionTypeMusic)
		bobColl := createCollection(t, db, constants.CollectionTypeMusic)
		grantAccess(t, db, alice, aliceColl)

		aliceTrack := addAudioTrack(t, db, aliceColl, "Phoenix Rising", nil, nil)
		bobTrack := addAudioTrack(t, db, bobColl, "Phoenix Rising", nil, nil)

		hits, err := repository.SearchAudioTracks(bg(), db, alice,
			repository.SearchParams{Query: "phoenix", Limit: 50})
		require.NoError(t, err)
		var got []int64
		for _, h := range hits {
			got = append(got, h.ID)
		}
		assert.Equal(t, []int64{aliceTrack}, got)
		assert.NotContains(t, got, bobTrack)
	})
}
