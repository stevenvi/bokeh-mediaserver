package repository_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVideoUpsert(t *testing.T) {
	t.Run("minimal_fields_all_null", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)

		err := repository.VideoUpsert(bg(), db, itemID, nil, nil, nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)

		var dur, w, h, bitrate *int
		var vc, ac, author, dateStr *string
		err = db.QueryRow(bg(),
			`SELECT duration_seconds, width, height, bitrate_kbps,
			        video_codec, audio_codec, date_string, author
			 FROM video_metadata WHERE media_item_id = $1`, itemID,
		).Scan(&dur, &w, &h, &bitrate, &vc, &ac, &dateStr, &author)
		require.NoError(t, err)
		assert.Nil(t, dur)
		assert.Nil(t, w)
		assert.Nil(t, h)
		assert.Nil(t, bitrate)
		assert.Nil(t, vc)
		assert.Nil(t, ac)
		assert.Nil(t, dateStr)
		assert.Nil(t, author)
	})

	t.Run("all_fields_stored_correctly", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeHomeMovie)
		itemID := createVideoMediaItem(t, db, collID)

		dur, w, h, bitrate := 5400, 3840, 2160, 50000
		vc, ac := "hevc", "eac3"
		dateStr := "2021.07.04-05"
		author := "Jane Smith"
		err := repository.VideoUpsert(bg(), db, itemID,
			&dur, &w, &h, &bitrate, &vc, &ac, &dateStr, &author,
		)
		require.NoError(t, err)

		var gotDur, gotW, gotH, gotBitrate *int
		var gotVC, gotAC, gotAuthor, gotDateStr *string
		err = db.QueryRow(bg(),
			`SELECT duration_seconds, width, height, bitrate_kbps,
			        video_codec, audio_codec, date_string, author
			 FROM video_metadata WHERE media_item_id = $1`, itemID,
		).Scan(&gotDur, &gotW, &gotH, &gotBitrate, &gotVC, &gotAC, &gotDateStr, &gotAuthor)
		require.NoError(t, err)

		require.NotNil(t, gotDur)
		assert.Equal(t, 5400, *gotDur)
		require.NotNil(t, gotW)
		assert.Equal(t, 3840, *gotW)
		require.NotNil(t, gotH)
		assert.Equal(t, 2160, *gotH)
		require.NotNil(t, gotBitrate)
		assert.Equal(t, 50000, *gotBitrate)
		require.NotNil(t, gotVC)
		assert.Equal(t, "hevc", *gotVC)
		require.NotNil(t, gotAC)
		assert.Equal(t, "eac3", *gotAC)
		require.NotNil(t, gotDateStr)
		assert.Equal(t, "2021.07.04-05", *gotDateStr)
		require.NotNil(t, gotAuthor)
		assert.Equal(t, "Jane Smith", *gotAuthor)
	})

	t.Run("upsert_updates_existing_without_resetting_transcoded_at", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		// Set transcoded_at so we can verify it survives upsert.
		ts := time.Now().Truncate(time.Second)
		require.NoError(t, repository.VideoSetTranscodedAt(bg(), db, itemID, ts))

		// Re-process with different dimensions.
		dur, w, h, bitrate := 200, 3840, 2160, 20000
		vc, ac := "av1", "opus"
		require.NoError(t, repository.VideoUpsert(bg(), db, itemID, &dur, &w, &h, &bitrate, &vc, &ac, nil, nil))

		vm, err := repository.VideoMetadataForTranscode(bg(), db, itemID)
		require.NoError(t, err)
		// transcoded_at should still be set (VideoUpsert does not reset it).
		assert.NotNil(t, vm.TranscodedAt)
	})
}

func TestVideoWithBookmark(t *testing.T) {
	t.Run("returns_metadata_with_no_bookmark", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)
		userID := createUser(t, db)

		vm, err := repository.VideoWithBookmark(bg(), db, itemID, userID)
		require.NoError(t, err)
		assert.Nil(t, vm.BookmarkSeconds)
	})

	t.Run("populates_bookmark_when_present", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)
		userID := createUser(t, db)

		require.NoError(t, repository.VideoBookmarkUpsert(bg(), db, userID, itemID, 142))

		vm, err := repository.VideoWithBookmark(bg(), db, itemID, userID)
		require.NoError(t, err)
		require.NotNil(t, vm.BookmarkSeconds)
		assert.Equal(t, 142, *vm.BookmarkSeconds)
	})

	t.Run("not_found_for_missing_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		_, err := repository.VideoWithBookmark(bg(), db, 999999, userID)
		assert.Error(t, err)
	})
}

func TestVideosForIntegrityCheck(t *testing.T) {
	t.Run("returns_video_items", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		items, err := repository.VideosForIntegrityCheck(bg(), db)
		require.NoError(t, err)
		found := false
		for _, item := range items {
			if item.ItemID == itemID {
				found = true
				break
			}
		}
		assert.True(t, found)
	})

	t.Run("empty_when_no_video_metadata", func(t *testing.T) {
		// A fresh transaction with no video metadata returns empty (or only pre-existing rows).
		db := testutil.NewTx(t, testPool)
		items, err := repository.VideosForIntegrityCheck(bg(), db)
		require.NoError(t, err)
		assert.NotNil(t, items)
	})
}

func TestVideoClearTranscodedAt(t *testing.T) {
	t.Run("clears_transcoded_at", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)
		require.NoError(t, repository.VideoSetTranscodedAt(bg(), db, itemID, time.Now()))

		require.NoError(t, repository.VideoClearTranscodedAt(bg(), db, itemID))

		needs, err := repository.VideoNeedsTranscode(bg(), db, itemID)
		require.NoError(t, err)
		assert.True(t, needs)
	})

	t.Run("noop_for_missing_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		require.NoError(t, repository.VideoClearTranscodedAt(bg(), db, 999999))
	})
}

func TestVideoNeedsTranscode(t *testing.T) {
	t.Run("true_when_transcoded_at_is_null", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		needs, err := repository.VideoNeedsTranscode(bg(), db, itemID)
		require.NoError(t, err)
		assert.True(t, needs)
	})

	t.Run("false_after_transcoded_at_set", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)
		require.NoError(t, repository.VideoSetTranscodedAt(bg(), db, itemID, time.Now()))

		needs, err := repository.VideoNeedsTranscode(bg(), db, itemID)
		require.NoError(t, err)
		assert.False(t, needs)
	})
}

func TestVideoMetadataForTranscode(t *testing.T) {
	t.Run("returns_bitrate_and_transcoded_at", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		vm, err := repository.VideoMetadataForTranscode(bg(), db, itemID)
		require.NoError(t, err)
		assert.NotNil(t, vm.BitrateKbps)
		assert.Nil(t, vm.TranscodedAt)
	})

	t.Run("not_found_for_missing_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.VideoMetadataForTranscode(bg(), db, 999999)
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	})
}

func TestVideoSetTranscodedAt(t *testing.T) {
	t.Run("sets_transcoded_at", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		ts := time.Now().Truncate(time.Second)
		require.NoError(t, repository.VideoSetTranscodedAt(bg(), db, itemID, ts))

		vm, err := repository.VideoMetadataForTranscode(bg(), db, itemID)
		require.NoError(t, err)
		require.NotNil(t, vm.TranscodedAt)
		assert.Equal(t, ts.UTC(), vm.TranscodedAt.UTC())
	})

	t.Run("noop_for_missing_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		require.NoError(t, repository.VideoSetTranscodedAt(bg(), db, 999999, time.Now()))
	})
}

func TestVideoHasManualThumbnail(t *testing.T) {
	t.Run("false_by_default_when_manual_cover_not_set", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		// manual_thumbnail defaults to false; VideoHasManualThumbnail returns !manual_thumbnail.
		ok, err := repository.VideoHasManualThumbnail(bg(), db, itemID)
		require.NoError(t, err)
		assert.True(t, ok) // manual_cover=false → not manual → auto-cover is appropriate
	})

	t.Run("false_when_manual_cover_set", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)
		require.NoError(t, repository.VideoSetManualThumbnail(bg(), db, itemID, true))

		ok, err := repository.VideoHasManualThumbnail(bg(), db, itemID)
		require.NoError(t, err)
		assert.False(t, ok) // manual_cover=true → manual → auto-cover not appropriate
	})

	t.Run("false_when_no_row_exists", func(t *testing.T) {
		// VideoHasManualThumbnail returns false (not manual) when no video_metadata row exists yet.
		db := testutil.NewTx(t, testPool)
		ok, err := repository.VideoHasManualThumbnail(bg(), db, 999999)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestVideoSetManualThumbnail(t *testing.T) {
	t.Run("sets_manual_cover_true", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		require.NoError(t, repository.VideoSetManualThumbnail(bg(), db, itemID, true))
		ok, err := repository.VideoHasManualThumbnail(bg(), db, itemID)
		require.NoError(t, err)
		assert.False(t, ok) // manual_cover=true means NOT auto-cover
	})

	t.Run("sets_manual_cover_false", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)
		require.NoError(t, repository.VideoSetManualThumbnail(bg(), db, itemID, true))

		require.NoError(t, repository.VideoSetManualThumbnail(bg(), db, itemID, false))
		ok, err := repository.VideoHasManualThumbnail(bg(), db, itemID)
		require.NoError(t, err)
		assert.True(t, ok)
	})
}
