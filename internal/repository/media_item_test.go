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

func TestMediaItemUpsert(t *testing.T) {
	t.Run("inserts_new_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)

		id, wasUnchanged, err := repository.MediaItemUpsert(bg(), db, collID,
			"Photo", "photos/one.jpg", 2048, "abc123", "image/jpeg",
		)
		require.NoError(t, err)
		assert.Greater(t, id, int64(0))
		assert.False(t, wasUnchanged) // new row — not unchanged
	})

	t.Run("upsert_returns_unchanged_for_same_file", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)

		id1, _, err := repository.MediaItemUpsert(bg(), db, collID,
			"Photo", "photos/same.jpg", 2048, "samehash", "image/jpeg",
		)
		require.NoError(t, err)

		id2, wasUnchanged, err := repository.MediaItemUpsert(bg(), db, collID,
			"Photo", "photos/same.jpg", 2048, "samehash", "image/jpeg",
		)
		require.NoError(t, err)
		assert.Equal(t, id1, id2)
		assert.True(t, wasUnchanged)
	})
}


func TestMediaItemForProcessing(t *testing.T) {
	t.Run("returns_path_and_hash", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		id, _, err := repository.MediaItemUpsert(bg(), db, collID,
			"Photo", "photos/proc.jpg", 1024, "proc-hash", "image/jpeg",
		)
		require.NoError(t, err)

		relPath, mimeType, hash, err := repository.MediaItemForProcessing(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, "photos/proc.jpg", relPath)
		assert.Equal(t, "image/jpeg", mimeType)
		assert.Equal(t, "proc-hash", hash)
	})

	t.Run("error_for_missing_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, _, _, err := repository.MediaItemForProcessing(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestDeleteMediaItem(t *testing.T) {
	t.Run("deletes_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)

		require.NoError(t, repository.DeleteMediaItem(bg(), db, itemID))

		_, _, _, err := repository.MediaItemForProcessing(bg(), db, itemID)
		assert.Error(t, err)
	})

	t.Run("noop_for_missing_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		require.NoError(t, repository.DeleteMediaItem(bg(), db, 999999))
	})
}

func TestMediaItemFileHashAndPath(t *testing.T) {
	t.Run("returns_hash_and_path_for_accessible_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)
		id, _, err := repository.MediaItemUpsert(bg(), db, collID,
			"Photo", "photos/hashed.jpg", 1024, "my-hash", "image/jpeg",
		)
		require.NoError(t, err)

		hash, relPath, err := repository.MediaItemFileHashAndPath(bg(), db, id, userID)
		require.NoError(t, err)
		assert.Equal(t, "my-hash", hash)
		assert.Equal(t, "photos/hashed.jpg", relPath)
	})

	t.Run("error_without_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)

		_, _, err := repository.MediaItemFileHashAndPath(bg(), db, itemID, userID)
		assert.Error(t, err)
	})
}

func TestMediaItemUpdateTitle(t *testing.T) {
	t.Run("updates_title", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)

		require.NoError(t, repository.MediaItemUpdateTitle(bg(), db, itemID, "New Title"))

		var got string
		err := db.QueryRow(bg(), `SELECT title FROM media_items WHERE id = $1`, itemID).Scan(&got)
		require.NoError(t, err)
		assert.Equal(t, "New Title", got)
	})

	t.Run("noop_for_missing_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		require.NoError(t, repository.MediaItemUpdateTitle(bg(), db, 999999, "Ghost"))
	})
}


func TestMediaItemSetHidden(t *testing.T) {
	t.Run("hides_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		createPhotoMetadata(t, db, itemID)

		require.NoError(t, repository.MediaItemSetHidden(bg(), db, itemID, true))

		var hiddenAt *time.Time
		err := db.QueryRow(bg(), `SELECT hidden_at FROM media_items WHERE id = $1`, itemID).Scan(&hiddenAt)
		require.NoError(t, err)
		assert.NotNil(t, hiddenAt) // hidden_at should be set
	})

	t.Run("unhides_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		createPhotoMetadata(t, db, itemID)
		require.NoError(t, repository.MediaItemSetHidden(bg(), db, itemID, true))

		require.NoError(t, repository.MediaItemSetHidden(bg(), db, itemID, false))

		var hiddenAt *time.Time
		err := db.QueryRow(bg(), `SELECT hidden_at FROM media_items WHERE id = $1`, itemID).Scan(&hiddenAt)
		require.NoError(t, err)
		assert.Nil(t, hiddenAt) // hidden_at should be cleared
	})
}

func TestMediaItemFindExistingHashes(t *testing.T) {
	t.Run("returns_matching_hashes", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		_, _, err := repository.MediaItemUpsert(bg(), db, collID,
			"Photo", "photos/findme.jpg", 1024, "unique-hash-abc", "image/jpeg",
		)
		require.NoError(t, err)

		found, err := repository.MediaItemFindExistingHashes(bg(), db, []string{"unique-hash-abc", "nonexistent-hash"})
		require.NoError(t, err)
		assert.Contains(t, found, "unique-hash-abc")
		assert.NotContains(t, found, "nonexistent-hash")
	})

	t.Run("empty_for_no_matches", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		found, err := repository.MediaItemFindExistingHashes(bg(), db, []string{"no-match-1", "no-match-2"})
		require.NoError(t, err)
		assert.Empty(t, found)
	})
}

func TestMediaItemsStale(t *testing.T) {
	t.Run("returns_items_missing_over_90_days", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		testutil.MustExec(t, db,
			`UPDATE media_items SET missing_since = now() - interval '100 days' WHERE id = $1`, itemID)

		stale, err := repository.MediaItemsStale(bg(), db)
		require.NoError(t, err)
		found := false
		for _, s := range stale {
			if s.ID == itemID {
				found = true
				break
			}
		}
		assert.True(t, found)
	})

	t.Run("excludes_recently_missing_items", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		testutil.MustExec(t, db,
			`UPDATE media_items SET missing_since = now() - interval '1 day' WHERE id = $1`, itemID)

		stale, err := repository.MediaItemsStale(bg(), db)
		require.NoError(t, err)
		for _, s := range stale {
			assert.NotEqual(t, itemID, s.ID)
		}
	})
}

func TestMediaItemHashesByCollection(t *testing.T) {
	t.Run("returns_hashes_in_collection", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		_, _, err := repository.MediaItemUpsert(bg(), db, collID,
			"Photo", "photos/hashcoll.jpg", 1024, "col-hash-xyz", "image/jpeg",
		)
		require.NoError(t, err)

		hashes, err := repository.MediaItemHashesByCollection(bg(), db, collID)
		require.NoError(t, err)
		assert.Contains(t, hashes, "col-hash-xyz")
	})

	t.Run("empty_for_empty_collection", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		hashes, err := repository.MediaItemHashesByCollection(bg(), db, collID)
		require.NoError(t, err)
		assert.Empty(t, hashes)
	})
}

func TestMediaItemRandomHashWithVariants(t *testing.T) {
	t.Run("returns_hash_when_variants_exist", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		createPhotoMetadata(t, db, itemID)
		require.NoError(t, repository.PhotoUpdateVariants(bg(), db, itemID))

		hash, err := repository.MediaItemRandomHashWithVariants(bg(), db, collID)
		require.NoError(t, err)
		assert.NotEmpty(t, hash)
	})

	t.Run("no_rows_when_no_variants_generated", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		// Item exists but no photo_metadata (hence no variants).
		createMediaItem(t, db, collID)

		_, err := repository.MediaItemRandomHashWithVariants(bg(), db, collID)
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	})
}

func TestMediaItemCollectionID(t *testing.T) {
	t.Run("returns_collection_id", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)

		got, err := repository.MediaItemCollectionID(bg(), db, itemID)
		require.NoError(t, err)
		assert.Equal(t, collID, got)
	})

	t.Run("error_for_missing_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.MediaItemCollectionID(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestMediaItemRootCollectionID(t *testing.T) {
	t.Run("returns_root_collection_id", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		rootID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, rootID)

		got, err := repository.MediaItemRootCollectionID(bg(), db, itemID)
		require.NoError(t, err)
		assert.Equal(t, rootID, got)
	})

	t.Run("error_for_missing_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.MediaItemRootCollectionID(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestMediaItemRootCollectionType(t *testing.T) {
	t.Run("returns_collection_type", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)

		colType, err := repository.MediaItemRootCollectionType(bg(), db, itemID)
		require.NoError(t, err)
		assert.Equal(t, constants.CollectionTypePhoto, colType)
	})

	t.Run("error_for_missing_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.MediaItemRootCollectionType(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestMediaItemGetAudioStreamInfo(t *testing.T) {
	t.Run("returns_path_and_mime_for_accessible_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		grantAccess(t, db, userID, collID)
		itemID := createAudioMediaItem(t, db, collID)

		relPath, mimeType, err := repository.MediaItemGetAudioStreamInfo(bg(), db, itemID, userID)
		require.NoError(t, err)
		assert.NotEmpty(t, relPath)
		assert.Equal(t, "audio/mpeg", mimeType)
	})

	t.Run("error_without_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMusic)
		itemID := createAudioMediaItem(t, db, collID)

		_, _, err := repository.MediaItemGetAudioStreamInfo(bg(), db, itemID, userID)
		assert.Error(t, err)
	})
}

func TestMediaItemGetVideoStreamInfo(t *testing.T) {
	t.Run("returns_path_mime_hash_for_accessible_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		grantAccess(t, db, userID, collID)
		itemID := createVideoMediaItem(t, db, collID)

		relPath, mimeType, hash, err := repository.MediaItemGetVideoStreamInfo(bg(), db, itemID, userID)
		require.NoError(t, err)
		assert.NotEmpty(t, relPath)
		assert.Equal(t, "video/mp4", mimeType)
		assert.NotEmpty(t, hash)
	})

	t.Run("error_without_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)

		_, _, _, err := repository.MediaItemGetVideoStreamInfo(bg(), db, itemID, userID)
		assert.Error(t, err)
	})
}

func TestMediaItemVideosByCollection(t *testing.T) {
	t.Run("returns_videos_with_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		grantAccess(t, db, userID, collID)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		items, err := repository.MediaItemVideosByCollection(bg(), db, collID, userID, constants.CollectionTypeMovie, 10, 0)
		require.NoError(t, err)
		ids := make([]int64, len(items))
		for i, it := range items {
			ids[i] = it.ID
		}
		assert.Contains(t, ids, itemID)
	})

	t.Run("empty_without_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		items, err := repository.MediaItemVideosByCollection(bg(), db, collID, userID, constants.CollectionTypeMovie, 10, 0)
		require.NoError(t, err)
		assert.Empty(t, items)
	})
}

func TestPhotoItems(t *testing.T) {
	t.Run("returns_items_with_photo_metadata", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		ts := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
		createPhotoMetadataWithDate(t, db, itemID, ts)

		q := repository.PhotoQuery{
			CollectionID: collID,
			Limit:        10,
			Ascending:    true,
		}
		items, err := repository.PhotoItems(bg(), db, q)
		require.NoError(t, err)
		ids := make([]int64, len(items))
		for i, it := range items {
			ids[i] = it.ID
		}
		assert.Contains(t, ids, itemID)
	})

	t.Run("empty_for_collection_with_no_photo_metadata", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		// Item without photo_metadata is not returned (PhotoItems JOINs on photo_metadata).
		createMediaItem(t, db, collID)

		q := repository.PhotoQuery{CollectionID: collID, Limit: 10, Ascending: true}
		items, err := repository.PhotoItems(bg(), db, q)
		require.NoError(t, err)
		assert.Empty(t, items)
	})

	t.Run("ordinal_is_zero_from_repository", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		createPhotoMetadata(t, db, itemID)

		q := repository.PhotoQuery{CollectionID: collID, Limit: 10, Ascending: true}
		items, err := repository.PhotoItems(bg(), db, q)
		require.NoError(t, err)
		require.Len(t, items, 1)
		// Ordinal is set by the handler, not the repository.
		assert.Equal(t, int64(0), items[0].Ordinal)
	})

	t.Run("recursive_false_excludes_subcollection_items", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		rootID := createCollection(t, db, constants.CollectionTypePhoto)
		subID := createSubCollection(t, db, rootID, constants.CollectionTypePhoto)
		itemInRoot := createMediaItem(t, db, rootID)
		itemInSub := createMediaItem(t, db, subID)
		createPhotoMetadata(t, db, itemInRoot)
		createPhotoMetadata(t, db, itemInSub)

		q := repository.PhotoQuery{CollectionID: rootID, Limit: 10, Ascending: true, Recursive: false}
		items, err := repository.PhotoItems(bg(), db, q)
		require.NoError(t, err)
		ids := make([]int64, len(items))
		for i, it := range items {
			ids[i] = it.ID
		}
		assert.Contains(t, ids, itemInRoot)
		assert.NotContains(t, ids, itemInSub)
	})

	t.Run("recursive_true_includes_subcollection_items", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		rootID := createCollection(t, db, constants.CollectionTypePhoto)
		subID := createSubCollection(t, db, rootID, constants.CollectionTypePhoto)
		itemInRoot := createMediaItem(t, db, rootID)
		itemInSub := createMediaItem(t, db, subID)
		createPhotoMetadata(t, db, itemInRoot)
		createPhotoMetadata(t, db, itemInSub)

		q := repository.PhotoQuery{CollectionID: rootID, Limit: 10, Ascending: true, Recursive: true}
		items, err := repository.PhotoItems(bg(), db, q)
		require.NoError(t, err)
		ids := make([]int64, len(items))
		for i, it := range items {
			ids[i] = it.ID
		}
		assert.Contains(t, ids, itemInRoot)
		assert.Contains(t, ids, itemInSub)
	})
}

func TestPhotoStatistics(t *testing.T) {
	t.Run("returns_month_counts", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		ts := time.Date(2022, 3, 15, 0, 0, 0, 0, time.UTC)
		createPhotoMetadataWithDate(t, db, itemID, ts)

		counts, err := repository.PhotoStatistics(bg(), db, collID, false)
		require.NoError(t, err)
		require.Len(t, counts, 1)
		assert.Equal(t, 2022, counts[0].Year)
		assert.Equal(t, 3, counts[0].Month)
		assert.Equal(t, 1, counts[0].Count)
	})

	t.Run("empty_when_no_dated_photos", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		// Photo without created_at is excluded from month counts.
		itemID := createMediaItem(t, db, collID)
		createPhotoMetadata(t, db, itemID) // created_at is nil

		counts, err := repository.PhotoStatistics(bg(), db, collID, false)
		require.NoError(t, err)
		assert.Empty(t, counts)
	})
}

func TestMediaItemMarkMissingSince(t *testing.T) {
	t.Run("marks_stale_items_missing", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		// Backdate indexed_at so it falls before our cutoff.
		testutil.MustExec(t, db,
			`UPDATE media_items SET indexed_at = now() - interval '1 hour' WHERE id = $1`, itemID)

		n, err := repository.MediaItemMarkMissingSince(bg(), db, collID, time.Now())
		require.NoError(t, err)
		assert.Equal(t, int64(1), n)

		var missingSince *time.Time
		err = db.QueryRow(bg(),
			`SELECT missing_since FROM media_items WHERE id = $1`, itemID,
		).Scan(&missingSince)
		require.NoError(t, err)
		assert.NotNil(t, missingSince)
	})

	t.Run("zero_rows_when_all_recently_indexed", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		createMediaItem(t, db, collID) // indexed_at defaults to now()

		// Use a cutoff in the past — nothing should be older than it.
		cutoff := time.Now().Add(-24 * time.Hour)
		n, err := repository.MediaItemMarkMissingSince(bg(), db, collID, cutoff)
		require.NoError(t, err)
		assert.Equal(t, int64(0), n)
	})
}
