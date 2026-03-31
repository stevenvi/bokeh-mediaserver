package repository_test

import (
	"testing"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVideoBookmarkUpsert(t *testing.T) {
	t.Run("inserts_bookmark", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		require.NoError(t, repository.VideoBookmarkUpsert(bg(), db, userID, itemID, 90))

		vm, err := repository.VideoWithBookmark(bg(), db, itemID, userID)
		require.NoError(t, err)
		require.NotNil(t, vm.BookmarkSeconds)
		assert.Equal(t, 90, *vm.BookmarkSeconds)
	})

	t.Run("updates_existing_bookmark", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)

		require.NoError(t, repository.VideoBookmarkUpsert(bg(), db, userID, itemID, 90))
		require.NoError(t, repository.VideoBookmarkUpsert(bg(), db, userID, itemID, 300))

		vm, err := repository.VideoWithBookmark(bg(), db, itemID, userID)
		require.NoError(t, err)
		require.NotNil(t, vm.BookmarkSeconds)
		assert.Equal(t, 300, *vm.BookmarkSeconds)
	})
}

func TestVideoBookmarkDelete(t *testing.T) {
	t.Run("removes_bookmark", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		createVideoMetadata(t, db, itemID)
		require.NoError(t, repository.VideoBookmarkUpsert(bg(), db, userID, itemID, 60))

		require.NoError(t, repository.VideoBookmarkDelete(bg(), db, userID, itemID))

		vm, err := repository.VideoWithBookmark(bg(), db, itemID, userID)
		require.NoError(t, err)
		assert.Nil(t, vm.BookmarkSeconds)
	})

	t.Run("noop_when_no_bookmark_exists", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypeMovie)
		itemID := createVideoMediaItem(t, db, collID)
		require.NoError(t, repository.VideoBookmarkDelete(bg(), db, userID, itemID))
	})
}
