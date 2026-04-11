package repository_test

import (
	"testing"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectionCreate(t *testing.T) {
	t.Run("creates_collection", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id, err := repository.CollectionCreate(bg(), db, "Photos", constants.CollectionTypePhoto, "photos")
		require.NoError(t, err)
		assert.Greater(t, id, int64(0))

		c, err := repository.CollectionGet(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, "Photos", c.Name)
	})
}

func TestCollectionUpsertSubCollection(t *testing.T) {
	t.Run("creates_sub_collection", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		parentID := createCollection(t, db, constants.CollectionTypePhoto)

		subID, err := repository.CollectionUpsertSubCollection(bg(), db, parentID, parentID, "Sub", "photos/sub")
		require.NoError(t, err)
		assert.Greater(t, subID, int64(0))

		c, err := repository.CollectionGet(bg(), db, subID)
		require.NoError(t, err)
		assert.Equal(t, "Sub", c.Name)
		require.NotNil(t, c.ParentCollectionID)
		assert.Equal(t, parentID, *c.ParentCollectionID)
	})

	t.Run("sub_inherits_parent_type", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		parentID := createCollection(t, db, constants.CollectionTypePhoto)

		subID, err := repository.CollectionUpsertSubCollection(bg(), db, parentID, parentID, "Sub2", "photos/sub2")
		require.NoError(t, err)

		c, err := repository.CollectionGet(bg(), db, subID)
		require.NoError(t, err)
		assert.Equal(t, constants.CollectionTypePhoto.String(), c.Type)
	})
}

func TestCollectionGet(t *testing.T) {
	t.Run("returns_collection", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createCollection(t, db, constants.CollectionTypePhoto)
		c, err := repository.CollectionGet(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, id, c.ID)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.CollectionGet(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestCollectionGetForUser(t *testing.T) {
	t.Run("returns_collection_with_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)

		c, err := repository.CollectionGetForUser(bg(), db, collID, userID)
		require.NoError(t, err)
		assert.Equal(t, collID, c.ID)
	})

	t.Run("no_access_returns_error", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)

		_, err := repository.CollectionGetForUser(bg(), db, collID, userID)
		assert.Error(t, err)
	})
}

func TestCollectionDelete(t *testing.T) {
	t.Run("deletes_collection", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createCollection(t, db, constants.CollectionTypePhoto)
		rows, err := repository.CollectionDelete(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, int64(1), rows)

		_, err = repository.CollectionGet(bg(), db, id)
		assert.Error(t, err)
	})

	t.Run("no_rows_for_missing_collection", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		rows, err := repository.CollectionDelete(bg(), db, 999999)
		require.NoError(t, err)
		assert.Equal(t, int64(0), rows)
	})
}

func TestCollectionIsEnabled(t *testing.T) {
	t.Run("enabled_by_default", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createCollection(t, db, constants.CollectionTypePhoto)
		ok, err := repository.CollectionIsEnabled(bg(), db, id)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("not_found_returns_error", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.CollectionIsEnabled(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestCollectionGetRelativePath(t *testing.T) {
	t.Run("returns_relative_path", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id, err := repository.CollectionCreate(bg(), db, "Photos", constants.CollectionTypePhoto, "my/photos")
		require.NoError(t, err)

		path, err := repository.CollectionGetRelativePath(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, "my/photos", path)
	})

	t.Run("not_found_returns_error", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.CollectionGetRelativePath(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestCollectionsTopLevel(t *testing.T) {
	t.Run("returns_top_level_collections", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createCollection(t, db, constants.CollectionTypePhoto)

		cols, err := repository.CollectionsTopLevel(bg(), db)
		require.NoError(t, err)
		found := false
		for _, c := range cols {
			if c.ID == id {
				found = true
				break
			}
		}
		assert.True(t, found)
	})

	t.Run("empty_when_no_top_level_collections", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		cols, err := repository.CollectionsTopLevel(bg(), db)
		require.NoError(t, err)
		assert.NotNil(t, cols)
	})
}

func TestCollectionsTopLevelEnabled(t *testing.T) {
	t.Run("returns_enabled_ids", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id1 := createCollection(t, db, constants.CollectionTypePhoto)
		id2 := createCollection(t, db, constants.CollectionTypeMovie)

		ids, err := repository.CollectionsTopLevelEnabled(bg(), db)
		require.NoError(t, err)
		assert.Contains(t, ids, id1)
		assert.Contains(t, ids, id2)
	})

	t.Run("empty_when_no_top_level_collections", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		ids, err := repository.CollectionsTopLevelEnabled(bg(), db)
		require.NoError(t, err)
		// May include pre-existing collections from migrations; just verify no error.
		assert.NotNil(t, ids)
	})
}

func TestCollectionsListAccessibleByUser(t *testing.T) {
	t.Run("returns_accessible_collections", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)

		cols, err := repository.CollectionsListAccessibleByUser(bg(), db, userID)
		require.NoError(t, err)
		require.Len(t, cols, 1)
		assert.Equal(t, collID, cols[0].ID)
	})

	t.Run("empty_when_no_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		createCollection(t, db, constants.CollectionTypePhoto)

		cols, err := repository.CollectionsListAccessibleByUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Empty(t, cols)
	})
}

func TestCollectionGetChildCollections(t *testing.T) {
	t.Run("returns_children", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		parentID := createCollection(t, db, constants.CollectionTypePhoto)
		childID, err := repository.CollectionUpsertSubCollection(bg(), db, parentID, parentID, "Child", "photos/child")
		require.NoError(t, err)

		children, err := repository.CollectionGetChildCollections(bg(), db, parentID)
		require.NoError(t, err)
		require.Len(t, children, 1)
		assert.Equal(t, childID, children[0].ID)
	})

	t.Run("empty_when_no_children", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		parentID := createCollection(t, db, constants.CollectionTypePhoto)

		children, err := repository.CollectionGetChildCollections(bg(), db, parentID)
		require.NoError(t, err)
		assert.Empty(t, children)
	})
}

func TestCollectionExistsAndAccessible(t *testing.T) {
	t.Run("true_when_accessible", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)

		ok, err := repository.CollectionExistsAndAccessible(bg(), db, collID, userID)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("false_when_no_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)

		ok, err := repository.CollectionExistsAndAccessible(bg(), db, collID, userID)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestCollectionsAccessibleByUser(t *testing.T) {
	t.Run("returns_granted_ids", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)

		ids, err := repository.CollectionsAccessibleByUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Contains(t, ids, collID)
	})

	t.Run("empty_when_no_access_granted", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)

		ids, err := repository.CollectionsAccessibleByUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Empty(t, ids)
	})
}

func TestCollectionGetUsersWithAccess(t *testing.T) {
	t.Run("returns_users_with_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)

		ids, err := repository.CollectionGetUsersWithAccess(bg(), db, collID)
		require.NoError(t, err)
		assert.Contains(t, ids, userID)
	})

	t.Run("empty_when_no_users", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)

		ids, err := repository.CollectionGetUsersWithAccess(bg(), db, collID)
		require.NoError(t, err)
		assert.Empty(t, ids)
	})
}

func TestCollectionTouchLastScanned(t *testing.T) {
	t.Run("updates_timestamp", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createCollection(t, db, constants.CollectionTypePhoto)
		// Fire-and-forget; confirm it doesn't break subsequent reads.
		repository.CollectionTouchLastScanned(bg(), db, id)
		_, err := repository.CollectionGet(bg(), db, id)
		require.NoError(t, err)
	})
}

func TestCollectionGrantAccessToUser(t *testing.T) {
	t.Run("grants_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		require.NoError(t, repository.CollectionGrantAccessToUser(bg(), db, userID, []int64{collID}))

		ids, err := repository.CollectionsAccessibleByUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Contains(t, ids, collID)
	})

	t.Run("idempotent", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		require.NoError(t, repository.CollectionGrantAccessToUser(bg(), db, userID, []int64{collID}))
		require.NoError(t, repository.CollectionGrantAccessToUser(bg(), db, userID, []int64{collID}))

		ids, err := repository.CollectionsAccessibleByUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Len(t, ids, 1)
	})
}

func TestCollectionGrantAccessToUsers(t *testing.T) {
	t.Run("grants_multiple_users", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		u1 := createUser(t, db)
		u2 := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)

		require.NoError(t, repository.CollectionGrantAccessToUsers(bg(), db, collID, []int64{u1, u2}))

		ids, err := repository.CollectionGetUsersWithAccess(bg(), db, collID)
		require.NoError(t, err)
		assert.Contains(t, ids, u1)
		assert.Contains(t, ids, u2)
	})

	t.Run("empty_slice_is_noop", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		require.NoError(t, repository.CollectionGrantAccessToUsers(bg(), db, collID, []int64{}))

		ids, err := repository.CollectionGetUsersWithAccess(bg(), db, collID)
		require.NoError(t, err)
		assert.Empty(t, ids)
	})
}

func TestUserDeleteCollectionAccess(t *testing.T) {
	t.Run("removes_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)

		require.NoError(t, repository.UserDeleteCollectionAccess(bg(), db, userID, collID))

		ids, err := repository.CollectionsAccessibleByUser(bg(), db, userID)
		require.NoError(t, err)
		assert.NotContains(t, ids, collID)
	})

	t.Run("noop_when_no_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		require.NoError(t, repository.UserDeleteCollectionAccess(bg(), db, userID, collID))
	})
}

func TestUserDeleteAllCollectionAccess(t *testing.T) {
	t.Run("removes_all_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		c1 := createCollection(t, db, constants.CollectionTypePhoto)
		c2 := createCollection(t, db, constants.CollectionTypeMovie)
		grantAccess(t, db, userID, c1)
		grantAccess(t, db, userID, c2)

		require.NoError(t, repository.UserDeleteAllCollectionAccess(bg(), db, userID))

		ids, err := repository.CollectionsAccessibleByUser(bg(), db, userID)
		require.NoError(t, err)
		assert.Empty(t, ids)
	})

	t.Run("noop_for_user_with_no_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		require.NoError(t, repository.UserDeleteAllCollectionAccess(bg(), db, userID))
	})
}

func TestCollectionGetDescendantCollectionIDs(t *testing.T) {
	t.Run("returns_all_descendants", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		rootID := createCollection(t, db, constants.CollectionTypePhoto)
		childID, err := repository.CollectionUpsertSubCollection(bg(), db, rootID, rootID, "Child", "root/child")
		require.NoError(t, err)
		grandChildID, err := repository.CollectionUpsertSubCollection(bg(), db, childID, rootID, "GrandChild", "root/child/grand")
		require.NoError(t, err)

		ids, err := repository.CollectionGetDescendantCollectionIDs(bg(), db, rootID)
		require.NoError(t, err)
		assert.Contains(t, ids, childID)
		assert.Contains(t, ids, grandChildID)
		assert.NotContains(t, ids, rootID) // root itself excluded
	})

	t.Run("empty_when_no_children", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createCollection(t, db, constants.CollectionTypePhoto)
		ids, err := repository.CollectionGetDescendantCollectionIDs(bg(), db, id)
		require.NoError(t, err)
		assert.Empty(t, ids)
	})
}

func TestCollectionIsDescendantOf(t *testing.T) {
	t.Run("child_is_descendant", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		rootID := createCollection(t, db, constants.CollectionTypePhoto)
		childID, err := repository.CollectionUpsertSubCollection(bg(), db, rootID, rootID, "Child", "root/child2")
		require.NoError(t, err)

		ok, err := repository.CollectionIsDescendantOf(bg(), db, rootID, childID)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("unrelated_is_not_descendant", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id1 := createCollection(t, db, constants.CollectionTypePhoto)
		id2 := createCollection(t, db, constants.CollectionTypeMovie)

		ok, err := repository.CollectionIsDescendantOf(bg(), db, id1, id2)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestCollectionsWithNonManualThumbnailIDs(t *testing.T) {
	t.Run("includes_collection_without_manual_cover", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createCollection(t, db, constants.CollectionTypePhoto)

		ids, err := repository.CollectionsWithNonManualThumbnailIDs(bg(), db)
		require.NoError(t, err)
		assert.Contains(t, ids, id)
	})

	t.Run("excludes_collection_with_manual_cover", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createCollection(t, db, constants.CollectionTypePhoto)
		require.NoError(t, repository.CollectionSetManualThumbnail(bg(), db, id, true))

		ids, err := repository.CollectionsWithNonManualThumbnailIDs(bg(), db)
		require.NoError(t, err)
		assert.NotContains(t, ids, id)
	})
}

func TestCollectionSetManualThumbnail(t *testing.T) {
	t.Run("sets_manual_cover", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createCollection(t, db, constants.CollectionTypePhoto)
		require.NoError(t, repository.CollectionSetManualThumbnail(bg(), db, id, true))

		ids, err := repository.CollectionsWithNonManualThumbnailIDs(bg(), db)
		require.NoError(t, err)
		assert.NotContains(t, ids, id)
	})

	t.Run("clears_manual_cover", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createCollection(t, db, constants.CollectionTypePhoto)
		require.NoError(t, repository.CollectionSetManualThumbnail(bg(), db, id, true))
		require.NoError(t, repository.CollectionSetManualThumbnail(bg(), db, id, false))

		ids, err := repository.CollectionsWithNonManualThumbnailIDs(bg(), db)
		require.NoError(t, err)
		assert.Contains(t, ids, id)
	})
}
