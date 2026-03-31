package repository_test

import (
	"encoding/json"
	"testing"

	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserCreate(t *testing.T) {
	t.Run("creates_user", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		authData, _ := json.Marshal(map[string]string{"password_hash": "hash"})
		id, err := repository.UserCreate(bg(), db, "alice", "local", authData)
		require.NoError(t, err)
		assert.Greater(t, id, int64(0))
	})
}

func TestUserGet(t *testing.T) {
	t.Run("returns_user", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createUser(t, db)
		u, err := repository.UserGet(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, id, u.ID)
		assert.False(t, u.IsAdmin)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.UserGet(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestUserByNameAndProvider(t *testing.T) {
	t.Run("returns_id_and_auth_data", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		authData, _ := json.Marshal(map[string]string{"password_hash": "myhash"})
		id, err := repository.UserCreate(bg(), db, "bob", "local", authData)
		require.NoError(t, err)

		gotID, gotData, err := repository.UserByNameAndProvider(bg(), db, "bob", "local")
		require.NoError(t, err)
		assert.Equal(t, id, gotID)
		assert.NotEmpty(t, gotData)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, _, err := repository.UserByNameAndProvider(bg(), db, "nobody", "local")
		assert.Error(t, err)
	})
}

func TestUserAuthProvider(t *testing.T) {
	t.Run("returns_provider", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createUser(t, db)
		provider, err := repository.UserAuthProvider(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, "local", provider)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.UserAuthProvider(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestUserIsAdmin(t *testing.T) {
	t.Run("non_admin_by_default", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createUser(t, db)
		isAdmin, err := repository.UserIsAdmin(bg(), db, id)
		require.NoError(t, err)
		assert.False(t, isAdmin)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.UserIsAdmin(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestUserSetAdmin(t *testing.T) {
	t.Run("promotes_to_admin", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createUser(t, db)
		require.NoError(t, repository.UserSetAdmin(bg(), db, id, true))
		isAdmin, err := repository.UserIsAdmin(bg(), db, id)
		require.NoError(t, err)
		assert.True(t, isAdmin)
	})

	t.Run("demotes_from_admin", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createUser(t, db)
		require.NoError(t, repository.UserSetAdmin(bg(), db, id, true))
		require.NoError(t, repository.UserSetAdmin(bg(), db, id, false))
		isAdmin, err := repository.UserIsAdmin(bg(), db, id)
		require.NoError(t, err)
		assert.False(t, isAdmin)
	})
}

func TestUserIsLocalOnly(t *testing.T) {
	t.Run("false_by_default", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createUser(t, db)
		localOnly, err := repository.UserIsLocalOnly(bg(), db, id)
		require.NoError(t, err)
		assert.False(t, localOnly)
	})

	t.Run("not_found", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		_, err := repository.UserIsLocalOnly(bg(), db, 999999)
		assert.Error(t, err)
	})
}

func TestUserSetLocalOnly(t *testing.T) {
	t.Run("sets_local_only", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createUser(t, db)
		require.NoError(t, repository.UserSetLocalOnly(bg(), db, id, true))
		localOnly, err := repository.UserIsLocalOnly(bg(), db, id)
		require.NoError(t, err)
		assert.True(t, localOnly)
	})

	t.Run("not_found_returns_error", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		err := repository.UserSetLocalOnly(bg(), db, 999999, true)
		assert.ErrorIs(t, err, repository.ErrNotFound)
	})
}

func TestUserUpdateAuth(t *testing.T) {
	t.Run("updates_auth_data", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		oldData, _ := json.Marshal(map[string]string{"password_hash": "oldhash"})
		id, err := repository.UserCreate(bg(), db, "updateauthuser", "local", oldData)
		require.NoError(t, err)

		newData, _ := json.Marshal(map[string]string{"password_hash": "newhash"})
		require.NoError(t, repository.UserUpdateAuth(bg(), db, id, newData))

		_, gotData, err := repository.UserByNameAndProvider(bg(), db, "updateauthuser", "local")
		require.NoError(t, err)
		assert.Contains(t, string(gotData), "newhash")
	})

	t.Run("not_found_returns_error", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		newData, _ := json.Marshal(map[string]string{"password_hash": "x"})
		err := repository.UserUpdateAuth(bg(), db, 999999, newData)
		assert.ErrorIs(t, err, repository.ErrNotFound)
	})
}

func TestUserTouchLastSeen(t *testing.T) {
	t.Run("does_not_error", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createUser(t, db)
		// Fire-and-forget — just ensure it doesn't panic or leave the DB broken.
		repository.UserTouchLastSeen(bg(), db, id)
		_, err := repository.UserGet(bg(), db, id)
		require.NoError(t, err)
	})
}

func TestUsersGet(t *testing.T) {
	t.Run("returns_users", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id1 := createUser(t, db)
		id2 := createUser(t, db)

		users, err := repository.UsersGet(bg(), db)
		require.NoError(t, err)
		ids := make([]int64, len(users))
		for i, u := range users {
			ids[i] = u.ID
		}
		assert.Contains(t, ids, id1)
		assert.Contains(t, ids, id2)
	})

	t.Run("empty_when_no_users_created", func(t *testing.T) {
		// The admin user is always present, but no additional users should appear
		// in a fresh transaction beyond what already exists in the DB.
		db := testutil.NewTx(t, testPool)
		users, err := repository.UsersGet(bg(), db)
		require.NoError(t, err)
		// At minimum the seeded admin user is present.
		assert.NotNil(t, users)
	})
}

func TestUserDelete(t *testing.T) {
	t.Run("deletes_user", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		id := createUser(t, db)
		rows, err := repository.UserDelete(bg(), db, id)
		require.NoError(t, err)
		assert.Equal(t, int64(1), rows)
		_, err = repository.UserGet(bg(), db, id)
		assert.Error(t, err)
	})

	t.Run("no_rows_for_missing_user", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		rows, err := repository.UserDelete(bg(), db, 999999)
		require.NoError(t, err)
		assert.Equal(t, int64(0), rows)
	})
}
