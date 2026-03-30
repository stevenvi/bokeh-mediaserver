package repository

import (
	"context"
	"encoding/json"

	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// UserCreate inserts a new user and returns the ID.
func UserCreate(ctx context.Context, db utils.DBTX, name, authProvider string, authData json.RawMessage) (int64, error) {
	var id int64
	err := db.QueryRow(ctx,
		`INSERT INTO users (name, auth_provider, auth_data) VALUES ($1, $2, $3) RETURNING id`,
		name, authProvider, authData,
	).Scan(&id)
	return id, err
}

// UserGet returns a user's public fields.
func UserGet(ctx context.Context, db utils.DBTX, id int64) (*models.User, error) {
	var u models.User
	err := db.QueryRow(ctx,
		`SELECT id, name, is_admin FROM users WHERE id = $1`, id,
	).Scan(&u.ID, &u.Name, &u.IsAdmin)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// UserByNameAndProvider returns user ID and auth_data for credential verification.
func UserByNameAndProvider(ctx context.Context, db utils.DBTX, name, provider string) (int64, json.RawMessage, error) {
	var id int64
	var authData json.RawMessage
	err := db.QueryRow(ctx,
		`SELECT id, auth_data FROM users WHERE name = $1 AND auth_provider = $2`,
		name, provider,
	).Scan(&id, &authData)
	return id, authData, err
}

// UserAuthProvider returns the auth provider name for a user.
func UserAuthProvider(ctx context.Context, db utils.DBTX, userID int64) (string, error) {
	var provider string
	err := db.QueryRow(ctx, `SELECT auth_provider FROM users WHERE id = $1`, userID).Scan(&provider)
	return provider, err
}

// UserIsAdmin returns whether a user is an admin.
func UserIsAdmin(ctx context.Context, db utils.DBTX, userID int64) (bool, error) {
	var isAdmin bool
	err := db.QueryRow(ctx, `SELECT is_admin FROM users WHERE id = $1`, userID).Scan(&isAdmin)
	return isAdmin, err
}

// UserSetAdmin sets the is_admin flag for a user.
func UserSetAdmin(ctx context.Context, db utils.DBTX, userID int64, isAdmin bool) error {
	_, err := db.Exec(ctx,
		`UPDATE users SET is_admin = $2 WHERE id = $1`,
		userID, isAdmin,
	)
	return err
}

// UserIsLocalOnly returns whether a user is restricted to local network access only.
func UserIsLocalOnly(ctx context.Context, db utils.DBTX, userID int64) (bool, error) {
	var localAccessOnly bool
	err := db.QueryRow(ctx, `SELECT local_access_only FROM users WHERE id = $1`, userID).Scan(&localAccessOnly)
	return localAccessOnly, err
}

// UserSetLocalOnly updates a user's local network access restrictions.
func UserSetLocalOnly(ctx context.Context, db utils.DBTX, userID int64, localAccessOnly bool) error {
	tag, err := db.Exec(ctx, `UPDATE users SET local_access_only = $1 WHERE id = $2`, localAccessOnly, userID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// UserUpdateAuth updates the auth_data for a user.
func UserUpdateAuth(ctx context.Context, db utils.DBTX, userID int64, authData json.RawMessage) error {
	tag, err := db.Exec(ctx, `UPDATE users SET auth_data = $2 WHERE id = $1`, userID, authData)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// UserTouchLastSeen updates the last_seen_at timestamp. Errors are intentionally ignored by callers.
func UserTouchLastSeen(ctx context.Context, db utils.DBTX, userID int64) {
	_, _ = db.Exec(ctx, `UPDATE users SET last_seen_at = now() WHERE id = $1`, userID)
}

// UsersGet returns id and name for every user, ordered by id.
func UsersGet(ctx context.Context, db utils.DBTX) ([]models.User, error) {
	rows, err := db.Query(ctx, `SELECT id, name FROM users ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []models.User
	for rows.Next() {
		var u models.User
		if err := rows.Scan(&u.ID, &u.Name); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// UserDelete removes a user and returns the number of rows affected.
func UserDelete(ctx context.Context, db utils.DBTX, userID int64) (int64, error) {
	tag, err := db.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
