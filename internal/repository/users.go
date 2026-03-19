package repository

import (
	"context"
	"encoding/json"

	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type UserRepository struct {
	db utils.DBTX
}

func NewUserRepository(db utils.DBTX) *UserRepository {
	return &UserRepository{db: db}
}

// DB returns the underlying DBTX, useful for passing to auth plugins.
func (r *UserRepository) DB() utils.DBTX { return r.db }

// FindByID returns a user's public fields.
func (r *UserRepository) FindByID(ctx context.Context, id int64) (*models.User, error) {
	var u models.User
	err := r.db.QueryRow(ctx,
		`SELECT id, name, is_admin FROM users WHERE id = $1`, id,
	).Scan(&u.ID, &u.Name, &u.IsAdmin)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// FindByNameAndProvider returns user ID and auth_data for credential verification.
func (r *UserRepository) FindByNameAndProvider(ctx context.Context, name, provider string) (int64, json.RawMessage, error) {
	var id int64
	var authData json.RawMessage
	err := r.db.QueryRow(ctx,
		`SELECT id, auth_data FROM users WHERE name = $1 AND auth_provider = $2`,
		name, provider,
	).Scan(&id, &authData)
	return id, authData, err
}

// GetAuthProvider returns the auth provider name for a user.
func (r *UserRepository) GetAuthProvider(ctx context.Context, userID int64) (string, error) {
	var provider string
	err := r.db.QueryRow(ctx, `SELECT auth_provider FROM users WHERE id = $1`, userID).Scan(&provider)
	return provider, err
}

// GetAdminStatus returns whether a user is an admin.
func (r *UserRepository) GetAdminStatus(ctx context.Context, userID int64) (bool, error) {
	var isAdmin bool
	err := r.db.QueryRow(ctx, `SELECT is_admin FROM users WHERE id = $1`, userID).Scan(&isAdmin)
	return isAdmin, err
}

// Create inserts a new user and returns the ID.
func (r *UserRepository) Create(ctx context.Context, name, authProvider string, authData json.RawMessage) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`INSERT INTO users (name, auth_provider, auth_data) VALUES ($1, $2, $3) RETURNING id`,
		name, authProvider, authData,
	).Scan(&id)
	return id, err
}

// UpdateAuthData updates the auth_data for a user.
func (r *UserRepository) UpdateAuthData(ctx context.Context, userID int64, authData json.RawMessage) error {
	tag, err := r.db.Exec(ctx, `UPDATE users SET auth_data = $2 WHERE id = $1`, userID, authData)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetAdmin sets the is_admin flag for a user.
func (r *UserRepository) SetAdmin(ctx context.Context, userID int64, isAdmin bool) error {
	_, err := r.db.Exec(ctx,
		`UPDATE users SET is_admin = $2 WHERE id = $1`,
		userID, isAdmin,
	)
	return err
}

// TouchLastSeen updates the last_seen_at timestamp. Errors are intentionally ignored by callers.
func (r *UserRepository) TouchLastSeen(ctx context.Context, userID int64) {
	_, _ = r.db.Exec(ctx, `UPDATE users SET last_seen_at = now() WHERE id = $1`, userID)
}

// Delete removes a user and returns the number of rows affected.
func (r *UserRepository) Delete(ctx context.Context, userID int64) (int64, error) {
	tag, err := r.db.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
