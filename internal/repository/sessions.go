package repository

import (
	"context"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type SessionRepository struct {
	db utils.DBTX
}

func NewSessionRepository(db utils.DBTX) *SessionRepository {
	return &SessionRepository{db: db}
}

// Create inserts a new session and returns its ID.
func (r *SessionRepository) Create(ctx context.Context, userID int64, tokenHash string, expiresAt time.Time, deviceName *string, ipAddress string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`INSERT INTO user_sessions (user_id, token_hash, expires_at, device_name, ip_address)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		userID, tokenHash, expiresAt, deviceName, ipAddress,
	).Scan(&id)
	return id, err
}

// FindByTokenHash looks up a session by its current token hash,
// joining on users to return admin status.
func (r *SessionRepository) FindByTokenHash(ctx context.Context, hash string) (sessionID, userID int64, isAdmin bool, expiresAt time.Time, err error) {
	err = r.db.QueryRow(ctx,
		`SELECT s.id, s.user_id, u.is_admin, s.expires_at
		 FROM user_sessions s
		 JOIN users u ON u.id = s.user_id
		 WHERE s.token_hash = $1`,
		hash,
	).Scan(&sessionID, &userID, &isAdmin, &expiresAt)
	return
}

// FindByPreviousTokenHash checks if a token hash matches a previously-rotated token,
// which indicates token theft. Returns the user ID and whether a match was found.
func (r *SessionRepository) FindByPreviousTokenHash(ctx context.Context, hash string) (userID int64, found bool, err error) {
	err = r.db.QueryRow(ctx,
		`SELECT user_id FROM user_sessions WHERE previous_token_hash = $1`, hash,
	).Scan(&userID)
	if err != nil {
		return 0, false, err
	}
	return userID, true, nil
}

// RotateToken updates a session with a new token hash, recording the old hash for theft detection.
func (r *SessionRepository) RotateToken(ctx context.Context, sessionID int64, newHash, oldHash string, expiresAt time.Time, ipAddress string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE user_sessions
		 SET token_hash = $2, previous_token_hash = $3,
		     expires_at = $4, ip_address = $5, last_used_at = now()
		 WHERE id = $1`,
		sessionID, newHash, oldHash, expiresAt, ipAddress,
	)
	return err
}

// Delete removes a session scoped to a specific user. Returns rows affected.
func (r *SessionRepository) Delete(ctx context.Context, sessionID, userID int64) (int64, error) {
	tag, err := r.db.Exec(ctx,
		`DELETE FROM user_sessions WHERE id = $1 AND user_id = $2`,
		sessionID, userID,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// DeleteByID removes a session by ID without user scoping (admin use).
func (r *SessionRepository) DeleteByID(ctx context.Context, sessionID int64) error {
	_, err := r.db.Exec(ctx, `DELETE FROM user_sessions WHERE id = $1`, sessionID)
	return err
}

// DeleteAllForUser removes all sessions for a user.
func (r *SessionRepository) DeleteAllForUser(ctx context.Context, userID int64) error {
	_, err := r.db.Exec(ctx, `DELETE FROM user_sessions WHERE user_id = $1`, userID)
	return err
}

// ListForUser returns all sessions for a user, ordered by most recently used.
func (r *SessionRepository) ListForUser(ctx context.Context, userID int64) ([]models.SessionView, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, device_name, ip_address, last_used_at, created_at, expires_at
		 FROM user_sessions
		 WHERE user_id = $1
		 ORDER BY last_used_at DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []models.SessionView
	for rows.Next() {
		var s models.SessionView
		if err := rows.Scan(&s.ID, &s.DeviceName, &s.IPAddress, &s.LastUsedAt, &s.CreatedAt, &s.ExpiresAt); err != nil {
			continue
		}
		sessions = append(sessions, s)
	}
	return sessions, nil
}
