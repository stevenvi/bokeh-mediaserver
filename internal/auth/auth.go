package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

// Plugin is the interface all auth providers must implement.
// Adding a new auth method means implementing this interface.
type Plugin interface {
	// Name returns the unique identifier for this provider (e.g. "local").
	Name() string
	// Authenticate validates credentials and returns the user ID on success.
	Authenticate(ctx context.Context, db *pgxpool.Pool, credentials json.RawMessage) (int, error)
}

// Claims are the JWT payload fields. Intentionally minimal.
type Claims struct {
	jwt.RegisteredClaims
	IsAdmin bool `json:"adm"`
}

// IssueToken generates a signed JWT for the given user.
func IssueToken(userID int, isAdmin bool, secret string) (string, error) {
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   fmt.Sprintf("%d", userID),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(30 * time.Minute)),
		},
		IsAdmin: isAdmin,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// ParseToken validates a JWT and returns its claims.
func ParseToken(tokenStr, secret string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(secret), nil
	})
	if err != nil {
		return nil, err
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token")
	}
	return claims, nil
}

// ─── Local auth plugin ────────────────────────────────────────────────────────

// LocalPlugin authenticates users with a username and bcrypt-hashed password
// stored in users.auth_data as {"password_hash": "..."}.
type LocalPlugin struct{}

func (LocalPlugin) Name() string { return "local" }

type localCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type localAuthData struct {
	PasswordHash string `json:"password_hash"`
}

func (LocalPlugin) Authenticate(ctx context.Context, db *pgxpool.Pool, raw json.RawMessage) (int, error) {
	var creds localCredentials
	if err := json.Unmarshal(raw, &creds); err != nil {
		return 0, fmt.Errorf("invalid credentials format: %w", err)
	}

	var (
		userID   int
		authData json.RawMessage
	)
	err := db.QueryRow(ctx,
		`SELECT id, auth_data FROM users WHERE name = $1 AND auth_provider = 'local'`,
		creds.Username,
	).Scan(&userID, &authData)
	if err != nil {
		return 0, errors.New("invalid username or password")
	}

	var data localAuthData
	if err := json.Unmarshal(authData, &data); err != nil {
		return 0, errors.New("invalid username or password")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(data.PasswordHash), []byte(creds.Password)); err != nil {
		return 0, errors.New("invalid username or password")
	}

	return userID, nil
}
