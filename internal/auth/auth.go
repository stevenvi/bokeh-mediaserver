package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
	"golang.org/x/crypto/bcrypt"
)

const (
	AccessTokenTTL  = 15 * time.Minute
	RefreshTokenTTL = 90 * 24 * time.Hour // 90 days
)

// Plugin is the interface all auth providers must implement.
type Plugin interface {
	// Name returns the unique identifier for this provider (e.g. "local").
	Name() string
	// Authenticate validates credentials and returns the user ID on success.
	Authenticate(ctx context.Context, db utils.DBTX, credentials json.RawMessage) (int64, error)
	// CreateUser creates a new user with the given name and provider-specific credentials.
	// The user row is inserted by the plugin. Returns the new user ID.
	CreateUser(ctx context.Context, db utils.DBTX, name string, credentials json.RawMessage) (int64, error)
	// UpdateCredentials updates the credentials for an existing user.
	// Invalidates any active refresh tokens.
	UpdateCredentials(ctx context.Context, db utils.DBTX, userID int64, credentials json.RawMessage) error
}

// DefaultPlugins returns the default set of registered auth plugins.
func DefaultPlugins() map[string]Plugin {
	return map[string]Plugin{
		"local": LocalPlugin{},
	}
}

// Claims are the JWT payload fields. Intentionally minimal.
type Claims struct {
	jwt.RegisteredClaims
	IsAdmin  bool  `json:"adm"`
	DeviceID int64 `json:"did"`
}

// IssueToken generates a signed JWT for the given user and device.
func IssueToken(userID, deviceID int64, isAdmin bool, secret string) (string, error) {
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   fmt.Sprintf("%d", userID),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(AccessTokenTTL)),
		},
		IsAdmin:  isAdmin,
		DeviceID: deviceID,
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

// GenerateRefreshToken creates a cryptographically random refresh token.
// Returns the raw token (to send to the client) and its SHA-256 hex hash (to store in DB).
func GenerateRefreshToken() (raw, hash string, err error) {
	b := make([]byte, 32)
	if _, err = rand.Read(b); err != nil {
		return "", "", fmt.Errorf("generate refresh token: %w", err)
	}
	raw = base64.RawURLEncoding.EncodeToString(b)
	hash = HashRefreshToken(raw)
	return raw, hash, nil
}

// HashRefreshToken returns the SHA-256 hex hash of a raw refresh token.
func HashRefreshToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
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

func (LocalPlugin) Authenticate(ctx context.Context, db utils.DBTX, raw json.RawMessage) (int64, error) {
	var creds localCredentials
	if err := json.Unmarshal(raw, &creds); err != nil {
		return 0, fmt.Errorf("invalid credentials format: %w", err)
	}

	userID, authData, err := repository.UserByNameAndProvider(ctx, db, creds.Username, "local")
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

func (LocalPlugin) CreateUser(ctx context.Context, db utils.DBTX, name string, raw json.RawMessage) (int64, error) {
	var creds struct {
		Password string `json:"password"`
	}
	if err := json.Unmarshal(raw, &creds); err != nil || creds.Password == "" {
		return 0, errors.New("password is required")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(creds.Password), bcrypt.DefaultCost)
	if err != nil {
		return 0, fmt.Errorf("hash password: %w", err)
	}

	authData, _ := json.Marshal(localAuthData{PasswordHash: string(hash)})

	return repository.UserCreate(ctx, db, name, "local", authData)
}

func (LocalPlugin) UpdateCredentials(ctx context.Context, db utils.DBTX, userID int64, raw json.RawMessage) error {
	var creds struct {
		Password string `json:"password"`
	}
	if err := json.Unmarshal(raw, &creds); err != nil || creds.Password == "" {
		return errors.New("password is required")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(creds.Password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}

	authData, _ := json.Marshal(localAuthData{PasswordHash: string(hash)})

	if err := repository.UserUpdateAuth(ctx, db, userID, authData); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return errors.New("user not found or uses a different auth provider")
		}
		return fmt.Errorf("update credentials: %w", err)
	}

	_, err = repository.DevicesDeleteForUser(ctx, db, userID)
	return err
}
