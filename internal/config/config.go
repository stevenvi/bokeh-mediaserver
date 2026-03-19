package config

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"strconv"
)

type Config struct {
	DatabaseURL           string // Database connection URL (e.g. postgres://user:pass@host:port/dbname)
	DataPath              string // Base path for storing generated media variants
	MediaPath             string // Base path for the media library
	Port                  string // Port to listen on
	JWTSecret             string // Secret for signing JWTs
	WorkerCount           int    // Number of worker goroutines to use
	ProcessingWorkerCount int    // Number of worker goroutines for media processing (EXIF, variants)
	LogLevel              string // Log level (debug, info, warn, error)
	LogPath               string // Path to output log files to (empty for stdout)
}

func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL: env("DATABASE_URL", ""),
		DataPath:    env("DATA_PATH", "./data"),
		MediaPath:   env("MEDIA_PATH", "/media"),
		Port:        env("PORT", "3000"),
		JWTSecret:   env("JWT_SECRET", ""),
		LogLevel:    env("LOG_LEVEL", "warn"),
		LogPath:     env("LOG_PATH", ""),
	}

	workerCount, err := strconv.Atoi(env("WORKER_COUNT", "2"))
	if err != nil {
		return nil, fmt.Errorf("invalid WORKER_COUNT: %w", err)
	}
	cfg.WorkerCount = workerCount

	processingWorkerCount, err := strconv.Atoi(env("PROCESSING_WORKER_COUNT", "2"))
	if err != nil {
		return nil, fmt.Errorf("invalid PROCESSING_WORKER_COUNT: %w", err)
	}
	cfg.ProcessingWorkerCount = processingWorkerCount

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.JWTSecret == "" {
		// If no JWT secret is provided, generate a random 32-byte key.
		// This is intended for quick development use only; the key will
		// change on every restart, causing all existing tokens to become invalid.
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			return nil, fmt.Errorf("generating JWT secret: %w", err)
		}
		cfg.JWTSecret = base64.RawURLEncoding.EncodeToString(b)
		slog.Warn("JWT_SECRET is not set; generated a random key (insecure for production). All users will be logged out on restart.")
	}

	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
