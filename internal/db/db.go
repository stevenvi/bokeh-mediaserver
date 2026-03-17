package db

import (
	"context"
	"embed"
	"fmt"
	"net/url"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Open creates a pgxpool connection and verifies connectivity.
func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("db ping: %w", err)
	}
	return pool, nil
}

func bootstapDatabase(databaseURL string) error {
	// Parse the database URL to extract the database name
	u, err := url.Parse(databaseURL)
	if err != nil {
		return fmt.Errorf("parse database URL: %w", err)
	}
	dbname := strings.TrimPrefix(u.Path, "/")
	if dbname == "" {
		return fmt.Errorf("database name not found in URL path")
	}

	// Create a connection URL for the 'postgres' admin database
	adminURL := *u
	adminURL.Path = "/postgres"

	// Connect to the admin database to create the target database if needed
	adminPool, err := pgxpool.New(context.Background(), adminURL.String())
	if err != nil {
		return fmt.Errorf("connect to admin database: %w", err)
	}
	defer adminPool.Close()

	// Create the database, ignoring "already exists" errors
	_, err = adminPool.Exec(context.Background(), "CREATE DATABASE "+dbname)
	if err != nil {
		// already exists error can be skipped
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("create database %q: %w", dbname, err)
		}
	}
	return nil
}

// Migrate runs all pending up migrations embedded in the binary.
func Migrate(databaseURL string) error {
	// Ensure the target database exists before running migrations
	if err := bootstapDatabase(databaseURL); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}

	// Now run the migrations on the target database
	src, err := iofs.New(migrationsFS, "migrations") // matches internal/db/migrations/
	if err != nil {
		return fmt.Errorf("iofs.New: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, databaseURL)
	if err != nil {
		return fmt.Errorf("migrate.New: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migrate.Up: %w", err)
	}
	return nil
}
