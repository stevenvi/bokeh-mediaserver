package testutil

import (
	"context"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/db"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
	"github.com/stretchr/testify/require"
)

const testDBURL = "postgres://test:test@localhost:5433/bokeh_test?sslmode=disable"

// Setup starts the test database container (if not running), runs migrations,
// and returns a connection pool. Call the returned cleanup function in defer.
//
// Usage in TestMain:
//
//	var testPool *pgxpool.Pool
//
//	func TestMain(m *testing.M) {
//	    var cleanup func()
//	    testPool, cleanup = testutil.Setup()
//	    code := m.Run()
//	    cleanup()
//	    os.Exit(code)
//	}
func Setup() (*pgxpool.Pool, func()) {
	if os.Getenv("SKIP_DB_TESTS") != "" {
		return nil, func() {}
	}

	root := findRepoRoot()
	composePath := filepath.Join(root, "docker-compose.test.yml")

	// Start postgres-test if not already running
	if !isTestDBRunning(composePath) {
		log.Println("testutil: starting test database container...")
		cmd := exec.Command("docker", "compose", "-f", composePath, "up", "-d", "--wait")
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatalf("testutil: failed to start test database: %v", err)
		}
	}

	// Wait for connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var pool *pgxpool.Pool
	var err error
	for {
		pool, err = pgxpool.New(ctx, testDBURL)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				break
			}
			pool.Close()
		}
		if ctx.Err() != nil {
			log.Fatalf("testutil: timed out waiting for test database: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	pool.Close()

	// Run migrations (idempotent)
	if err := db.Migrate(testDBURL); err != nil {
		log.Fatalf("testutil: migrations failed: %v", err)
	}

	// Create pool for tests
	pool, err = pgxpool.New(context.Background(), testDBURL)
	if err != nil {
		log.Fatalf("testutil: pool creation failed: %v", err)
	}

	return pool, func() { pool.Close() }
}

// NewTx begins a transaction and registers a cleanup that rolls it back
// when the test ends. Returns the transaction as a utils.DBTX.
// Skips the test if pool is nil (database not available).
func NewTx(t *testing.T, pool *pgxpool.Pool) utils.DBTX {
	t.Helper()
	if pool == nil {
		t.Skip("test database not available (set SKIP_DB_TESTS to skip explicitly)")
	}
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = tx.Rollback(ctx)
	})
	return tx
}

func isTestDBRunning(composePath string) bool {
	cmd := exec.Command("docker", "compose", "-f", composePath, "ps", "--status=running", "-q")
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	return len(out) > 0
}

func findRepoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("testutil: cannot get working directory")
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			log.Fatal("testutil: could not find repo root (go.mod)")
		}
		dir = parent
	}
}

// InsertCollection is a test helper that creates a collection and returns its ID.
func InsertCollection(t *testing.T, db utils.DBTX, name, colType, relativePath string) int64 {
	t.Helper()
	repo := repository.NewCollectionRepository(db)
	id, err := repo.Create(context.Background(), name, colType, relativePath)
	require.NoError(t, err)
	return id
}

// InsertMediaItem is a test helper that creates a media_item and returns its ID.
func InsertMediaItem(t *testing.T, db utils.DBTX, collectionID int64, title, relativePath, mimeType string) int64 {
	t.Helper()
	repo := repository.NewMediaItemRepository(db)
	id, _, err := repo.Upsert(context.Background(), collectionID, title, relativePath, 1024, "abc123", mimeType)
	require.NoError(t, err)
	return id
}

// MustExec executes a SQL statement in a test, failing the test on error.
// Use for test-specific setup that doesn't warrant a repository method.
func MustExec(t *testing.T, db utils.DBTX, sql string, args ...any) {
	t.Helper()
	_, err := db.Exec(context.Background(), sql, args...)
	require.NoError(t, err)
}

// IntPtr returns a pointer to the given int.
func IntPtr(v int) *int { return &v }

// Int64Ptr returns a pointer to the given int64.
func Int64Ptr(v int64) *int64 { return &v }

// StrPtr returns a pointer to the given string.
func StrPtr(v string) *string { return &v }
