package utils

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
)

var TestPool *pgxpool.Pool

func TestMain(m *testing.M) {
	var cleanup func()
	TestPool, cleanup = testutil.Setup()
	code := m.Run()
	cleanup()
	os.Exit(code)
}