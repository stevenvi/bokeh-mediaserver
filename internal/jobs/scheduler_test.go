package jobs_test

import (
	"context"
	"testing"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduler_TriggerScans(t *testing.T) {
	t.Run("creates_scan_jobs_for_enabled_collections", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		ctx := context.Background()

		// Create two top-level enabled collections
		c1 := testutil.InsertCollection(t, db, "Photos", constants.CollectionTypePhoto, "photos")
		c2 := testutil.InsertCollection(t, db, "Movies", constants.CollectionTypeMovie, "movies")

		// Create a disabled collection — should be skipped
		testutil.MustExec(t, db,
			`INSERT INTO collections (name, type, relative_path, is_enabled) VALUES ('Disabled', 'image:photo', 'disabled', false)`)

		// Create scheduler and trigger scans
		s := jobs.NewScheduler(db)
		s.TriggerScans(ctx)

		// Verify scan jobs were created for enabled collections
		active1, err := repository.JobIsActive(ctx, db, "library_scan", c1)
		require.NoError(t, err)
		assert.True(t, active1, "should have created scan job for collection 1")

		active2, err := repository.JobIsActive(ctx, db, "library_scan", c2)
		require.NoError(t, err)
		assert.True(t, active2, "should have created scan job for collection 2")
	})

	t.Run("skips_collections_with_active_scans", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		ctx := context.Background()

		c1 := testutil.InsertCollection(t, db, "Photos", constants.CollectionTypePhoto, "photos")

		// Create an already-active scan
		relatedType := "collection"
		_, err := repository.JobCreate(ctx, db, "library_scan", &c1, &relatedType)
		require.NoError(t, err)

		// Count jobs before trigger
		var countBefore int
		err = db.QueryRow(ctx,
			`SELECT COUNT(*) FROM jobs WHERE type = 'library_scan' AND related_id = $1`, c1,
		).Scan(&countBefore)
		require.NoError(t, err)

		s := jobs.NewScheduler(db)
		s.TriggerScans(ctx)

		// Count should not increase
		var countAfter int
		err = db.QueryRow(ctx,
			`SELECT COUNT(*) FROM jobs WHERE type = 'library_scan' AND related_id = $1`, c1,
		).Scan(&countAfter)
		require.NoError(t, err)
		assert.Equal(t, countBefore, countAfter, "should not create duplicate scan job")
	})

	t.Run("skips_child_collections", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		parent := testutil.InsertCollection(t, tx, "Photos", constants.CollectionTypePhoto, "photos")

		// Create child collection
		testutil.MustExec(t, tx,
			`INSERT INTO collections (parent_collection_id, name, type, relative_path) VALUES ($1, 'Sub', 'image:photo', 'photos/sub')`,
			parent)

		s := jobs.NewScheduler(tx)
		s.TriggerScans(ctx)

		// Only parent should have a scan job
		var count int
		err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE type = 'library_scan'`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "should only create scan for top-level collection")
	})
}

func TestScheduler_TriggerIntegrityCheck(t *testing.T) {
	t.Run("creates_integrity_job", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		s := jobs.NewScheduler(tx)
		s.TriggerIntegrityCheck(ctx)

		var count int
		err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM jobs WHERE type = 'integrity_check' AND status = 'queued'`,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("skips_if_already_active", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		ctx := context.Background()

		// Create an active integrity check
		_, err := repository.JobCreate(ctx, db, "integrity_check", nil, nil)
		require.NoError(t, err)

		s := jobs.NewScheduler(db)
		s.TriggerIntegrityCheck(ctx)

		var count int
		err = db.QueryRow(ctx,
			`SELECT COUNT(*) FROM jobs WHERE type = 'integrity_check'`,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "should not create duplicate integrity job")
	})
}

func TestScheduler_LoadSchedules(t *testing.T) {
	tests := []struct {
		name              string
		scanSchedule      *string
		integritySchedule *string
		wantScan          string
		wantIntegrity     string
	}{
		{
			"defaults_when_null",
			nil, nil,
			"0 3 * * *", "0 4 * * 0",
		},
		{
			"custom_schedules",
			testutil.StrPtr("*/5 * * * *"), testutil.StrPtr("0 2 * * 1"),
			"*/5 * * * *", "0 2 * * 1",
		},
		{
			"empty_string_uses_default",
			testutil.StrPtr(""), testutil.StrPtr(""),
			"0 3 * * *", "0 4 * * 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := testutil.NewTx(t, testPool)
			ctx := context.Background()

			// Update server_config with test values
			testutil.MustExec(t, tx,
				`UPDATE server_config SET scan_schedule = $1, integrity_schedule = $2 WHERE id = 1`,
				tt.scanSchedule, tt.integritySchedule)

			s := jobs.NewScheduler(tx)
			cfg := s.LoadSchedules(ctx)

			assert.Equal(t, tt.wantScan, cfg["scan_schedule"])
			assert.Equal(t, tt.wantIntegrity, cfg["integrity_schedule"])
		})
	}
}
