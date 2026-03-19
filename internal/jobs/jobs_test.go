package jobs_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	var cleanup func()
	testPool, cleanup = testutil.Setup()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func TestCreate(t *testing.T) {
	tests := []struct {
		name        string
		jobType     string
		relatedID   *int64
		relatedType *string
	}{
		{"library_scan", "library_scan", testutil.Int64Ptr(1), testutil.StrPtr("collection")},
		{"process_media", "process_media", testutil.Int64Ptr(42), testutil.StrPtr("media_item")},
		{"orphan_cleanup_no_related", "orphan_cleanup", nil, nil},
		{"integrity_check", "integrity_check", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := testutil.NewTx(t, testPool)
			ctx := context.Background()

			id, err := jobs.Create(ctx, tx, tt.jobType, tt.relatedID, tt.relatedType)
			require.NoError(t, err)
			assert.Greater(t, id, int64(0))

			job, err := jobs.GetByID(ctx, tx, id)
			require.NoError(t, err)
			assert.Equal(t, tt.jobType, job.Type)
			assert.Equal(t, "queued", job.Status)
			assert.Equal(t, tt.relatedID, job.RelatedID)
			assert.Equal(t, tt.relatedType, job.RelatedType)
			assert.Nil(t, job.StartedAt)
			assert.Nil(t, job.CompletedAt)
		})
	}
}

func TestClaimNextJob(t *testing.T) {
	t.Run("claims_queued_job", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		id, err := jobs.Create(ctx, tx, "library_scan", testutil.Int64Ptr(1), testutil.StrPtr("collection"))
		require.NoError(t, err)

		job, err := jobs.ClaimNextJob(ctx, tx, []string{"library_scan"})
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, id, job.ID)
		assert.Equal(t, "running", job.Status)
		assert.NotNil(t, job.StartedAt)
	})

	t.Run("returns_nil_when_empty", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		job, err := jobs.ClaimNextJob(ctx, tx, []string{"library_scan"})
		require.NoError(t, err)
		assert.Nil(t, job)
	})

	t.Run("skips_running_jobs", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		id, err := jobs.Create(ctx, tx, "library_scan", testutil.Int64Ptr(1), testutil.StrPtr("collection"))
		require.NoError(t, err)
		require.NoError(t, jobs.MarkRunning(ctx, tx, id))

		job, err := jobs.ClaimNextJob(ctx, tx, []string{"library_scan"})
		require.NoError(t, err)
		assert.Nil(t, job, "should not claim already-running job")
	})

	t.Run("filters_by_type", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		_, err := jobs.Create(ctx, tx, "library_scan", testutil.Int64Ptr(1), testutil.StrPtr("collection"))
		require.NoError(t, err)

		job, err := jobs.ClaimNextJob(ctx, tx, []string{"process_media"})
		require.NoError(t, err)
		assert.Nil(t, job, "should not claim job of wrong type")
	})

	t.Run("claims_oldest_first", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		id1, err := jobs.Create(ctx, tx, "library_scan", testutil.Int64Ptr(1), testutil.StrPtr("collection"))
		require.NoError(t, err)
		_, err = jobs.Create(ctx, tx, "library_scan", testutil.Int64Ptr(2), testutil.StrPtr("collection"))
		require.NoError(t, err)

		job, err := jobs.ClaimNextJob(ctx, tx, []string{"library_scan"})
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, id1, job.ID, "should claim oldest queued job first")
	})
}

func TestUpdateProgress(t *testing.T) {
	tx := testutil.NewTx(t, testPool)
	ctx := context.Background()

	id, err := jobs.Create(ctx, tx, "library_scan", nil, nil)
	require.NoError(t, err)

	require.NoError(t, jobs.UpdateProgress(ctx, tx, id, "step 1"))
	require.NoError(t, jobs.UpdateProgress(ctx, tx, id, "step 2"))

	job, err := jobs.GetByID(ctx, tx, id)
	require.NoError(t, err)
	require.NotNil(t, job.Log)
	assert.Equal(t, "step 1\nstep 2\n", *job.Log)
}

func TestMarkStateTransitions(t *testing.T) {
	tests := []struct {
		name           string
		action         string
		expectedStatus string
		hasCompletedAt bool
	}{
		{"mark_running", "running", "running", false},
		{"mark_done", "done", "done", true},
		{"mark_failed", "failed", "failed", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := testutil.NewTx(t, testPool)
			ctx := context.Background()

			id, err := jobs.Create(ctx, tx, "library_scan", nil, nil)
			require.NoError(t, err)

			switch tt.action {
			case "running":
				require.NoError(t, jobs.MarkRunning(ctx, tx, id))
			case "done":
				require.NoError(t, jobs.MarkDone(ctx, tx, id))
			case "failed":
				require.NoError(t, jobs.MarkFailed(ctx, tx, id, "something broke"))
			}

			job, err := jobs.GetByID(ctx, tx, id)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedStatus, job.Status)

			if tt.hasCompletedAt {
				assert.NotNil(t, job.CompletedAt)
			}
			if tt.action == "failed" {
				require.NotNil(t, job.ErrorMessage)
				assert.Equal(t, "something broke", *job.ErrorMessage)
			}
		})
	}
}

func TestIsActive(t *testing.T) {
	tests := []struct {
		name     string
		status   string
		expected bool
	}{
		{"queued_is_active", "queued", true},
		{"running_is_active", "running", true},
		{"done_is_not_active", "done", false},
		{"failed_is_not_active", "failed", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := testutil.NewTx(t, testPool)
			ctx := context.Background()

			relatedID := int64(999)
			id, err := jobs.Create(ctx, tx, "library_scan", &relatedID, testutil.StrPtr("collection"))
			require.NoError(t, err)

			switch tt.status {
			case "running":
				require.NoError(t, jobs.MarkRunning(ctx, tx, id))
			case "done":
				require.NoError(t, jobs.MarkDone(ctx, tx, id))
			case "failed":
				require.NoError(t, jobs.MarkFailed(ctx, tx, id, "err"))
			}

			active, err := jobs.IsActive(ctx, tx, "library_scan", relatedID)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, active)
		})
	}
}

func TestRecoverStuckJobs(t *testing.T) {
	tx := testutil.NewTx(t, testPool)
	ctx := context.Background()

	// Create two jobs and mark them running (simulating a crash)
	id1, err := jobs.Create(ctx, tx, "library_scan", nil, nil)
	require.NoError(t, err)
	require.NoError(t, jobs.MarkRunning(ctx, tx, id1))

	id2, err := jobs.Create(ctx, tx, "process_media", nil, nil)
	require.NoError(t, err)
	require.NoError(t, jobs.MarkRunning(ctx, tx, id2))

	// Also create a done job that should NOT be affected
	id3, err := jobs.Create(ctx, tx, "library_scan", nil, nil)
	require.NoError(t, err)
	require.NoError(t, jobs.MarkDone(ctx, tx, id3))

	// Recover
	require.NoError(t, jobs.RecoverStuckJobs(ctx, tx))

	// Verify running jobs are now queued
	j1, err := jobs.GetByID(ctx, tx, id1)
	require.NoError(t, err)
	assert.Equal(t, "queued", j1.Status)
	assert.Nil(t, j1.StartedAt)

	j2, err := jobs.GetByID(ctx, tx, id2)
	require.NoError(t, err)
	assert.Equal(t, "queued", j2.Status)

	// Done job should still be done
	j3, err := jobs.GetByID(ctx, tx, id3)
	require.NoError(t, err)
	assert.Equal(t, "done", j3.Status)
}
