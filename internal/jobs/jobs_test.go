package jobs_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
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
			repo := repository.NewJobRepository(tx)

			id, err := repo.Create(ctx, tt.jobType, tt.relatedID, tt.relatedType)
			require.NoError(t, err)
			assert.Greater(t, id, int64(0))

			job, err := repo.GetByID(ctx, id)
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
		repo := repository.NewJobRepository(tx)

		id, err := repo.Create(ctx, "library_scan", testutil.Int64Ptr(1), testutil.StrPtr("collection"))
		require.NoError(t, err)

		job, err := repo.ClaimNext(ctx, []string{"library_scan"})
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, id, job.ID)
		assert.Equal(t, "running", job.Status)
		assert.NotNil(t, job.StartedAt)
	})

	t.Run("returns_nil_when_empty", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()
		repo := repository.NewJobRepository(tx)

		job, err := repo.ClaimNext(ctx, []string{"library_scan"})
		require.NoError(t, err)
		assert.Nil(t, job)
	})

	t.Run("skips_running_jobs", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()
		repo := repository.NewJobRepository(tx)

		id, err := repo.Create(ctx, "library_scan", testutil.Int64Ptr(1), testutil.StrPtr("collection"))
		require.NoError(t, err)
		require.NoError(t, repo.MarkRunning(ctx, id))

		job, err := repo.ClaimNext(ctx, []string{"library_scan"})
		require.NoError(t, err)
		assert.Nil(t, job, "should not claim already-running job")
	})

	t.Run("filters_by_type", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()
		repo := repository.NewJobRepository(tx)

		_, err := repo.Create(ctx, "library_scan", testutil.Int64Ptr(1), testutil.StrPtr("collection"))
		require.NoError(t, err)

		job, err := repo.ClaimNext(ctx, []string{"process_media"})
		require.NoError(t, err)
		assert.Nil(t, job, "should not claim job of wrong type")
	})

	t.Run("claims_oldest_first", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()
		repo := repository.NewJobRepository(tx)

		id1, err := repo.Create(ctx, "library_scan", testutil.Int64Ptr(1), testutil.StrPtr("collection"))
		require.NoError(t, err)
		_, err = repo.Create(ctx, "library_scan", testutil.Int64Ptr(2), testutil.StrPtr("collection"))
		require.NoError(t, err)

		job, err := repo.ClaimNext(ctx, []string{"library_scan"})
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, id1, job.ID, "should claim oldest queued job first")
	})
}

func TestUpdateProgress(t *testing.T) {
	tx := testutil.NewTx(t, testPool)
	ctx := context.Background()
	repo := repository.NewJobRepository(tx)

	id, err := repo.Create(ctx, "library_scan", nil, nil)
	require.NoError(t, err)

	require.NoError(t, repo.UpdateProgress(ctx, id, "step 1"))
	require.NoError(t, repo.UpdateProgress(ctx, id, "step 2"))

	job, err := repo.GetByID(ctx, id)
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
			repo := repository.NewJobRepository(tx)

			id, err := repo.Create(ctx, "library_scan", nil, nil)
			require.NoError(t, err)

			switch tt.action {
			case "running":
				require.NoError(t, repo.MarkRunning(ctx, id))
			case "done":
				require.NoError(t, repo.MarkDone(ctx, id))
			case "failed":
				require.NoError(t, repo.MarkFailed(ctx, id, "something broke"))
			}

			job, err := repo.GetByID(ctx, id)
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
			repo := repository.NewJobRepository(tx)

			relatedID := int64(999)
			id, err := repo.Create(ctx, "library_scan", &relatedID, testutil.StrPtr("collection"))
			require.NoError(t, err)

			switch tt.status {
			case "running":
				require.NoError(t, repo.MarkRunning(ctx, id))
			case "done":
				require.NoError(t, repo.MarkDone(ctx, id))
			case "failed":
				require.NoError(t, repo.MarkFailed(ctx, id, "err"))
			}

			active, err := repo.IsActive(ctx, "library_scan", relatedID)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, active)
		})
	}
}

func TestRecoverStuckJobs(t *testing.T) {
	tx := testutil.NewTx(t, testPool)
	ctx := context.Background()
	repo := repository.NewJobRepository(tx)

	// Create two jobs and mark them running (simulating a crash)
	id1, err := repo.Create(ctx, "library_scan", nil, nil)
	require.NoError(t, err)
	require.NoError(t, repo.MarkRunning(ctx, id1))

	id2, err := repo.Create(ctx, "process_media", nil, nil)
	require.NoError(t, err)
	require.NoError(t, repo.MarkRunning(ctx, id2))

	// Also create a done job that should NOT be affected
	id3, err := repo.Create(ctx, "library_scan", nil, nil)
	require.NoError(t, err)
	require.NoError(t, repo.MarkDone(ctx, id3))

	// Recover
	require.NoError(t, repo.RecoverStuck(ctx))

	// Verify running jobs are now queued
	j1, err := repo.GetByID(ctx, id1)
	require.NoError(t, err)
	assert.Equal(t, "queued", j1.Status)
	assert.Nil(t, j1.StartedAt)

	j2, err := repo.GetByID(ctx, id2)
	require.NoError(t, err)
	assert.Equal(t, "queued", j2.Status)

	// Done job should still be done
	j3, err := repo.GetByID(ctx, id3)
	require.NoError(t, err)
	assert.Equal(t, "done", j3.Status)
}
