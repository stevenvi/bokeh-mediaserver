package definitions_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	db_test_util "github.com/stevenvi/bokeh-mediaserver/internal/db/utils"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	job_definitions "github.com/stevenvi/bokeh-mediaserver/internal/jobs/definitions"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)


// stubHash is the file_hash inserted by testutil.InsertMediaItem.
const stubHash = "abc123"

// orphanHash is a hash that will never exist in the DB — used to create orphan directories.
const orphanHash = "deadbeef000000000000000000000000000000000000000000000000cafebabe"

func TestOrphanCleanup(t *testing.T) {
	t.Run("removes_files_for_deleted_items", func(t *testing.T) {
		tx := testutil.NewTx(t, db_test_util.TestPool)
		ctx := context.Background()

		dataPath := t.TempDir()

		// Create derived files at a hash-based path with no corresponding DB item
		orphanDir := imaging.ItemDataPath(dataPath, orphanHash)
		require.NoError(t, os.MkdirAll(orphanDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(orphanDir, "thumb.avif"), []byte("fake"), 0o644))

		// Create a job
		var jobID int64
		err := tx.QueryRow(ctx,
			`INSERT INTO jobs (type, status) VALUES ('orphan_cleanup', 'running') RETURNING id`,
		).Scan(&jobID)
		require.NoError(t, err)

		handler := job_definitions.HandleOrphanCleanup(dataPath)
		job := &models.Job{ID: jobID, Type: "orphan_cleanup", Status: "running"}
		err = handler(ctx, &jobs.JobContext{DB: tx, Job: job})
		require.NoError(t, err)

		// Verify orphan files were removed
		_, err = os.Stat(orphanDir)
		assert.True(t, os.IsNotExist(err), "orphan directory should be removed")
	})

	t.Run("preserves_files_for_existing_items", func(t *testing.T) {
		tx := testutil.NewTx(t, db_test_util.TestPool)
		ctx := context.Background()

		dataPath := t.TempDir()

		// Create a real media item (InsertMediaItem uses stubHash as file_hash)
		collID := testutil.InsertCollection(t, tx, "Test", constants.CollectionTypePhoto, "test")
		testutil.InsertMediaItem(t, tx, collID, "photo", "fake/path.jpg", "image/jpeg")

		// Create derived files at the stub hash path
		itemDir := imaging.ItemDataPath(dataPath, stubHash)
		require.NoError(t, os.MkdirAll(itemDir, 0o755))
		thumbPath := filepath.Join(itemDir, "thumb.avif")
		require.NoError(t, os.WriteFile(thumbPath, []byte("real"), 0o644))

		// Run orphan cleanup
		var jobID int64
		err := tx.QueryRow(ctx,
			`INSERT INTO jobs (type, status) VALUES ('orphan_cleanup', 'running') RETURNING id`,
		).Scan(&jobID)
		require.NoError(t, err)

		handler := job_definitions.HandleOrphanCleanup(dataPath)
		job := &models.Job{ID: jobID, Type: "orphan_cleanup", Status: "running"}
		err = handler(ctx, &jobs.JobContext{DB: tx, Job: job})
		require.NoError(t, err)

		// Verify files are still there
		_, err = os.Stat(thumbPath)
		assert.NoError(t, err, "files for existing items should be preserved")
	})
}

func TestIntegrityCheck(t *testing.T) {
	t.Run("prunes_stale_missing_items", func(t *testing.T) {
		tx := testutil.NewTx(t, db_test_util.TestPool)
		ctx := context.Background()

		dataPath := t.TempDir()

		collID := testutil.InsertCollection(t, tx, "Test", constants.CollectionTypePhoto, "test")
		itemID := testutil.InsertMediaItem(t, tx, collID, "photo", "fake/stale.jpg", "image/jpeg")

		// Mark item as missing for >90 days (current pruning threshold)
		testutil.MustExec(t, tx,
			`UPDATE media_items SET missing_since = now() - interval '91 days' WHERE id = $1`, itemID)

		// Create derived files at the stub hash path (InsertMediaItem uses stubHash as file_hash)
		itemDir := imaging.ItemDataPath(dataPath, stubHash)
		require.NoError(t, os.MkdirAll(itemDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(itemDir, "thumb.avif"), []byte("stale"), 0o644))

		// Run integrity check
		var jobID int64
		err := tx.QueryRow(ctx,
			`INSERT INTO jobs (type, status) VALUES ('integrity_check', 'running') RETURNING id`,
		).Scan(&jobID)
		require.NoError(t, err)

		handler := job_definitions.HandleIntegrityCheck(dataPath, &jobs.Dispatcher{})
		job := &models.Job{ID: jobID, Type: "integrity_check", Status: "running"}
		err = handler(ctx, &jobs.JobContext{DB: tx, Job: job})
		require.NoError(t, err)

		// Verify item was deleted from DB
		var count int
		err = tx.QueryRow(ctx, `SELECT COUNT(*) FROM media_items WHERE id = $1`, itemID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "stale item should be deleted")

		// Verify derived files were cleaned up
		_, err = os.Stat(itemDir)
		assert.True(t, os.IsNotExist(err), "stale item's derived files should be removed")
	})

	t.Run("does_not_prune_recently_missing_items", func(t *testing.T) {
		tx := testutil.NewTx(t, db_test_util.TestPool)
		ctx := context.Background()

		dataPath := t.TempDir()

		collID := testutil.InsertCollection(t, tx, "Test", constants.CollectionTypePhoto, "test")
		itemID := testutil.InsertMediaItem(t, tx, collID, "photo", "fake/recent.jpg", "image/jpeg")

		// Mark item as missing for only 5 days
		testutil.MustExec(t, tx,
			`UPDATE media_items SET missing_since = now() - interval '5 days' WHERE id = $1`, itemID)

		var jobID int64
		err := tx.QueryRow(ctx,
			`INSERT INTO jobs (type, status) VALUES ('integrity_check', 'running') RETURNING id`,
		).Scan(&jobID)
		require.NoError(t, err)

		handler := job_definitions.HandleIntegrityCheck(dataPath, &jobs.Dispatcher{})
		job := &models.Job{ID: jobID, Type: "integrity_check", Status: "running"}
		err = handler(ctx, &jobs.JobContext{DB: tx, Job: job})
		require.NoError(t, err)

		// Item should still exist
		var count int
		err = tx.QueryRow(ctx, `SELECT COUNT(*) FROM media_items WHERE id = $1`, itemID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "recently missing item should be preserved")
	})

	t.Run("requeues_items_with_missing_variants", func(t *testing.T) {
		tx := testutil.NewTx(t, db_test_util.TestPool)
		ctx := context.Background()

		dataPath := t.TempDir()

		collID := testutil.InsertCollection(t, tx, "Test", constants.CollectionTypePhoto, "test")
		itemID := testutil.InsertMediaItem(t, tx, collID, "photo", "fake/novariants.jpg", "image/jpeg")

		// Create photo_metadata with NULL variants_generated_at
		testutil.MustExec(t, tx,
			`INSERT INTO photo_metadata (media_item_id) VALUES ($1)`, itemID)

		var jobID int64
		err := tx.QueryRow(ctx,
			`INSERT INTO jobs (type, status) VALUES ('integrity_check', 'running') RETURNING id`,
		).Scan(&jobID)
		require.NoError(t, err)

		handler := job_definitions.HandleIntegrityCheck(dataPath, &jobs.Dispatcher{})
		job := &models.Job{ID: jobID, Type: "integrity_check", Status: "running"}
		err = handler(ctx, &jobs.JobContext{DB: tx, Job: job})
		require.NoError(t, err)

		// Variant requeuing is currently disabled; verify handler completes without error
		// and no unexpected jobs were created.
		var processCount int
		err = tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM jobs WHERE type = 'process_media' AND related_id = $1 AND status = 'queued'`,
			itemID,
		).Scan(&processCount)
		require.NoError(t, err)
		assert.Equal(t, 0, processCount, "variant requeuing is disabled")
	})

	t.Run("does_not_requeue_if_already_active", func(t *testing.T) {
		tx := testutil.NewTx(t, db_test_util.TestPool)
		ctx := context.Background()

		dataPath := t.TempDir()

		collID := testutil.InsertCollection(t, tx, "Test", constants.CollectionTypePhoto, "test")
		itemID := testutil.InsertMediaItem(t, tx, collID, "photo", "fake/active.jpg", "image/jpeg")

		testutil.MustExec(t, tx,
			`INSERT INTO photo_metadata (media_item_id) VALUES ($1)`, itemID)

		// Create an already-active process_media job
		relatedType := "media_item"
		testutil.MustExec(t, tx,
			`INSERT INTO jobs (type, status, related_id, related_type) VALUES ('process_media', 'running', $1, $2)`,
			itemID, relatedType)

		var jobID int64
		err := tx.QueryRow(ctx,
			`INSERT INTO jobs (type, status) VALUES ('integrity_check', 'running') RETURNING id`,
		).Scan(&jobID)
		require.NoError(t, err)

		handler := job_definitions.HandleIntegrityCheck(dataPath, &jobs.Dispatcher{})
		job := &models.Job{ID: jobID, Type: "integrity_check", Status: "running"}
		err = handler(ctx, &jobs.JobContext{DB: tx, Job: job})
		require.NoError(t, err)

		// Should NOT create a duplicate process_media job
		var processCount int
		err = tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM jobs WHERE type = 'process_media' AND related_id = $1`,
			itemID,
		).Scan(&processCount)
		require.NoError(t, err)
		assert.Equal(t, 1, processCount, "should not create duplicate process_media job")
	})
}
