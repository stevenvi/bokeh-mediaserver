package indexer_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/indexer"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	var cleanup func()
	testPool, cleanup = testutil.Setup()
	imaging.Startup()
	code := m.Run()
	imaging.Shutdown()
	cleanup()
	os.Exit(code)
}

func TestRunScan(t *testing.T) {
	testdataDir := findTestdata(t)

	t.Run("enumerates_files_and_queues_processing", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		// Create a temp directory structure mimicking a media library
		mediaPath := t.TempDir()
		collectionRoot := "test_photos"
		collectionDir := filepath.Join(mediaPath, collectionRoot)
		require.NoError(t, os.MkdirAll(collectionDir, 0o755))

		// Copy test images into the collection directory
		copyTestImage(t, testdataDir, "photo_with_exif.jpg", collectionDir)
		copyTestImage(t, testdataDir, "photo_no_exif.png", collectionDir)

		// Create collection in DB — relative_path must match the relative path from mediaPath
		collID := testutil.InsertCollection(t, tx, "Test Photos", constants.CollectionTypePhoto, collectionRoot)

		dataPath := t.TempDir()

		jobID := createTestJob(t, tx, "library_scan", &collID)
		err := indexer.RunScan(ctx, tx, jobID, collID, collectionRoot, mediaPath, dataPath, false)
		require.NoError(t, err)

		// Verify media_items were created
		var itemCount int
		err = tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM media_items WHERE collection_id = $1`, collID,
		).Scan(&itemCount)
		require.NoError(t, err)
		assert.Equal(t, 2, itemCount, "should have created 2 media items")

		// Verify process_media jobs were queued
		var processJobCount int
		err = tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM jobs WHERE type = 'process_media' AND status = 'queued'`,
		).Scan(&processJobCount)
		require.NoError(t, err)
		assert.Equal(t, 2, processJobCount, "should have queued 2 process_media jobs")
	})

	t.Run("unchanged_files_not_requeued", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		mediaPath := t.TempDir()
		collectionRoot := "test_photos"
		collectionDir := filepath.Join(mediaPath, collectionRoot)
		require.NoError(t, os.MkdirAll(collectionDir, 0o755))
		copyTestImage(t, testdataDir, "photo_with_exif.jpg", collectionDir)

		collID := testutil.InsertCollection(t, tx, "Test Photos", constants.CollectionTypePhoto, collectionRoot)
		dataPath := t.TempDir()

		// First scan
		jobID1 := createTestJob(t, tx, "library_scan", &collID)
		require.NoError(t, indexer.RunScan(ctx, tx, jobID1, collID, collectionRoot, mediaPath, dataPath, false))

		// Mark first scan's process jobs as done so they're not counted
		testutil.MustExec(t, tx, "UPDATE jobs SET status = 'done' WHERE type = 'process_media'")

		// Second scan — file hasn't changed
		jobID2 := createTestJob(t, tx, "library_scan", &collID)
		require.NoError(t, indexer.RunScan(ctx, tx, jobID2, collID, collectionRoot, mediaPath, dataPath, false))

		// No new process_media jobs should have been created
		var queuedCount int
		err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM jobs WHERE type = 'process_media' AND status = 'queued'`,
		).Scan(&queuedCount)
		require.NoError(t, err)
		assert.Equal(t, 0, queuedCount, "unchanged files should not be re-queued")
	})

	t.Run("marks_missing_files", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		mediaPath := t.TempDir()
		collectionRoot := "test_photos"
		collectionDir := filepath.Join(mediaPath, collectionRoot)
		require.NoError(t, os.MkdirAll(collectionDir, 0o755))
		copyTestImage(t, testdataDir, "photo_with_exif.jpg", collectionDir)

		collID := testutil.InsertCollection(t, tx, "Test Photos", constants.CollectionTypePhoto, collectionRoot)
		dataPath := t.TempDir()

		// First scan
		jobID1 := createTestJob(t, tx, "library_scan", &collID)
		require.NoError(t, indexer.RunScan(ctx, tx, jobID1, collID, collectionRoot, mediaPath, dataPath, false))

		// Delete the file
		require.NoError(t, os.Remove(filepath.Join(collectionDir, "photo_with_exif.jpg")))

		// Second scan
		jobID2 := createTestJob(t, tx, "library_scan", &collID)
		require.NoError(t, indexer.RunScan(ctx, tx, jobID2, collID, collectionRoot, mediaPath, dataPath, false))

		// Verify the item is marked missing
		var missingCount int
		err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM media_items WHERE collection_id = $1 AND missing_since IS NOT NULL`, collID,
		).Scan(&missingCount)
		require.NoError(t, err)
		assert.Equal(t, 1, missingCount, "deleted file should be marked missing")
	})
}

func TestHandleProcessMedia(t *testing.T) {
	testdataDir := findTestdata(t)

	t.Run("extracts_exif_and_generates_variants", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		// Set up mediaPath and dataPath
		mediaPath := t.TempDir()
		dataPath := t.TempDir()

		// Copy test image into mediaPath
		imgRelPath := "photo.jpg"
		imgPath := filepath.Join(mediaPath, imgRelPath)
		copyFile(t, filepath.Join(testdataDir, "photo_with_exif.jpg"), imgPath)

		// Create collection and media_item using relative path
		collID := testutil.InsertCollection(t, tx, "Test", constants.CollectionTypePhoto, "test")
		itemID := testutil.InsertMediaItem(t, tx, collID, "photo", imgRelPath, "image/jpeg")

		// Create a process_media job (already in running state, as the dispatcher would set it)
		relatedType := "media_item"
		var jobID int64
		err := tx.QueryRow(ctx,
			`INSERT INTO jobs (type, status, related_id, related_type)
			 VALUES ('process_media', 'running', $1, $2) RETURNING id`,
			itemID, relatedType,
		).Scan(&jobID)
		require.NoError(t, err)

		// Run the handler
		pw := indexer.NewProcessingWorkers(1)
		defer pw.CloseAll()

		handler := indexer.HandleProcessMediaWithWorkers(pw, mediaPath, dataPath, 4000)
		job := &models.Job{
			ID:          jobID,
			Type:        "process_media",
			Status:      "running",
			RelatedID:   &itemID,
			RelatedType: &relatedType,
		}
		err = handler(ctx, tx, job)
		require.NoError(t, err)

		// Verify photo_metadata was created with EXIF data
		var width, height *int
		var cameraMake *string
		err = tx.QueryRow(ctx,
			`SELECT width_px, height_px, camera_make FROM photo_metadata WHERE media_item_id = $1`,
			itemID,
		).Scan(&width, &height, &cameraMake)
		require.NoError(t, err)

		require.NotNil(t, width)
		require.NotNil(t, height)
		assert.Equal(t, 64, *width)
		assert.Equal(t, 64, *height)

		require.NotNil(t, cameraMake, "camera make should be extracted from EXIF")
		assert.Equal(t, "TestCamera", *cameraMake)

		// Verify variants_generated_at was set
		var variantsGenerated bool
		err = tx.QueryRow(ctx,
			`SELECT variants_generated_at IS NOT NULL FROM photo_metadata WHERE media_item_id = $1`,
			itemID,
		).Scan(&variantsGenerated)
		require.NoError(t, err)
		assert.True(t, variantsGenerated, "variants_generated_at should be set")
	})

	t.Run("handles_missing_media_item", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		nonExistentID := int64(999999)
		relatedType := "media_item"
		var jobID int64
		err := tx.QueryRow(ctx,
			`INSERT INTO jobs (type, status, related_id, related_type)
			 VALUES ('process_media', 'running', $1, $2) RETURNING id`,
			nonExistentID, relatedType,
		).Scan(&jobID)
		require.NoError(t, err)

		mediaPath := t.TempDir()
		dataPath := t.TempDir()
		pw := indexer.NewProcessingWorkers(1)
		defer pw.CloseAll()

		handler := indexer.HandleProcessMediaWithWorkers(pw, mediaPath, dataPath, 4000)
		job := &models.Job{
			ID:          jobID,
			Type:        "process_media",
			Status:      "running",
			RelatedID:   &nonExistentID,
			RelatedType: &relatedType,
		}
		err = handler(ctx, tx, job)
		assert.Error(t, err, "should fail for non-existent media item")
	})

	t.Run("handles_no_related_id", func(t *testing.T) {
		tx := testutil.NewTx(t, testPool)
		ctx := context.Background()

		var jobID int64
		err := tx.QueryRow(ctx,
			`INSERT INTO jobs (type, status) VALUES ('process_media', 'running') RETURNING id`,
		).Scan(&jobID)
		require.NoError(t, err)

		mediaPath := t.TempDir()
		dataPath := t.TempDir()
		pw := indexer.NewProcessingWorkers(1)
		defer pw.CloseAll()

		handler := indexer.HandleProcessMediaWithWorkers(pw, mediaPath, dataPath, 4000)
		job := &models.Job{ID: jobID, Type: "process_media", Status: "running"}
		err = handler(ctx, tx, job)
		assert.Error(t, err, "should fail without related_id")
		assert.Contains(t, err.Error(), "no related_id")
	})
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func findTestdata(t *testing.T) string {
	t.Helper()
	dir := "testdata"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Skip("testdata directory not found — provide test images (see CLAUDE.md)")
	}
	for _, f := range []string{"photo_with_exif.jpg", "photo_no_exif.png"} {
		if _, err := os.Stat(filepath.Join(dir, f)); os.IsNotExist(err) {
			t.Skipf("testdata/%s not found — provide test images", f)
		}
	}
	return dir
}

func copyTestImage(t *testing.T, testdataDir, filename, destDir string) {
	t.Helper()
	copyFile(t, filepath.Join(testdataDir, filename), filepath.Join(destDir, filename))
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, data, 0o644))
}

func createTestJob(t *testing.T, db utils.DBTX, jobType string, relatedID *int64) int64 {
	t.Helper()
	relatedType := "collection"
	id, err := repository.JobCreate(context.Background(), db, jobType, relatedID, &relatedType)
	require.NoError(t, err)
	return id
}
