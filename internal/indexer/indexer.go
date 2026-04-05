package indexer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/blake2b"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// ── Building blocks ────────────────────────────────────────────────────────────

// walkFolders upserts a sub-collection row for each directory under rootPath.
// Returns a map from absolute directory path to collection ID for use during
// file enumeration.
func walkFolders(ctx context.Context, db utils.DBTX, rootCollectionID int64, rootPath string, mediaPath string) (map[string]int64, error) {
	pathToID := map[string]int64{rootPath: rootCollectionID}

	err := filepath.WalkDir(rootPath, func(path string, d os.DirEntry, walkErr error) error {
		slog.Debug("walk folder", "path", path)
		if walkErr != nil || !d.IsDir() || path == rootPath {
			return nil
		}

		relPath, err := filepath.Rel(mediaPath, path)
		if err != nil {
			return fmt.Errorf("relative path for %s: %w", path, err)
		}

		parentPath := filepath.Dir(path)
		parentID, ok := pathToID[parentPath]
		if !ok {
			parentID = rootCollectionID
		}

		name := filepath.Base(path)
		id, err := repository.CollectionUpsertSubCollection(ctx, db, parentID, rootCollectionID, name, relPath)
		if err != nil {
			return fmt.Errorf("upsert folder %s: %w", path, err)
		}

		pathToID[path] = id
		return nil
	})

	return pathToID, err
}

const (
	fullHashThreshold = 1024 * 1024 // 1 MB — files at or below this size are fully hashed
	partialHashFront  = 1024 * 128  // 128 KB in front, hopefully enough to overcome the header
	partialHashBack   = 1024 * 32   // 32 KB in back, to make sure the tail matches what we expect
)

// computeFileHash returns a BLAKE2b-256 hex hash of the file's content.
// (Relatively) small files are fully hashed. Larger ones use a partial strategy:
// hash(beginning ∥ end), which is fast for large video files while
// (hopefully) remaining collision-free.
// The result is stored on the media_item record and used to address derived data on disk.
func computeFileHash(path string, size int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h, err := blake2b.New256(nil)
	if err != nil {
		return "", err
	}

	if size <= fullHashThreshold {
		// File is small enough to hash the entire thing
		if _, err := io.Copy(h, f); err != nil {
			return "", err
		}
	} else {
		// File is larger, only hash portions of it
		// Hash front
		buf := make([]byte, partialHashFront)
		n, err := io.ReadFull(f, buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			return "", err
		}
		h.Write(buf[:n])

		// Hash back
		buf = make([]byte, partialHashBack)
		if _, err := f.Seek(-partialHashBack, io.SeekEnd); err != nil {
			return "", err
		}
		n, err = io.ReadFull(f, buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			return "", err
		}
		h.Write(buf[:n])
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// walkAndUpsert walks collectionPath, hashes and upserts every supported file
// matching mimeCategory, and queues process_media for each one.
//
// When knownPaths is nil all matching files are processed (initial scan); n
// counts files that pass the extension filter regardless of later errors.
// When knownPaths is non-nil files already present in the map are skipped
// (filesystem scan); n counts only newly upserted files.
func walkAndUpsert(ctx context.Context, db utils.DBTX, mimeCategory, collectionPath, mediaPath string, pathToID map[string]int64, knownPaths map[string]struct{}) (n, queued, errCount int64, err error) {
	relatedType := "media_item"

	err = filepath.WalkDir(collectionPath, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() || ctx.Err() != nil {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		mimeType, ok := constants.SupportedExtensions[ext]
		if !ok || !strings.HasPrefix(mimeType, mimeCategory) {
			return nil
		}

		if knownPaths == nil {
			n++ // count all matching files (initial scan: enumerated)
		}

		relPath, relErr := filepath.Rel(mediaPath, path)
		if relErr != nil {
			slog.Warn("compute relative path", "path", path, "err", relErr)
			errCount++
			return nil
		}

		// Skip files already known to the DB — no hashing needed.
		if knownPaths != nil {
			if _, known := knownPaths[relPath]; known {
				return nil
			}
		}

		stat, statErr := os.Stat(path)
		if statErr != nil {
			slog.Warn("stat failed", "path", path, "err", statErr)
			errCount++
			return nil
		}
		fileSize := stat.Size()

		fileHash, hashErr := computeFileHash(path, fileSize)
		if hashErr != nil {
			slog.Warn("hash failed", "path", path, "err", hashErr)
			errCount++
			return nil
		}

		dirPath := filepath.Dir(path)
		folderCollectionID, ok := pathToID[dirPath]
		if !ok {
			slog.Warn("collection lookup failed", "path", dirPath)
			errCount++
			return nil
		}

		title := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))

		itemID, _, upsertErr := repository.MediaItemUpsert(ctx, db, folderCollectionID, title, relPath, fileSize, fileHash, mimeType)
		if upsertErr != nil {
			slog.Warn("upsert media_item failed", "path", path, "err", upsertErr)
			errCount++
			return nil
		}

		if _, jobErr := repository.JobCreate(ctx, db, "process_media", &itemID, &relatedType); jobErr != nil {
			slog.Warn("queue process_media job", "item_id", itemID, "err", jobErr)
			errCount++
			return nil
		}

		if knownPaths != nil {
			n++ // count newly added files (filesystem scan: added)
		}
		queued++
		slog.Debug("queue process_media job", "path", path)

		return nil
	})

	return
}

// syncMissingFromDB compares DB items against the filesystem:
// - Files that have disappeared are marked missing.
// - Previously-missing files that have reappeared have missing_since cleared.
func syncMissingFromDB(ctx context.Context, db utils.DBTX, items []repository.ScanItem, mediaPath string) (marked, restored int64, err error) {
	for _, item := range items {
		fsPath := filepath.Join(mediaPath, item.RelativePath)
		_, statErr := os.Stat(fsPath)

		if os.IsNotExist(statErr) {
			if !item.IsMissing {
				if e := repository.MediaItemMarkMissing(ctx, db, item.ID); e != nil {
					slog.Warn("mark missing failed", "item_id", item.ID, "err", e)
					continue
				}
				marked++
			}
		} else if statErr == nil {
			if item.IsMissing {
				if e := repository.MediaItemClearMissing(ctx, db, item.ID); e != nil {
					slog.Warn("clear missing failed", "item_id", item.ID, "err", e)
					continue
				}
				restored++
			}
		} else {
			slog.Warn("stat error", "path", fsPath, "err", statErr)
		}
	}
	return
}

// checkChangedByDBWalk checks all DB items for content changes:
// - Files that have disappeared are marked missing.
// - Previously-missing files that have reappeared are restored, then checked for changes.
// - Files whose size has changed are re-hashed; if the hash also differs, their
//   size and hash are updated and process_media is queued.
func checkChangedByDBWalk(ctx context.Context, db utils.DBTX, items []repository.ScanItem, mediaPath string) (changed, markedMissing, restored int64, err error) {
	relatedType := "media_item"

	for _, item := range items {
		fsPath := filepath.Join(mediaPath, item.RelativePath)
		stat, statErr := os.Stat(fsPath)

		if os.IsNotExist(statErr) {
			if !item.IsMissing {
				if e := repository.MediaItemMarkMissing(ctx, db, item.ID); e != nil {
					slog.Warn("mark missing failed", "item_id", item.ID, "err", e)
				}
				markedMissing++
			}
			continue
		}
		if statErr != nil {
			slog.Warn("stat error", "path", fsPath, "err", statErr)
			continue
		}

		// File is present on disk.
		if item.IsMissing {
			if e := repository.MediaItemClearMissing(ctx, db, item.ID); e != nil {
				slog.Warn("clear missing failed", "item_id", item.ID, "err", e)
				continue
			}
			restored++
		}

		if stat.Size() == item.FileSizeBytes {
			continue // size unchanged; skip hash check
		}

		hash, hashErr := computeFileHash(fsPath, stat.Size())
		if hashErr != nil {
			slog.Warn("hash failed", "path", fsPath, "err", hashErr)
			continue
		}

		if hash == item.FileHash {
			continue // content unchanged despite size difference; skip reprocess
		}

		if e := repository.MediaItemUpdateSizeAndHash(ctx, db, item.ID, stat.Size(), hash); e != nil {
			slog.Warn("update size/hash failed", "item_id", item.ID, "err", e)
			continue
		}

		itemID := item.ID
		if _, e := repository.JobCreate(ctx, db, "process_media", &itemID, &relatedType); e != nil {
			slog.Warn("queue process_media job", "item_id", itemID, "err", e)
			continue
		}
		changed++
	}

	return
}

// ── Scan functions ─────────────────────────────────────────────────────────────

// RunInitialScan walks the filesystem, hashes and upserts all media files, and
// queues process_media for everything found. Does not mark missing items.
// Intended for first-time collection setup; auto-triggered on collection creation.
func RunInitialScan(ctx context.Context, db utils.DBTX, jobID, collectionID int64, collectionType, relativePath, mediaPath, dataPath string, dispatcher *jobs.Dispatcher) error {
	slog.Info("RunInitialScan starting", "job_id", jobID, "collection_id", collectionID)

	logMsg := func(msg string) {
		slog.Info(msg, "job_id", jobID, "collection_id", collectionID)
		_ = repository.JobUpdateProgress(ctx, db, jobID, msg)
	}

	collectionPath := filepath.Join(mediaPath, relativePath)
	logMsg(fmt.Sprintf("initial scan started: %s", collectionPath))

	pathToID, err := walkFolders(ctx, db, collectionID, collectionPath, mediaPath)
	if err != nil {
		return fmt.Errorf("walk folders: %w", err)
	}

	mimeCategory := strings.SplitN(collectionType, ":", 2)[0]

	enumerated, queued, errCount, err := walkAndUpsert(ctx, db, mimeCategory, collectionPath, mediaPath, pathToID, nil)
	if err != nil {
		return fmt.Errorf("walk files: %w", err)
	}

	if queued > 0 {
		dispatcher.TriggerImmediately()
	}

	logMsg(fmt.Sprintf("initial scan complete: %d enumerated, %d queued for processing, %d errors",
		enumerated, queued, errCount))

	repository.CollectionTouchLastScanned(ctx, db, collectionID)

	return nil
}

// RunFilesystemScan synchronizes the DB's view of the filesystem:
// - New files are added and queued for processing.
// - Files no longer present on disk are marked missing.
// - Previously-missing files that have reappeared are restored.
// Does not hash files already known to the DB.
func RunFilesystemScan(ctx context.Context, db utils.DBTX, jobID, collectionID int64, collectionType, relativePath, mediaPath, dataPath string, dispatcher *jobs.Dispatcher) error {
	slog.Info("RunFilesystemScan starting", "job_id", jobID, "collection_id", collectionID)

	logMsg := func(msg string) {
		slog.Info(msg, "job_id", jobID, "collection_id", collectionID)
		_ = repository.JobUpdateProgress(ctx, db, jobID, msg)
	}

	collectionPath := filepath.Join(mediaPath, relativePath)
	logMsg(fmt.Sprintf("filesystem scan started: %s", collectionPath))

	pathToID, err := walkFolders(ctx, db, collectionID, collectionPath, mediaPath)
	if err != nil {
		return fmt.Errorf("walk folders: %w", err)
	}

	knownPaths, err := repository.MediaItemPathsByCollection(ctx, db, collectionID)
	if err != nil {
		return fmt.Errorf("load known paths: %w", err)
	}

	mimeCategory := strings.SplitN(collectionType, ":", 2)[0]

	added, queued, errCount, err := walkAndUpsert(ctx, db, mimeCategory, collectionPath, mediaPath, pathToID, knownPaths)
	if err != nil {
		return fmt.Errorf("walk new files: %w", err)
	}

	if queued > 0 {
		dispatcher.TriggerImmediately()
	}

	logMsg(fmt.Sprintf("new files: %d added, %d queued for processing, %d errors", added, queued, errCount))

	// Collect all items (missing + non-missing) then sync missing state.
	// Items must be fully collected before further DB operations (pgx conn-busy constraint).
	items, err := repository.MediaItemsForScan(ctx, db, collectionID)
	if err != nil {
		return fmt.Errorf("load items for sync: %w", err)
	}

	marked, restored, err := syncMissingFromDB(ctx, db, items, mediaPath)
	if err != nil {
		return fmt.Errorf("sync missing: %w", err)
	}

	logMsg(fmt.Sprintf("filesystem scan complete: %d marked missing, %d restored", marked, restored))

	repository.CollectionTouchLastScanned(ctx, db, collectionID)

	return nil
}

// RunMetadataScan checks existing DB items for file content changes. Changed
// files are re-queued for metadata extraction. Missing files are marked;
// reappeared files are restored and checked for changes. Does not scan for
// new files — use RunFilesystemScan for that.
func RunMetadataScan(ctx context.Context, db utils.DBTX, jobID, collectionID int64, collectionType, relativePath, mediaPath, dataPath string, dispatcher *jobs.Dispatcher) error {
	slog.Info("RunMetadataScan starting", "job_id", jobID, "collection_id", collectionID)

	logMsg := func(msg string) {
		slog.Info(msg, "job_id", jobID, "collection_id", collectionID)
		_ = repository.JobUpdateProgress(ctx, db, jobID, msg)
	}

	collectionPath := filepath.Join(mediaPath, relativePath)
	logMsg(fmt.Sprintf("metadata scan started: %s", collectionPath))

	items, err := repository.MediaItemsForScan(ctx, db, collectionID)
	if err != nil {
		return fmt.Errorf("load items: %w", err)
	}

	changed, markedMissing, restored, err := checkChangedByDBWalk(ctx, db, items, mediaPath)
	if err != nil {
		return fmt.Errorf("check changes: %w", err)
	}

	if changed > 0 {
		dispatcher.TriggerImmediately()
	}

	logMsg(fmt.Sprintf("metadata scan complete: %d changed (queued), %d marked missing, %d restored",
		changed, markedMissing, restored))

	return nil
}

// ── Job handlers ───────────────────────────────────────────────────────────────

// scanRunFn is the signature shared by all Run*Scan functions.
type scanRunFn func(ctx context.Context, db utils.DBTX, jobID, collectionID int64, collectionType, relativePath, mediaPath, dataPath string, dispatcher *jobs.Dispatcher) error

// makeScanJobHandler returns a job handler that resolves the collection from
// job.RelatedID and delegates to runFn. All three scan job types share this
// boilerplate.
func makeScanJobHandler(jobType, mediaPath, dataPath string, dispatcher *jobs.Dispatcher, runFn scanRunFn) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		if job.RelatedID == nil {
			return fmt.Errorf("%s job %d has no related_id", jobType, job.ID)
		}
		collectionID := *job.RelatedID
		collection, err := repository.CollectionGet(ctx, db, collectionID)
		if err != nil {
			return fmt.Errorf("fetch collection %d: %w", collectionID, err)
		}
		if collection.RelativePath == nil {
			return fmt.Errorf("collection %d has no relative path", collectionID)
		}
		return runFn(ctx, db, job.ID, collectionID, collection.Type, *collection.RelativePath, mediaPath, dataPath, dispatcher)
	}
}

// HandleInitialScanJob is a job handler for initial_scan jobs.
// Performs a full filesystem walk and queues process_media for all items.
// Auto-triggered on collection creation; not exposed via the manual scan API.
func HandleInitialScanJob(mediaPath, dataPath string, dispatcher *jobs.Dispatcher) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return makeScanJobHandler("initial_scan", mediaPath, dataPath, dispatcher, RunInitialScan)
}

// HandleFilesystemScanJob is a job handler for filesystem_scan jobs.
// Adds new files, marks missing files, and restores reappeared files.
func HandleFilesystemScanJob(mediaPath, dataPath string, dispatcher *jobs.Dispatcher) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return makeScanJobHandler("filesystem_scan", mediaPath, dataPath, dispatcher, RunFilesystemScan)
}

// HandleMetadataScanJob is a job handler for metadata_scan jobs.
// Checks for changed files, marks missing, and restores reappeared files.
func HandleMetadataScanJob(mediaPath, dataPath string, dispatcher *jobs.Dispatcher) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return makeScanJobHandler("metadata_scan", mediaPath, dataPath, dispatcher, RunMetadataScan)
}
