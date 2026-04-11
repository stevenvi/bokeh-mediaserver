package definitions

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
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// CollectionScanMeta describes the collection_scan job type.
var CollectionScanMeta = jobs.JobMeta{
	Description:     "Scan collection for new, changed, and missing files",
	TotalSteps:      3,
	SupportsSubjobs: true,
	MaxConcurrency:  0, // runtime.NumCPU()
}

// mimeToScanJobType maps MIME type prefixes to sub-job types.
var mimeToScanJobType = map[string]string{
	"image/": "scan_photo",
	"video/": "scan_video",
	"audio/": "scan_audio",
}

const (
	fullHashThreshold = 1024 * 1024 // 1 MB — files at or below this size are fully hashed
	partialHashFront  = 1024 * 128  // 128 KB in front
	partialHashBack   = 1024 * 32   // 32 KB in back
)

// HandleCollectionScan returns a job handler that scans a collection for new,
// changed, and missing files. The job's related_id must be the collection ID.
//
// Step 1: Walk directories and upsert sub-collections.
// Step 2: Walk files, upsert media items, queue sub-jobs for new/changed files.
// Step 3: Mark items not seen in walk as missing.
func HandleCollectionScan(mediaPath, dataPath string) jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		db, job := jc.DB, jc.Job
		if job.RelatedID == nil {
			return fmt.Errorf("collection_scan job %d has no related_id", job.ID)
		}
		collectionID := *job.RelatedID

		collection, err := repository.CollectionGet(ctx, db, collectionID)
		if err != nil {
			return fmt.Errorf("fetch collection %d: %w", collectionID, err)
		}
		if collection.RelativePath == nil {
			return fmt.Errorf("collection %d has no relative path", collectionID)
		}

		collectionPath := filepath.Join(mediaPath, *collection.RelativePath)
		mimeCategory := strings.SplitN(string(collection.Type), ":", 2)[0]

		// Step 1: Walk directories and upsert sub-collections
		jc.SetStep(ctx, 1)
		_ = repository.JobUpdateProgress(ctx, db, job.ID, "Walking directories")
		slog.Info("collection scan: walking directories", "job_id", job.ID, "collection_id", collectionID, "path", collectionPath)

		pathToID, err := walkFolders(ctx, db, collectionID, collectionPath, mediaPath)
		if err != nil {
			return fmt.Errorf("walk folders: %w", err)
		}

		// Step 2: Walk files and queue sub-jobs for new/changed files
		jc.SetStep(ctx, 2)
		_ = repository.JobUpdateProgress(ctx, db, job.ID, "Walking files")
		slog.Info("collection scan: walking files", "job_id", job.ID, "collection_id", collectionID)

		// Load known items to detect new vs existing files and whether metadata is complete.
		knownItems, err := repository.MediaItemsKnownForScan(ctx, db, collectionID, mimeCategory)
		if err != nil {
			return fmt.Errorf("load known items: %w", err)
		}

		seenPaths := make(map[string]struct{})
		var filesWalked, filesQueued, errCount int64

		err = filepath.WalkDir(collectionPath, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() || ctx.Err() != nil {
				return nil
			}

			ext := strings.ToLower(filepath.Ext(path))
			mimeType, ok := constants.SupportedExtensions[ext]
			if !ok || !strings.HasPrefix(mimeType, mimeCategory) {
				return nil
			}

			filesWalked++
			if filesWalked%1000 == 0 {
				slog.Info("collection scan: walking files", "job_id", job.ID, "walked", filesWalked)
			}

			relPath, relErr := filepath.Rel(mediaPath, path)
			if relErr != nil {
				slog.Warn("compute relative path", "path", path, "err", relErr)
				errCount++
				return nil
			}

			seenPaths[relPath] = struct{}{}

			// Determine sub-job type from MIME prefix
			var scanJobType string
			for prefix, jobType := range mimeToScanJobType {
				if strings.HasPrefix(mimeType, prefix) {
					scanJobType = jobType
					break
				}
			}
			if scanJobType == "" {
				return nil // unsupported MIME
			}

			relatedType := "media_item"

			// If the file is already in the DB, check whether its metadata sub-job completed.
			if known, exists := knownItems[relPath]; exists {
				if known.HasMetadata {
					return nil // fully processed, skip
				}
				// media_item exists but metadata record is missing — sub-job likely failed.
				// Re-queue it without re-hashing or re-upserting.
				jc.AddSubJob(scanJobType, &known.ID, &relatedType)
				filesQueued++
				slog.Debug("re-queued scan sub-job for missing metadata", "path", path, "type", scanJobType)
				return nil
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

			jc.AddSubJob(scanJobType, &itemID, &relatedType)
			filesQueued++
			slog.Debug("queued scan sub-job", "path", path, "type", scanJobType)

			return nil
		})
		if err != nil {
			return fmt.Errorf("walk files: %w", err)
		}

		slog.Info("collection scan: files walked",
			"job_id", job.ID,
			"walked", filesWalked,
			"queued", filesQueued,
			"errors", errCount,
		)

		// Step 3: Mark items missing / restore reappeared items
		jc.SetStep(ctx, 3)
		_ = repository.JobUpdateProgress(ctx, db, job.ID, "Marking missing items")
		slog.Info("collection scan: syncing missing items", "job_id", job.ID)

		items, err := repository.MediaItemsForScan(ctx, db, collectionID)
		if err != nil {
			return fmt.Errorf("load items for sync: %w", err)
		}

		var marked, restored int64
		for _, item := range items {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			_, seen := seenPaths[item.RelativePath]

			if !seen {
				if !item.IsMissing {
					if e := repository.MediaItemMarkMissing(ctx, db, item.ID); e != nil {
						slog.Warn("mark missing failed", "item_id", item.ID, "err", e)
						continue
					}
					marked++
				}
			} else {
				if item.IsMissing {
					if e := repository.MediaItemClearMissing(ctx, db, item.ID); e != nil {
						slog.Warn("clear missing failed", "item_id", item.ID, "err", e)
						continue
					}
					restored++
				}
			}
		}

		slog.Info("collection scan: sync complete",
			"job_id", job.ID,
			"marked_missing", marked,
			"restored", restored,
		)

		repository.CollectionTouchLastScanned(ctx, db, collectionID)

		return nil
	}
}

// walkFolders upserts a sub-collection row for each directory under rootPath.
// Returns a map from absolute directory path to collection ID.
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

// computeFileHash returns a BLAKE2b-256 hex hash of the file's content.
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
		if _, err := io.Copy(h, f); err != nil {
			return "", err
		}
	} else {
		buf := make([]byte, partialHashFront)
		n, err := io.ReadFull(f, buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			return "", err
		}
		h.Write(buf[:n])

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
