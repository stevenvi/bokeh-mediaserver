package indexer

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/blake2b"

	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

var supportedExtensions = map[string]string{
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".png":  "image/png",
	".heic": "image/heic",
	".heif": "image/heif",
	".tiff": "image/tiff",
	".tif":  "image/tiff",
	".webp": "image/webp",
	".avif": "image/avif",
}

// RunScan performs a lightweight enumeration scan of a collection's relative_path.
// It walks the directory tree, upserts media_items, and queues process_media
// jobs for new or changed files. Heavy processing (EXIF, variants, DZI) is
// handled by the process_media handler in the processing worker pool.
//
// Called by HandleScanJob — the job is already marked as 'running' by the dispatcher.
// Returns an error on failure; the dispatcher handles MarkDone/MarkFailed.
func RunScan(ctx context.Context, db utils.DBTX,
	jobID, collectionID int64, relativePath string, mediaPath string, dataPath string) error {

	slog.Info("RunScan starting", "job_id", jobID, "collection_id", collectionID)

	jobStart := time.Now()

	logMsg := func(msg string) {
		slog.Info(msg, "job_id", jobID, "collection_id", collectionID)
		_ = jobs.UpdateProgress(ctx, db, jobID, msg)
	}

	// Construct full path: mediaPath is base, rootPath is relative path within media
	collectionPath := filepath.Join(mediaPath, relativePath)
	logMsg(fmt.Sprintf("scan started: %s", collectionPath))

	// Phase 1 — walk directory tree and upsert sub-collections (folders)
	pathToID, err := walkFolders(ctx, db, collectionID, collectionPath, mediaPath)
	if err != nil {
		return fmt.Errorf("walk folders: %w", err)
	}
	slog.Info("Folder walk complete", "job_id", jobID)

	// Phase 2 — enumerate files, upsert media_items, queue process_media jobs
	var (
		enumerated int64
		unchanged  int64
		queued     int64
		errCount   int64
	)

	err = filepath.WalkDir(collectionPath, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil // skip unreadable entries
		}
		if d.IsDir() || ctx.Err() != nil {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		mimeType, ok := supportedExtensions[ext]
		if !ok {
			return nil
		}

		enumerated++

		// Stat and hash for change detection
		stat, err := os.Stat(path)
		if err != nil {
			slog.Warn("stat failed", "path", path, "err", err)
			errCount++
			return nil
		}
		fileSize := stat.Size()

		fileHash, err := computeFileHash(path, fileSize)
		if err != nil {
			slog.Warn("hash failed", "path", path, "err", err)
			errCount++
			return nil
		}

		// Compute relative path for DB storage
		relativePath, err := filepath.Rel(mediaPath, path)
		if err != nil {
			slog.Warn("compute relative path", "path", path, "err", err)
			errCount++
			return nil
		}

		// Look up collection from cache (no DB query)
		dirPath := filepath.Dir(path)
		folderCollectionID, ok := pathToID[dirPath]
		if !ok {
			slog.Warn("collection lookup failed", "path", dirPath)
			errCount++
			return nil
		}

		title := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))

		// Single-trip upsert: captures pre-existing state in CTE to detect changes
		var itemID int64
		var wasUnchanged bool
		err = db.QueryRow(ctx, `
			WITH prev AS (
				SELECT id, file_size_bytes, file_hash
				FROM media_items
				WHERE relative_path = $3
			),
			upserted AS (
				INSERT INTO media_items (collection_id, title, relative_path, file_size_bytes, file_hash, mime_type)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (relative_path) DO UPDATE SET
					indexed_at      = now(),
					missing_since   = NULL,
					collection_id   = EXCLUDED.collection_id,
					file_size_bytes = EXCLUDED.file_size_bytes,
					file_hash       = EXCLUDED.file_hash,
					mime_type       = EXCLUDED.mime_type
				RETURNING id
			)
			SELECT
				u.id,
				(p.id IS NOT NULL AND p.file_size_bytes = $4 AND p.file_hash = $5) AS was_unchanged
			FROM upserted u
			LEFT JOIN prev p ON true`,
			folderCollectionID, title, relativePath, fileSize, fileHash, mimeType,
		).Scan(&itemID, &wasUnchanged)
		if err != nil {
			slog.Warn("upsert media_item failed", "path", path, "err", err)
			errCount++
			return nil
		}

		if wasUnchanged {
			unchanged++
			return nil
		}

		// Queue a process_media job for this item
		relatedType := "media_item"
		_, err = jobs.Create(ctx, db, "process_media", &itemID, &relatedType)
		if err != nil {
			slog.Warn("queue process_media job", "item_id", itemID, "err", err)
			errCount++
			return nil
		}
		queued++

		return nil
	})

	if err != nil {
		return fmt.Errorf("walk files: %w", err)
	}

	// Phase 3 — mark files not seen this scan as missing
	tag, err := db.Exec(ctx,
		`UPDATE media_items
		 SET missing_since = now()
		 WHERE collection_id = $1
		   AND missing_since IS NULL
		   AND indexed_at < $2`,
		collectionID, jobStart,
	)
	if err != nil {
		return fmt.Errorf("mark missing: %w", err)
	}

	summary := fmt.Sprintf("scan complete: %d enumerated, %d unchanged, %d queued for processing, %d errors, %d marked missing",
		enumerated, unchanged, queued, errCount, tag.RowsAffected())
	logMsg(summary)

	_, _ = db.Exec(ctx,
		`UPDATE collections SET last_scanned_at = now() WHERE id = $1`,
		collectionID,
	)

	return nil
}

// walkFolders upserts a sub-collection row for each directory under rootPath.
// Returns a map from absolute directory path to collection ID for use during file enumeration.
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
		var id int64
		err = db.QueryRow(ctx,
			// TODO: ON CONFLICT clause seems redundant, is there a better way to do this?
			`INSERT INTO collections (parent_collection_id, name, type, relative_path)
			 VALUES ($1, $2,
			     (SELECT type FROM collections WHERE id = $3),
			     $4)
			 ON CONFLICT (relative_path) WHERE relative_path IS NOT NULL
			     DO UPDATE SET name                 = EXCLUDED.name,
			                   parent_collection_id = EXCLUDED.parent_collection_id
			 RETURNING id`,
			parentID, name, rootCollectionID, relPath,
		).Scan(&id)
		if err != nil {
			return fmt.Errorf("upsert folder %s: %w", path, err)
		}

		pathToID[path] = id
		return nil
	})

	return pathToID, err
}


const (
	fullHashThreshold = 2 * 1024 * 1024 // 2 MB — files at or below this are fully hashed
	partialHashBlock  = 1 * 1024 * 1024 // 1 MB — read from start and end for larger files
)

// computeFileHash returns a BLAKE2b-256 hex hash of the file's content.
// Files ≤ 2 MB are fully hashed. Larger files use a partial strategy:
// hash(first 1 MB ∥ last 1 MB ∥ uint64(size)), which is fast for large
// video files while remaining practically collision-free.
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
		if _, err := io.Copy(h, f); err != nil {
			return "", err
		}
	} else {
		buf := make([]byte, partialHashBlock)

		// First 1 MB
		n, err := io.ReadFull(f, buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			return "", err
		}
		h.Write(buf[:n])

		// Last 1 MB
		if _, err := f.Seek(-partialHashBlock, io.SeekEnd); err != nil {
			return "", err
		}
		n, err = io.ReadFull(f, buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			return "", err
		}
		h.Write(buf[:n])

		// File size as big-endian uint64 — distinguishes files that differ only in size
		var sizeBuf [8]byte
		binary.BigEndian.PutUint64(sizeBuf[:], uint64(size))
		h.Write(sizeBuf[:])
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
