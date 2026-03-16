package indexer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
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

// RunScan performs a full index scan of a collection's root_path.
// Intended to run in a goroutine — updates the job row as it progresses.
func RunScan(ctx context.Context, db *pgxpool.Pool, pool *jobs.Pool,
	jobID, collectionID int, rootPath, dataPath string) {

	jobStart := time.Now()

	if err := jobs.MarkRunning(ctx, db, jobID); err != nil {
		slog.Error("mark job running", "err", err)
		return
	}

	logMsg := func(msg string) {
		slog.Info(msg, "job_id", jobID, "collection_id", collectionID)
		_ = jobs.UpdateProgress(ctx, db, jobID, msg)
	}

	logMsg(fmt.Sprintf("scan started: %s", rootPath))

	// Start a persistent exiftool process for this scan.
	// All files are piped through one process — avoids per-file Perl startup cost.
	// TODO: Only do this for image collections, it is a waste of time for any other media type.
	et, err := newExiftoolProcess()
	if err != nil {
		_ = jobs.MarkFailed(ctx, db, jobID, fmt.Sprintf("exiftool init: %s", err))
		return
	}
	defer et.close()

	// Phase 1 — walk directory tree and upsert sub-collections (folders)
	if err := walkFolders(ctx, db, collectionID, rootPath); err != nil {
		_ = jobs.MarkFailed(ctx, db, jobID, err.Error())
		return
	}

	// Phase 2 — enumerate files, dispatch to worker pool
	var (
		processed int64
		errCount  int64
		mu        sync.Mutex
	)

	err = filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
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

		// Capture loop vars for goroutine
		filePath := path
		fileMime := mimeType

		pool.Submit(func() {
			if err := processFile(ctx, db, et, filePath, fileMime, dataPath); err != nil {
				slog.Error("process file", "path", filePath, "err", err)
				mu.Lock()
				errCount++
				mu.Unlock()
			} else {
				mu.Lock()
				processed++
				mu.Unlock()
			}
		})

		return nil
	})

	// Wait for all workers before post-scan cleanup
	pool.Wait()

	if err != nil {
		_ = jobs.MarkFailed(ctx, db, jobID, err.Error())
		return
	}

	// Phase 3 — mark files not seen this scan as missing
	// TODO: BUG: we need to consider sub-collections as well, this only looks at the top-level collection.
	tag, err := db.Exec(ctx,
		`UPDATE media_items
		 SET missing_since = now()
		 WHERE collection_id = $1
		   AND missing_since IS NULL
		   AND indexed_at < $2`,
		collectionID, jobStart,
	)
	if err != nil {
		_ = jobs.MarkFailed(ctx, db, jobID, err.Error())
		return
	}

	summary := fmt.Sprintf("scan complete: %d processed, %d errors, %d marked missing",
		processed, errCount, tag.RowsAffected())
	logMsg(summary)

	// TODO: Pseudo-BUG: This only updates the top-level collection again, but likely is irrelevant
	_, _ = db.Exec(ctx,
		`UPDATE collections SET last_scanned_at = now() WHERE id = $1`,
		collectionID,
	)

	_ = jobs.MarkDone(ctx, db, jobID)
}

// walkFolders upserts a sub-collection row for each directory under rootPath.
func walkFolders(ctx context.Context, db *pgxpool.Pool, rootCollectionID int, rootPath string) error {
	pathToID := map[string]int{rootPath: rootCollectionID}

	return filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == rootPath {
			return nil
		}

		parentPath := filepath.Dir(path)
		parentID, ok := pathToID[parentPath]
		if !ok {
			parentID = rootCollectionID
		}

		name := filepath.Base(path)
		var id int
		err = db.QueryRow(ctx,
			// TODO: ON CONFLICT clause seems redundant, is there a better way to do this?
			`INSERT INTO collections (parent_collection_id, name, type, root_path)
			 VALUES ($1, $2,
			     (SELECT type FROM collections WHERE id = $3),
			     $4)
			 ON CONFLICT (root_path) WHERE root_path IS NOT NULL
			     DO UPDATE SET name                 = EXCLUDED.name,
			                   parent_collection_id = EXCLUDED.parent_collection_id
			 RETURNING id`,
			parentID, name, rootCollectionID, path,
		).Scan(&id)
		if err != nil {
			return fmt.Errorf("upsert folder %s: %w", path, err)
		}

		pathToID[path] = id
		return nil
	})
}

// processFile handles a single file: change detection, EXIF, DB upsert, variant generation.
func processFile(ctx context.Context, db *pgxpool.Pool, et *exiftoolProcess,
	path, mimeType, dataPath string) error {

	stat, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	fileSize := stat.Size()

	hashPrefix, err := hashFilePrefix(path)
	if err != nil {
		return fmt.Errorf("hash: %w", err)
	}

	// Change detection — touch indexed_at and move on if unchanged
	var existingID int
	var existingSize int64
	var existingHash string
	err = db.QueryRow(ctx,
		`SELECT id, file_size_bytes, file_hash_prefix FROM media_items WHERE fs_path = $1`,
		path,
	).Scan(&existingID, &existingSize, &existingHash)

	if err == nil && existingSize == fileSize && existingHash == hashPrefix {
		_, err = db.Exec(ctx,
			`UPDATE media_items SET indexed_at = now(), missing_since = NULL WHERE id = $1`,
			existingID,
		)
		return err
	}

	// Find the sub-collection (folder) for this file
	// TODO: This is inefficient, how many times are we going to call this for the same folder?!
	dirPath := filepath.Dir(path)
	var folderCollectionID int
	err = db.QueryRow(ctx,
		`SELECT id FROM collections WHERE root_path = $1`, dirPath,
	).Scan(&folderCollectionID)
	if err != nil {
		return fmt.Errorf("fetching collection id for %s: %w", dirPath, err)
	}

	// TODO: Can image titles be stored in EXIF or IPTC data? The title being the filname is
	//       likely never what we want to do. Nobody cares what the name of the file on disk is.
	title := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))

	// Extract EXIF via persistent exiftool process
	exifData, err := et.extract(path)
	if err != nil {
		slog.Warn("exiftool extract failed", "path", path, "err", err)
		exifData = map[string]any{}
	}

	// Upsert media_item
	var itemID int
	err = db.QueryRow(ctx,
		`INSERT INTO media_items
		     (collection_id, title, fs_path, file_size_bytes, file_hash_prefix, mime_type)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (fs_path)
		     DO UPDATE SET
		         collection_id    = EXCLUDED.collection_id,
		         title            = EXCLUDED.title,
		         file_size_bytes  = EXCLUDED.file_size_bytes,
		         file_hash_prefix = EXCLUDED.file_hash_prefix,
		         indexed_at       = now(),
		         missing_since    = NULL
		 RETURNING id`,
		folderCollectionID, title, path, fileSize, hashPrefix, mimeType,
	).Scan(&itemID)
	if err != nil {
		return fmt.Errorf("upsert media_item: %w", err)
	}

	// Build photo_metadata fields from exiftool output
	rawJSON, _ := json.Marshal(exifData)

	_, err = db.Exec(ctx,
		`INSERT INTO photo_metadata
		     (media_item_id, width_px, height_px, taken_at,
		      camera_make, camera_model, lens_model,
		      shutter_speed, aperture, iso,
		      focal_length_mm, focal_length_35mm_equiv,
		      gps_lat, gps_lng, color_space, exif_raw)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		 ON CONFLICT (media_item_id) DO UPDATE SET
		     width_px                = EXCLUDED.width_px,
		     height_px               = EXCLUDED.height_px,
		     taken_at                = EXCLUDED.taken_at,
		     camera_make             = EXCLUDED.camera_make,
		     camera_model            = EXCLUDED.camera_model,
		     lens_model              = EXCLUDED.lens_model,
		     shutter_speed           = EXCLUDED.shutter_speed,
		     aperture                = EXCLUDED.aperture,
		     iso                     = EXCLUDED.iso,
		     focal_length_mm         = EXCLUDED.focal_length_mm,
		     focal_length_35mm_equiv = EXCLUDED.focal_length_35mm_equiv,
		     gps_lat                 = EXCLUDED.gps_lat,
		     gps_lng                 = EXCLUDED.gps_lng,
		     color_space             = EXCLUDED.color_space,
		     exif_raw                = EXCLUDED.exif_raw,
		     variants_generated_at   = NULL`,
		itemID,
		exifInt(exifData, "ImageWidth"), exifInt(exifData, "ImageHeight"),
		exifTime(exifData, "DateTimeOriginal"),
		exifStr(exifData, "Make"), exifStr(exifData, "Model"), exifStr(exifData, "LensModel"),
		exifStr(exifData, "ExposureTime"),
		exifFloat(exifData, "FNumber"),
		exifInt(exifData, "ISO"),
		exifFloat(exifData, "FocalLength"),
		exifFloat(exifData, "FocalLengthIn35mmFormat"),
		exifFloat(exifData, "GPSLatitude"), exifFloat(exifData, "GPSLongitude"),
		exifStr(exifData, "ColorSpace"),
		rawJSON,
	)
	if err != nil {
		return fmt.Errorf("upsert photo_metadata: %w", err)
	}

	// Generate image variants and DZI tile pyramid
	if err := imaging.GenerateAllVariants(path, dataPath, itemID); err != nil {
		return fmt.Errorf("generate variants: %w", err)
	}
	if err := imaging.GenerateDZI(path, dataPath, itemID); err != nil {
		return fmt.Errorf("generate DZI: %w", err)
	}

	// Generate tiny AVIF placeholder (replaces blurhash — no external dependency)
	placeholder, err := imaging.GeneratePlaceholder(path)
	if err != nil {
		slog.Warn("placeholder generation failed", "path", path, "err", err)
	}

	_, err = db.Exec(ctx,
		`UPDATE photo_metadata
		 SET placeholder = $2, variants_generated_at = now()
		 WHERE media_item_id = $1`,
		itemID, placeholder,
	)
	return err
}

// hashFilePrefix reads the first 64KB of a file and returns an FNV-1a hex string.
// Used for change detection — not a security hash.
func hashFilePrefix(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := fnv.New64a()
	buf := make([]byte, 64*1024)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return "", err
	}
	h.Write(buf[:n])
	return fmt.Sprintf("%x", h.Sum64()), nil
}

// ─── Exiftool stay-open process ───────────────────────────────────────────────

// exiftoolProcess wraps a persistent exiftool process using stay_open mode.
// One process is started per scan job and reused for every file — avoids
// per-file Perl startup overhead which would be ~150ms × file count.
type exiftoolProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	mu     sync.Mutex
}

func newExiftoolProcess() (*exiftoolProcess, error) {
	// TODO: Evaluate if all these args are appropriate
	cmd := exec.Command("exiftool",
		"-stay_open", "true",
		"-@", "-",
		"-common_args",
		"-json",
		"-struct",
		"-n",             // numeric output for GPS, FNumber etc.
		"-GPSLatitude#",  // force numeric GPS
		"-GPSLongitude#",
		"-largefilesupport",
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start exiftool: %w", err)
	}

	return &exiftoolProcess{
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewScanner(stdoutPipe),
	}, nil
}

// extract sends a single file path to the running exiftool process and
// returns its metadata as a raw map. Thread-safe via mutex.
func (e *exiftoolProcess) extract(path string) (map[string]any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Send filename + -execute sentinel
	if _, err := fmt.Fprintf(e.stdin, "%s\n-execute\n", path); err != nil {
		return nil, fmt.Errorf("write to exiftool: %w", err)
	}

	// Read lines until exiftool writes "{ready}" as the completion sentinel
	var sb strings.Builder
	for e.stdout.Scan() {
		line := e.stdout.Text()
		if line == "{ready}" {
			break
		}
		sb.WriteString(line)
//		sb.WriteByte('\n')
	}
	if err := e.stdout.Err(); err != nil {
		return nil, fmt.Errorf("read from exiftool: %w", err)
	}

	// exiftool -json always returns an array
	var results []map[string]any
	if err := json.Unmarshal([]byte(sb.String()), &results); err != nil {
		return nil, fmt.Errorf("parse exiftool json: %w", err)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no exiftool results for %s", path)
	}

	return results[0], nil
}

func (e *exiftoolProcess) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, _ = fmt.Fprintln(e.stdin, "-stay_open\nfalse")
	_ = e.stdin.Close()
	_ = e.cmd.Wait()
}

// ─── Exiftool field helpers ───────────────────────────────────────────────────
// These extract typed values from the raw map exiftool returns.
// All return nil/zero on missing or unparseable fields — never panic.
// TODO: Why are these returning pointers and not primitives??

func exifStr(m map[string]any, key string) *string {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	s := fmt.Sprintf("%v", v)
	return &s
}

func exifInt(m map[string]any, key string) *int {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch t := v.(type) {
	case float64:
		i := int(t)
		return &i
	case string:
		i, err := strconv.Atoi(t)
		if err != nil {
			return nil
		}
		return &i
	}
	// TODO: What if the type is already an int? Is that possible?
	return nil
}

func exifFloat(m map[string]any, key string) *float64 {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch t := v.(type) {
	case float64:
		return &t
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return nil
		}
		return &f
	}
	// TODO: Can it be an int?
	return nil
}

func exifTime(m map[string]any, key string) *time.Time {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	s, ok := v.(string)
	if !ok {
		return nil
	}
	// Exiftool DateTimeOriginal format: "2023:06:15 14:30:00"
	// TODO: Do we handle TZ at all??
	t, err := time.ParseInLocation("2006:01:02 15:04:05", s, time.Local)
	if err != nil {
		return nil
	}
	return &t
}
