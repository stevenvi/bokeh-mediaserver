package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// HandleProcessMedia is a job handler that processes a single media item:
// EXIF extraction, photo_metadata upsert, variant generation, DZI tiles,
// and placeholder creation.
//
// The worker parameter provides a persistent exiftool process scoped to the
// processing pool worker goroutine that runs this handler.
func HandleProcessMedia(worker *processingWorker, mediaPath string, dataPath string) jobs.JobHandler {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		if job.RelatedID == nil {
			return fmt.Errorf("process_media job %d has no related_id", job.ID)
		}
		itemID := *job.RelatedID

		// Fetch the media item
		var relativePath, mimeType, fileHash string
		err := db.QueryRow(ctx,
			`SELECT relative_path, mime_type, file_hash FROM media_items WHERE id = $1 AND missing_since IS NULL`,
			itemID,
		).Scan(&relativePath, &mimeType, &fileHash)
		if err != nil {
			return fmt.Errorf("fetch media item %d: %w", itemID, err)
		}

		fsPath := filepath.Join(mediaPath, relativePath)

		_ = jobs.UpdateProgress(ctx, db, job.ID, fmt.Sprintf("processing %s", fsPath))

		// Only process images for now
		if !strings.HasPrefix(mimeType, "image/") {
			_ = jobs.UpdateProgress(ctx, db, job.ID, "skipping non-image media")
			_ = jobs.Delete(ctx, db, job.ID)
			return nil
		}

		// Extract EXIF
		et, err := worker.exiftool()
		if err != nil {
			return fmt.Errorf("exiftool init: %w", err)
		}

		exifData, err := et.Extract(fsPath)
		if err != nil {
			slog.Warn("exiftool extract failed", "path", fsPath, "err", err)
			exifData = map[string]any{}
		}

		// Update title from exiftool composite Title if available; fall back to filename already set at scan time.
		if title := utils.ExifStr(exifData, "Title"); title != nil && *title != "" {
			if _, err := db.Exec(ctx, `UPDATE media_items SET title = $2 WHERE id = $1`, itemID, *title); err != nil {
				slog.Warn("update title from exif", "item_id", itemID, "err", err)
			}
		}

		// Upsert photo_metadata
		rawJSON, _ := json.Marshal(exifData)
		_, err = db.Exec(ctx,
			`INSERT INTO photo_metadata
			     (media_item_id, width_px, height_px, taken_at,
			      camera_make, camera_model, lens_model,
			      shutter_speed, aperture, iso,
			      focal_length_mm, focal_length_35mm_equiv,
			      gps_lat, gps_lng, color_space, description, exif_raw)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
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
			     description             = EXCLUDED.description,
			     exif_raw                = EXCLUDED.exif_raw,
			     variants_generated_at   = NULL`,
			itemID,
			utils.ExifInt(exifData, "ImageWidth"), utils.ExifInt(exifData, "ImageHeight"),
			utils.ExifTime(exifData, "DateTimeOriginal"),
			utils.ExifStr(exifData, "Make"), utils.ExifStr(exifData, "Model"), utils.ExifStr(exifData, "LensModel"),
			utils.ExifStr(exifData, "ExposureTime"),
			utils.ExifFloat(exifData, "FNumber"),
			utils.ExifInt(exifData, "ISO"),
			utils.ExifFloat(exifData, "FocalLength"),
			utils.ExifFloat(exifData, "FocalLengthIn35mmFormat"),
			utils.ExifFloat(exifData, "GPSLatitude"), utils.ExifFloat(exifData, "GPSLongitude"),
			utils.ExifStr(exifData, "ColorSpace"),
			utils.ExifStr(exifData, "Description"),
			rawJSON,
		)
		if err != nil {
			return fmt.Errorf("upsert photo_metadata: %w", err)
		}

		_ = jobs.UpdateProgress(ctx, db, job.ID, "generating variants")

		// Generate image variants and DZI tile pyramid
		if err := imaging.GenerateAllVariants(fsPath, dataPath, fileHash); err != nil {
			return fmt.Errorf("generate variants: %w", err)
		}
		if err := imaging.GenerateDZI(fsPath, dataPath, fileHash); err != nil {
			return fmt.Errorf("generate DZI: %w", err)
		}

		// Generate tiny placeholder
		placeholder, err := imaging.GeneratePlaceholder(fsPath)
		if err != nil {
			slog.Warn("placeholder generation failed", "path", fsPath, "err", err)
		}

		_, err = db.Exec(ctx,
			`UPDATE photo_metadata
			 SET placeholder = $2, variants_generated_at = now()
			 WHERE media_item_id = $1`,
			itemID, placeholder,
		)
		if err != nil {
			return fmt.Errorf("update variants_generated_at: %w", err)
		}

		slog.Debug("finished processing media item", "item_id", itemID)
		_ = jobs.Delete(ctx, db, job.ID)
		return nil
	}
}

// HandleScanJob is a job handler for library_scan jobs.
// It performs enumeration only — heavy processing is queued as separate process_media jobs.
func HandleScanJob(mediaPath, dataPath string) jobs.JobHandler {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		if job.RelatedID == nil {
			return fmt.Errorf("library_scan job %d has no related_id", job.ID)
		}
		collectionID := *job.RelatedID

		// Fetch collection relative_path
		var relativePath string
		err := db.QueryRow(ctx,
			`SELECT COALESCE(relative_path, '') FROM collections WHERE id = $1`,
			collectionID,
		).Scan(&relativePath)
		if err != nil {
			return fmt.Errorf("fetch collection %d: %w", collectionID, err)
		}

		return RunScan(ctx, db, job.ID, collectionID, relativePath, mediaPath, dataPath)
	}
}
