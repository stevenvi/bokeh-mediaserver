package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// HandleProcessMedia is a job handler that processes a single media item:
// EXIF extraction, photo_metadata upsert, variant generation, DZI tiles,
// and placeholder creation.
//
// The worker parameter provides a persistent exiftool process scoped to the
// processing pool worker goroutine that runs this handler.
func HandleProcessMedia(worker *processingWorker, mediaPath string, dataPath string) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		if job.RelatedID == nil {
			return fmt.Errorf("process_media job %d has no related_id", job.ID)
		}
		itemID := *job.RelatedID

		mediaRepo := repository.NewMediaItemRepository(db)
		jobRepo := repository.NewJobRepository(db)

		// Fetch the media item
		relativePath, mimeType, fileHash, err := mediaRepo.GetForProcessing(ctx, itemID)
		if err != nil {
			return fmt.Errorf("fetch media item %d: %w", itemID, err)
		}

		fsPath := filepath.Join(mediaPath, relativePath)

		_ = jobRepo.UpdateProgress(ctx, job.ID, fmt.Sprintf("processing %s", fsPath))

		// Only process images for now
		if !strings.HasPrefix(mimeType, "image/") {
			_ = jobRepo.UpdateProgress(ctx, job.ID, "skipping non-image media")
			_ = jobRepo.Delete(ctx, job.ID)
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
			if err := mediaRepo.UpdateTitle(ctx, itemID, *title); err != nil {
				slog.Warn("update title from exif", "item_id", itemID, "err", err)
			}
		}

		// Strip keys that are large, redundant, or expose internal paths before storage.
		for _, key := range []string{"Directory", "SourceFile", "PreviewImage", "ThumbnailImage", "TiffMeteringImage"} {
			delete(exifData, key)
		}

		// Upsert photo_metadata
		rawJSON, _ := json.Marshal(exifData)
		// "ISO" is an exiftool composite shorthand; "PhotographicSensitivity" is the
		// actual EXIF spec tag. Some container formats (e.g. AVIF) only emit the latter.
		isoValue := utils.ExifInt(exifData, "ISO")
		if isoValue == nil {
			isoValue = utils.ExifInt(exifData, "PhotographicSensitivity")
		}

		// "FocalLengthIn35mmFormat" is an exiftool alias; "FocalLenIn35mmFilm" is the
		// canonical EXIF tag name (0xA405). Some formats only emit the latter.
		focalLength35mm := utils.ExifFloat(exifData, "FocalLengthIn35mmFormat")
		if focalLength35mm == nil {
			focalLength35mm = utils.ExifFloat(exifData, "FocalLenIn35mmFilm")
		}

		err = mediaRepo.UpsertPhotoMetadata(ctx, itemID,
			utils.ExifInt(exifData, "ImageWidth"), utils.ExifInt(exifData, "ImageHeight"),
			createdAt(fsPath, exifData),
			utils.ExifStr(exifData, "Make"), utils.ExifStr(exifData, "Model"), utils.ExifStr(exifData, "LensModel"),
			utils.ExifStr(exifData, "ExposureTime"),
			utils.ExifFloat(exifData, "FNumber"),
			isoValue,
			utils.ExifFloat(exifData, "FocalLength"),
			focalLength35mm,
			utils.ExifStr(exifData, "ColorSpace"),
			utils.ExifStr(exifData, "Description"),
			rawJSON,
		)
		if err != nil {
			return fmt.Errorf("upsert photo_metadata: %w", err)
		}

		_ = jobRepo.UpdateProgress(ctx, job.ID, "generating variants")

		// Generate image variants and DZI tile pyramid
		if err := imaging.GenerateAllVariants(fsPath, dataPath, fileHash); err != nil {
			return fmt.Errorf("generate variants: %w", err)
		}
		if err := imaging.GenerateDZI(fsPath, dataPath, fileHash); err != nil {
			return fmt.Errorf("generate DZI: %w", err)
		}

		// Generate tiny placeholder
		var placeholder *string
		if p, err := imaging.GeneratePlaceholder(fsPath); err != nil {
			slog.Warn("placeholder generation failed", "path", fsPath, "err", err)
		} else {
			placeholder = &p
		}

		if err := mediaRepo.UpdatePhotoVariants(ctx, itemID, placeholder); err != nil {
			return fmt.Errorf("update variants_generated_at: %w", err)
		}

		slog.Debug("finished processing media item", "item_id", itemID)
		_ = jobRepo.Delete(ctx, job.ID)
		return nil
	}
}

// createdAt returns the best available timestamp for a media file.
// Preference order:
//  1. DateTimeOriginal — standard EXIF capture time
//  2. CreateDate — EXIF digitized time; used by Lightroom/Photoshop AVIF exports
//  3. Earliest of FileCreateDate, FileModifyDate (exiftool), and OS mod time
func createdAt(fsPath string, exifData map[string]any) *time.Time {
	if t := utils.ExifTime(exifData, "DateTimeOriginal"); t != nil {
		return t
	}
	if t := utils.ExifTime(exifData, "CreateDate"); t != nil {
		return t
	}

	var earliest *time.Time
	consider := func(t *time.Time) {
		if t != nil && (earliest == nil || t.Before(*earliest)) {
			earliest = t
		}
	}

	parseFileDate := func(key string) *time.Time {
		v, ok := exifData[key]
		if !ok || v == nil {
			return nil
		}
		s, ok := v.(string)
		if !ok {
			return nil
		}
		t, err := time.Parse("2006:01:02 15:04:05-07:00", s)
		if err != nil {
			return nil
		}
		return &t
	}

	consider(parseFileDate("FileCreateDate"))
	consider(parseFileDate("FileModifyDate"))

	if info, err := os.Stat(fsPath); err == nil {
		mt := info.ModTime()
		consider(&mt)
	}

	return earliest
}
