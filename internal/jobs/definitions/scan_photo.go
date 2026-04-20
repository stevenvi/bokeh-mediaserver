package definitions

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	jobsutils "github.com/stevenvi/bokeh-mediaserver/internal/jobs/utils"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
)

// ScanPhotoMeta describes the scan_photo sub-job type.
var ScanPhotoMeta = jobs.JobMeta{
	Description: "Extract photo metadata and generate image variants",
	TotalSteps:  1,
}

// HandleScanPhoto returns a job handler that processes a single photo file.
func HandleScanPhoto(mediaPath, dataPath string) jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		db, job := jc.DB, jc.Job
		if job.RelatedID == nil {
			return fmt.Errorf("scan_photo job %d has no related_id", job.ID)
		}
		itemID := *job.RelatedID

		relativePath, _, fileHash, err := repository.MediaItemForProcessing(ctx, db, itemID)
		if err != nil {
			return fmt.Errorf("fetch media item %d: %w", itemID, err)
		}

		fsPath := filepath.Join(mediaPath, relativePath)

		// Extract EXIF
		exifData := extractExif(jc.Et, fsPath, "exiftool extract failed")

		// Update title from exiftool composite Title if available
		if title := jobsutils.ExifStr(exifData, "Title"); title != nil && *title != "" {
			if err := repository.MediaItemUpdateTitle(ctx, db, itemID, *title); err != nil {
				slog.Warn("update title from exif", "item_id", itemID, "err", err)
			}
		}

		// Strip keys that are large, redundant, or expose internal paths before storage.
		for _, key := range []string{"Directory", "SourceFile", "PreviewImage", "ThumbnailImage", "TiffMeteringImage"} {
			delete(exifData, key)
		}

		// Upsert photo_metadata
		rawJSON, _ := json.Marshal(exifData)
		isoValue := jobsutils.ExifInt(exifData, "ISO")
		if isoValue == nil {
			isoValue = jobsutils.ExifInt(exifData, "PhotographicSensitivity")
		}

		focalLength35mm := jobsutils.ExifFloat(exifData, "FocalLengthIn35mmFormat")
		if focalLength35mm == nil {
			focalLength35mm = jobsutils.ExifFloat(exifData, "FocalLenIn35mmFilm")
		}

		lensInfo := jobsutils.ExifStr(exifData, "Lens")
		if lensInfo == nil {
			lensInfo = jobsutils.ExifStr(exifData, "LensModel")
			if lensInfo == nil {
				lensInfo = jobsutils.ExifStr(exifData, "LensInfo")
				if lensInfo == nil {
					lensInfo = jobsutils.ExifStr(exifData, "LensID")
				}
			}
		}

		widthPx := jobsutils.ExifInt(exifData, "ImageWidth")
		heightPx := jobsutils.ExifInt(exifData, "ImageHeight")
		if orient := jobsutils.ExifInt(exifData, "Orientation"); orient != nil && *orient >= 5 && *orient <= 8 {
			widthPx, heightPx = heightPx, widthPx
		}

		err = repository.PhotoUpsert(ctx, db, itemID,
			widthPx, heightPx,
			createdAt(fsPath, exifData),
			jobsutils.ExifStr(exifData, "Make"),
			jobsutils.ExifStr(exifData, "Model"),
			lensInfo,
			jobsutils.ExifStr(exifData, "ExposureTime"),
			jobsutils.ExifFloat(exifData, "FNumber"),
			isoValue,
			jobsutils.ExifFloat(exifData, "FocalLength"),
			focalLength35mm,
			jobsutils.ExifStr(exifData, "ColorSpace"),
			jobsutils.ExifStr(exifData, "Description"),
			rawJSON,
		)
		if err != nil {
			return fmt.Errorf("upsert photo_metadata: %w", err)
		}

		// Generate WebP variants if not already present.
		// All derived files for the item are written to a temp directory and
		// moved into place atomically, so existing data is never partially overwritten.
		// DZI tiles are generated on-demand when first requested.
		if !imaging.VariantsExist(dataPath, fileHash) {
			if err := imaging.GenerateItemVariants(fsPath, dataPath, fileHash); err != nil {
				return fmt.Errorf("generate variants: %w", err)
			}
		}

		if err := repository.PhotoUpdateVariants(ctx, db, itemID); err != nil {
			return fmt.Errorf("update variants_generated_at: %w", err)
		}

		// Auto-generate collection thumbnail if none exists
		if collID, err := repository.MediaItemCollectionID(ctx, db, itemID); err == nil {
			if !imaging.CollectionThumbnailExists(dataPath, collID) {
				if err := GenerateThumbnailForCollection(ctx, db, dataPath, collID); err != nil {
					slog.Warn("auto-generate collection thumbnail", "collection_id", collID, "err", err)
				}
			}
		}

		slog.Debug("finished processing photo item", "item_id", itemID)
		return nil
	}
}
