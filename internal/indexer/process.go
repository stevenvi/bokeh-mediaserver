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
	"github.com/stevenvi/bokeh-mediaserver/internal/maintenance"
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
func HandleProcessMedia(worker *processingWorker, mediaPath string, dataPath string, transcodeBitrateKbps int, dispatcher *jobs.Dispatcher) func(ctx context.Context, db utils.DBTX, job *models.Job) error {
	return func(ctx context.Context, db utils.DBTX, job *models.Job) error {
		if job.RelatedID == nil {
			return fmt.Errorf("process_media job %d has no related_id", job.ID)
		}
		itemID := *job.RelatedID

		// Fetch the media item
		relativePath, mimeType, fileHash, err := repository.MediaItemForProcessing(ctx, db, itemID)
		if err != nil {
			return fmt.Errorf("fetch media item %d: %w", itemID, err)
		}

		fsPath := filepath.Join(mediaPath, relativePath)

		_ = repository.JobUpdateProgress(ctx, db, job.ID, fmt.Sprintf("processing %s", fsPath))

		// Route to the appropriate processor based on media type.
		if strings.HasPrefix(mimeType, "audio/") {
			return processAudioFile(ctx, worker, db, job, itemID, fsPath, dataPath)
		} else if strings.HasPrefix(mimeType, "image/") {
			return processImageFile(ctx, worker, db, job, itemID, fsPath, fileHash, dataPath)
		} else if strings.HasPrefix(mimeType, "video/") {
			return processVideoFile(ctx, worker, db, job, itemID, fsPath, fileHash, dataPath, transcodeBitrateKbps, dispatcher)
		} else {
			slog.Info("skipping unsupported media type", "mimeType", mimeType)
			_ = repository.JobDelete(ctx, db, job.ID)
			return nil
		}
	}
}

func processImageFile(ctx context.Context, worker *processingWorker, db utils.DBTX, job *models.Job, itemID int64, fsPath, fileHash, dataPath string) error {
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

	// EXIF ImageWidth/ImageHeight reflect the raw sensor dimensions, which
	// don't account for EXIF orientation rotation. Orientations 5-8 indicate
	// a 90° or 270° rotation, so we swap width and height to match the
	// displayed (auto-rotated) image dimensions.
	widthPx := utils.ExifInt(exifData, "ImageWidth")
	heightPx := utils.ExifInt(exifData, "ImageHeight")
	if orient := utils.ExifInt(exifData, "Orientation"); orient != nil && *orient >= 5 && *orient <= 8 {
		widthPx, heightPx = heightPx, widthPx
	}

	err = repository.PhotoUpsert(ctx, db, itemID,
		widthPx, heightPx,
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

	// Generate image variants, DZI tiles, and placeholder — but skip if they
	// already exist on disk (e.g. during a force rescan that only needs to
	// refresh EXIF metadata).
	variantsExist := imaging.VariantsExist(dataPath, fileHash)
	dziExists := imaging.DZIExists(dataPath, fileHash)

	if !variantsExist {
		_ = repository.JobUpdateProgress(ctx, db, job.ID, "generating variants")
		if err := imaging.GenerateAllVariants(fsPath, dataPath, fileHash); err != nil {
			return fmt.Errorf("generate variants: %w", err)
		}
	}
	if !dziExists {
		_ = repository.JobUpdateProgress(ctx, db, job.ID, "generating DZI tiles")
		if err := imaging.GenerateDZI(fsPath, dataPath, fileHash); err != nil {
			return fmt.Errorf("generate DZI: %w", err)
		}
	}

	// Always generate placeholder and update variants_generated_at.
	// UpsertPhotoMetadata resets variants_generated_at to NULL on every call,
	// so we must restore it even when variants already existed on disk.
	// TODO: I don't like this logic, let's circle back on it eventually
	// (Nobody wants to regenerate the placeholder for unchanged images, but
	// reimported collections whose derived files remain will need a placeholder
	// generated for their images as it is stored in the db)
	var placeholder *string
	if p, err := imaging.GeneratePlaceholder(fsPath); err != nil {
		slog.Warn("placeholder generation failed", "path", fsPath, "err", err)
	} else {
		placeholder = &p
	}
	if err := repository.PhotoUpdateVariants(ctx, db, itemID, placeholder); err != nil {
		return fmt.Errorf("update variants_generated_at: %w", err)
	}

	// Generate a collection cover from this item if the collection doesn't
	// have one yet. This ensures new collections get a cover as soon as
	// their first item is processed rather than waiting for the weekly cycle.
	// TODO: Making this extra call to the db is inefficient, we can know the collection id at this stage
	if collID, err := repository.MediaItemCollectionID(ctx, db, itemID); err == nil {
		if !imaging.CollectionCoverExists(dataPath, collID) {
			if err := maintenance.GenerateCoverForCollection(ctx, db, dataPath, collID); err != nil {
				slog.Warn("auto-generate collection cover", "collection_id", collID, "err", err)
			}
		}
	}

	slog.Debug("finished processing media item", "item_id", itemID)
	_ = repository.JobDelete(ctx, db, job.ID)
	return nil
}

// processAudioFile handles audio media: extracts tags via exiftool, upserts artist,
// album, and audio_metadata, and extracts album art.
func processAudioFile(ctx context.Context, worker *processingWorker, db utils.DBTX, job *models.Job, itemID int64, fsPath, dataPath string) error {
	_ = repository.JobUpdateProgress(ctx, db, job.ID, "extracting audio tags")

	et, err := worker.exiftool()
	if err != nil {
		return fmt.Errorf("exiftool init: %w", err)
	}

	exifData, err := et.Extract(fsPath)
	if err != nil {
		slog.Warn("exiftool extract failed for audio", "path", fsPath, "err", err)
		exifData = map[string]any{}
	}

	// Extract tag values
	title := utils.ExifStr(exifData, "Title")
	artist := utils.ExifStr(exifData, "Artist")
	albumArtist := utils.ExifStr(exifData, "AlbumArtist")
	albumName := utils.ExifStr(exifData, "Album")
	genre := utils.ExifStr(exifData, "Genre")
	compilationTag := utils.ExifInt(exifData, "Compilation")
	isCompilation := compilationTag != nil && *compilationTag == 1

	// Apply fallbacks: untagged files still get sensible defaults.
	effectiveTitle := ""
	if title != nil && strings.TrimSpace(*title) != "" {
		effectiveTitle = *title
	} else {
		base := filepath.Base(fsPath)
		effectiveTitle = strings.TrimSuffix(base, filepath.Ext(base))
	}
	title = &effectiveTitle

	effectiveArtist := "Unknown Artist"
	if artist != nil && strings.TrimSpace(*artist) != "" {
		effectiveArtist = *artist
	}

	effectiveAlbumArtist := ""
	if albumArtist != nil && strings.TrimSpace(*albumArtist) != "" {
		effectiveAlbumArtist = *albumArtist
	}

	effectiveAlbum := "Unknown Album"
	if albumName != nil && strings.TrimSpace(*albumName) != "" {
		effectiveAlbum = *albumName
	}

	// Update media item title from tag
	if err := repository.MediaItemUpdateTitle(ctx, db, itemID, effectiveTitle); err != nil {
		slog.Warn("update title from audio tag", "item_id", itemID, "err", err)
	}

	// Parse track number (exiftool may return "3" or "3/12")
	var trackNumber *int16
	if t := utils.ExifStr(exifData, "Track"); t != nil {
		trackNumber = parseTrackNumber(*t)
	}

	// Parse disc number (PartOfSet may return "1" or "1/2")
	var discNumber *int16
	if d := utils.ExifStr(exifData, "PartOfSet"); d != nil {
		discNumber = parseTrackNumber(*d)
	}

	// Parse year
	var year *int16
	if y := utils.ExifInt(exifData, "Year"); y != nil {
		v := int16(*y)
		year = &v
	}

	// Parse duration
	durationSeconds := parseDuration(exifData)

	// Check for embedded picture
	_, hasPicture := exifData["Picture"]
	if !hasPicture {
		// Some formats use CoverArt or other tag names
		_, hasPicture = exifData["CoverArt"]
	}

	// Upsert track artist (always non-empty due to fallback)
	artistID, err := repository.ArtistUpsert(ctx, db, effectiveArtist)
	if err != nil {
		slog.Warn("upsert artist", "name", effectiveArtist, "err", err)
	}

	// Upsert album artist (if present and different from track artist)
	var albumArtistID *int64
	if effectiveAlbumArtist != "" {
		if effectiveAlbumArtist == effectiveArtist {
			albumArtistID = &artistID
		} else {
			id, err := repository.ArtistUpsert(ctx, db, effectiveAlbumArtist)
			if err != nil {
				slog.Warn("upsert album artist", "name", effectiveAlbumArtist, "err", err)
			} else {
				albumArtistID = &id
			}
		}
	}

	// Resolve root collection for album scoping
	rootCollectionID, err := repository.MediaItemRootCollectionID(ctx, db, itemID)
	if err != nil {
		slog.Warn("could not resolve root collection", "item_id", itemID, "err", err)
	}

	// Upsert album. For compilations (TCMP/Compilation tag = 1), always use
	// "Various Artists" as the album artist so all tracks share one album entry
	// and the album appears under "Various Artists" in the library. For regular
	// albums, use the album artist tag if present, otherwise fall back to the
	// track artist so the album is attributed to the correct artist.
	var albumID *int64
	var albumManualCover bool
	if rootCollectionID != 0 {
		effectiveAlbumArtistID := albumArtistID
		if isCompilation {
			variousID, err := repository.ArtistUpsert(ctx, db, "Various Artists")
			if err != nil {
				slog.Warn("upsert Various Artists", "err", err)
			} else {
				effectiveAlbumArtistID = &variousID
			}
		} else if effectiveAlbumArtistID == nil {
			effectiveAlbumArtistID = &artistID
		}
		id, manualCover, err := repository.AlbumUpsert(ctx, db, effectiveAlbum, effectiveAlbumArtistID, year, genre, rootCollectionID, isCompilation)
		if err != nil {
			slog.Warn("upsert album", "name", effectiveAlbum, "err", err)
		} else {
			albumID = &id
			albumManualCover = manualCover
		}
	}

	// Store the non-nil artist ID pointer
	var artistIDPtr *int64
	if artistID != 0 {
		artistIDPtr = &artistID
	}

	// Upsert audio metadata
	if err := repository.AudioTrackUpsert(ctx, db, itemID,
		artistIDPtr, albumArtistID, albumID,
		title, trackNumber, discNumber,
		durationSeconds, genre, year,
		nil, // replay_gain_db — not extracted yet
		hasPicture,
	); err != nil {
		return fmt.Errorf("upsert audio_metadata: %w", err)
	}

	// Extract album art if not already present and not manually overridden
	if hasPicture && albumID != nil {
		if !albumManualCover && !imaging.AlbumCoverExists(dataPath, *albumID) {
			_ = repository.JobUpdateProgress(ctx, db, job.ID, "extracting album art")
			if err := extractAlbumArtBytes(et, fsPath, dataPath, *albumID); err != nil {
				slog.Warn("extract album art", "path", fsPath, "err", err)
			}
		}
	}

	slog.Debug("finished processing audio file", "item_id", itemID, "path", fsPath)
	_ = repository.JobDelete(ctx, db, job.ID)
	return nil
}

// extractAlbumArtBytes uses the worker's persistent exiftool process to read
// embedded art from an audio file (tries Picture then CoverArt) and generates
// album cover files. Avoids per-call Perl startup overhead.
func extractAlbumArtBytes(et *utils.ExiftoolProcess, fsPath, dataPath string, albumID int64) error {
	imageBytes, err := et.ExtractBinary(fsPath, "Picture")
	if err != nil || len(imageBytes) == 0 {
		// Try CoverArt tag (used by some formats)
		imageBytes, err = et.ExtractBinary(fsPath, "CoverArt")
		if err != nil || len(imageBytes) == 0 {
			return fmt.Errorf("no embedded art found in %s", fsPath)
		}
	}
	return imaging.GenerateAlbumCoverFromBytes(imageBytes, dataPath, albumID)
}
