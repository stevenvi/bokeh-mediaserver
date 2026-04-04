package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// homemovieFilenameRe matches home movie filenames with the pattern:
// YYYY.MM[.DD[-DD]] title
// Groups: year, month, start_day (optional), end_day (optional), description
var homemovieFilenameRe = regexp.MustCompile(`^(\d{4})\.(\d{2})(?:\.(\d{2})(?:-(\d{2}))?)?[ _](.+)$`)

// processVideoFile handles video media: extracts metadata via exiftool,
// applies home movie filename fallback, upserts video_metadata, generates
// cover art or thumbnail, and queues a transcode job if needed.
func processVideoFile(ctx context.Context, worker *processingWorker, db utils.DBTX, job *models.Job, itemID int64, fsPath, fileHash, dataPath string, transcodeBitrateKbps int, dispatcher *jobs.Dispatcher) error {
	_ = repository.JobUpdateProgress(ctx, db, job.ID, "extracting video metadata")

	// --- Step 1: exiftool extraction ---
	et, err := worker.exiftool()
	if err != nil {
		return fmt.Errorf("exiftool init: %w", err)
	}

	exifData, err := et.Extract(fsPath)
	if err != nil {
		slog.Warn("exiftool extract failed for video", "path", fsPath, "err", err)
		exifData = map[string]any{}
	}

	// Duration
	durationFloat := parseDuration(exifData)
	var durationSeconds *int
	if durationFloat != nil {
		v := int(*durationFloat)
		durationSeconds = &v
	}

	// Width/height
	width := utils.ExifInt(exifData, "ImageWidth")
	height := utils.ExifInt(exifData, "ImageHeight")

	// Bitrate — exiftool reports "AvgBitrate" as e.g. "4.50 Mbps" or "4500 kbps"
	bitrateKbps := parseVideoBitrate(exifData)

	// Video/audio codec
	videoCodec := utils.ExifStr(exifData, "VideoCodec")
	if videoCodec == nil {
		videoCodec = utils.ExifStr(exifData, "CompressorID")
	}
	audioCodec := utils.ExifStr(exifData, "AudioFormat")
	if audioCodec == nil {
		audioCodec = utils.ExifStr(exifData, "AudioCodec")
	}

	// Title
	exifTitle := utils.ExifStr(exifData, "Title")

	// Date — use the same createdAt helper which also checks ContentCreateDate
	exifDate := createdAt(fsPath, exifData)

	// Cover art bytes — extracted separately via exiftool -b
	var coverArtBytes []byte
	if _, hasPic := exifData["Picture"]; hasPic {
		coverArtBytes, _ = extractBinaryTag(fsPath, "-Picture")
	}
	if len(coverArtBytes) == 0 {
		if _, hasCover := exifData["CoverArt"]; hasCover {
			coverArtBytes, _ = extractBinaryTag(fsPath, "-CoverArt")
		}
	}

	// --- Step 2: root collection type for home movie filename fallback ---
	collType, err := repository.MediaItemRootCollectionType(ctx, db, itemID)
	if err != nil {
		slog.Warn("could not determine collection type", "item_id", itemID, "err", err)
	}

	var finalTitle *string
	var finalDate *time.Time
	var endDate *time.Time
	var author *string

	if exifTitle != nil && strings.TrimSpace(*exifTitle) != "" {
		finalTitle = exifTitle
	}
	finalDate = exifDate

	if collType == constants.CollectionTypeHomeMovie {
		basename := strings.TrimSuffix(filepath.Base(fsPath), filepath.Ext(fsPath))
		parsed := parseHomemovieFilename(basename)
		if parsed != nil {
			if finalTitle == nil && parsed.title != "" {
				finalTitle = &parsed.title
			}
			if finalDate == nil && parsed.date != nil {
				finalDate = parsed.date
			}
			if parsed.endDate != nil {
				endDate = parsed.endDate
			}
		}
	}

	// Apply title to media_items
	if finalTitle != nil && strings.TrimSpace(*finalTitle) != "" {
		if err := repository.MediaItemUpdateTitle(ctx, db, itemID, *finalTitle); err != nil {
			slog.Warn("update title from video metadata", "item_id", itemID, "err", err)
		}
	}

	// --- Step 3: upsert video_metadata ---
	if err := repository.VideoUpsert(ctx, db, itemID,
		durationSeconds, width, height, bitrateKbps,
		videoCodec, audioCodec,
		finalDate, endDate, author,
	); err != nil {
		return fmt.Errorf("upsert video_metadata: %w", err)
	}

	// --- Step 4: cover art ---
	// Crop ratio: movie posters are 2:3 (portrait); home movies use 4:3 (TV/landscape ratio).
	coverWidthRatio, coverHeightRatio := 4, 3
	if collType == constants.CollectionTypeMovie {
		coverWidthRatio, coverHeightRatio = 2, 3
	}

	// Priority: embedded metadata art > keyframe fallback. Manual covers are never overwritten.
	notManual, err := repository.VideoHasManulCover(ctx, db, itemID)
	if err == nil && notManual {
		if len(coverArtBytes) > 0 {
			_ = repository.JobUpdateProgress(ctx, db, job.ID, "generating cover art")
			if err := imaging.GenerateVideoCoverFromBytes(coverArtBytes, dataPath, fileHash, coverWidthRatio, coverHeightRatio); err != nil {
				slog.Warn("generate video cover from embedded art", "item_id", itemID, "err", err)
			}
		} else if durationSeconds != nil && *durationSeconds > 0 {
			// Only generate from keyframe if no cover exists yet
			coverPath := imaging.VariantPath(dataPath, fileHash, "cover", "avif")
			if !fileExists(coverPath) {
				_ = repository.JobUpdateProgress(ctx, db, job.ID, "generating cover from keyframe")
				if err := imaging.GenerateVideoCoverFromFrame(fsPath, dataPath, fileHash, *durationSeconds, coverWidthRatio, coverHeightRatio); err != nil {
					slog.Warn("generate video cover from keyframe", "item_id", itemID, "err", err)
				}
			}
		}
	}

	// --- Step 6: queue transcode if needed ---
	if bitrateKbps != nil && *bitrateKbps > transcodeBitrateKbps && transcodeBitrateKbps > 0 {
		// Only queue if not already transcoded
		needsTranscode, err := repository.VideoNeedsTranscode(ctx, db, itemID)
		if err != nil {
			slog.Warn("check transcode status", "item_id", itemID, "err", err)
		} else if needsTranscode {
			active, err := repository.JobIsActive(ctx, db, "transcode", itemID)
			if err == nil && !active {
				relatedType := "media_item"
				if _, err := repository.JobCreate(ctx, db, "transcode", &itemID, &relatedType); err != nil {
					slog.Warn("queue transcode job", "item_id", itemID, "err", err)
				} else {
					dispatcher.TriggerImmediately()
				}
			}
		}
	}

	slog.Debug("finished processing video file", "item_id", itemID)
	_ = repository.JobDelete(ctx, db, job.ID)
	return nil
}

// homemovieFilenameResult holds parsed fields from a home movie filename.
type homemovieFilenameResult struct {
	date    *time.Time
	endDate *time.Time
	title   string
}

// parseHomemovieFilename parses filenames like:
//
//	2023.07.15-18 Summer vacation
//	2023.07 Beach trip
//	2023 New Year
//
// Returns nil if the filename does not match the expected pattern.
func parseHomemovieFilename(basename string) *homemovieFilenameResult {
	m := homemovieFilenameRe.FindStringSubmatch(basename)
	if m == nil {
		return nil
	}
	// m[1]=year, m[2]=month, m[3]=start_day (opt), m[4]=end_day (opt), m[5]=title

	year, _ := strconv.Atoi(m[1])
	month, _ := strconv.Atoi(m[2])

	startDay := 1
	if m[3] != "" {
		startDay, _ = strconv.Atoi(m[3])
	}

	startTime := time.Date(year, time.Month(month), startDay, 0, 0, 0, 0, time.UTC)
	result := &homemovieFilenameResult{
		date:  &startTime,
		title: strings.TrimSpace(m[5]),
	}

	if m[4] != "" {
		endDay, _ := strconv.Atoi(m[4])
		endTime := time.Date(year, time.Month(month), endDay, 0, 0, 0, 0, time.UTC)
		result.endDate = &endTime
	}

	return result
}

// extractBinaryTag runs exiftool with -b to extract a binary tag (e.g. -Picture).
func extractBinaryTag(fsPath, tag string) ([]byte, error) {
	cmd := exec.Command("exiftool", "-b", tag, fsPath)
	return cmd.Output()
}

// parseVideoBitrate reads AvgBitrate from exifData and converts to kbps.
// Exiftool may return values like "4.50 Mbps", "4500 kbps", "4500000 bps".
func parseVideoBitrate(exifData map[string]any) *int {
	v, ok := exifData["AvgBitrate"]
	if !ok || v == nil {
		return nil
	}
	s, ok := v.(string)
	if !ok {
		return nil
	}
	s = strings.TrimSpace(s)

	if strings.HasSuffix(s, " Mbps") {
		f, err := strconv.ParseFloat(strings.TrimSuffix(s, " Mbps"), 64)
		if err != nil {
			return nil
		}
		kbps := int(f * 1000)
		return &kbps
	}
	if strings.HasSuffix(s, " kbps") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, " kbps"))
		if err != nil {
			return nil
		}
		return &n
	}
	if strings.HasSuffix(s, " bps") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, " bps"))
		if err != nil {
			return nil
		}
		kbps := n / 1000
		return &kbps
	}
	// Bare number: assume kbps
	if n, err := strconv.Atoi(s); err == nil {
		return &n
	}
	return nil
}

// fileExists reports whether the file at path exists on disk.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
