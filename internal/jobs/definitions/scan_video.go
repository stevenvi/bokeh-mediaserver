package definitions

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
	jobsutils "github.com/stevenvi/bokeh-mediaserver/internal/jobs/utils"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
)

// ScanVideoMeta describes the scan_video sub-job type.
var ScanVideoMeta = jobs.JobMeta{
	Description: "Extract video metadata and generate thumbnail",
	TotalSteps:  1,
}

// homemovieFilenameRe matches home movie filenames with the pattern:
// YYYY.MM[.DD[-DD]] title
var homemovieFilenameRe = regexp.MustCompile(`^(\d{4})\.(\d{2})(?:\.(\d{2})(?:-(\d{2}))?)?[ _](.+)$`)

// HandleScanVideo returns a job handler that processes a single video file.
func HandleScanVideo(mediaPath, dataPath string, transcodeBitrateKbps int) jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		db, job := jc.DB, jc.Job
		if job.RelatedID == nil {
			return fmt.Errorf("scan_video job %d has no related_id", job.ID)
		}
		itemID := *job.RelatedID

		relativePath, _, fileHash, err := repository.MediaItemForProcessing(ctx, db, itemID)
		if err != nil {
			return fmt.Errorf("fetch media item %d: %w", itemID, err)
		}

		fsPath := filepath.Join(mediaPath, relativePath)

		// --- Step 1: exiftool extraction ---
		et := jc.Et
		var exifData map[string]any
		if et != nil {
			exifData, err = et.Extract(fsPath)
			if err != nil {
				slog.Warn("exiftool extract failed for video", "path", fsPath, "err", err)
				exifData = map[string]any{}
			}
		} else {
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
		width := jobsutils.ExifInt(exifData, "ImageWidth")
		height := jobsutils.ExifInt(exifData, "ImageHeight")

		// Bitrate
		bitrateKbps := parseVideoBitrate(exifData)

		// Video/audio codec
		videoCodec := jobsutils.ExifStr(exifData, "VideoCodec")
		if videoCodec == nil {
			videoCodec = jobsutils.ExifStr(exifData, "CompressorID")
		}
		audioCodec := jobsutils.ExifStr(exifData, "AudioFormat")
		if audioCodec == nil {
			audioCodec = jobsutils.ExifStr(exifData, "AudioCodec")
		}

		// Title
		exifTitle := jobsutils.ExifStr(exifData, "Title")

		// Date
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
		coverWidthRatio, coverHeightRatio := 4, 3
		if collType == constants.CollectionTypeMovie {
			coverWidthRatio, coverHeightRatio = 2, 3
		}

		// Priority: embedded metadata art > keyframe fallback. Manual thumbnails are never overwritten.
		// Skip cover generation entirely if a cover already exists.
		notManual, err := repository.VideoHasManualThumbnail(ctx, db, itemID)
		if err == nil && notManual {
			coverPath := imaging.VariantPath(dataPath, fileHash, "cover", "avif")
			if !fileExists(coverPath) {
				if len(coverArtBytes) > 0 {
					if err := imaging.GenerateVideoCoverFromBytes(coverArtBytes, dataPath, fileHash, coverWidthRatio, coverHeightRatio); err != nil {
						slog.Warn("generate video cover from embedded art", "item_id", itemID, "err", err)
					}
				} else if durationSeconds != nil && *durationSeconds > 0 {
					if err := imaging.GenerateVideoCoverFromFrame(fsPath, dataPath, fileHash, *durationSeconds, coverWidthRatio, coverHeightRatio); err != nil {
						slog.Warn("generate video cover from keyframe", "item_id", itemID, "err", err)
					}
				}
			}
		}

		// --- Step 5: auto-generate collection thumbnail for home movies ---
		if collType == constants.CollectionTypeHomeMovie {
			if collID, err := repository.MediaItemCollectionID(ctx, db, itemID); err == nil {
				if !imaging.CollectionThumbnailExists(dataPath, collID) {
					if err := GenerateThumbnailForCollection(ctx, db, dataPath, collID); err != nil {
						slog.Warn("auto-generate collection thumbnail for home movie", "collection_id", collID, "err", err)
					}
				}
			}
		}

		// --- Step 6: attach transcode sub-job if needed ---
		if bitrateKbps != nil && *bitrateKbps > transcodeBitrateKbps && transcodeBitrateKbps > 0 {
			needsTranscode, err := repository.VideoNeedsTranscode(ctx, db, itemID)
			if err != nil {
				slog.Warn("check transcode status", "item_id", itemID, "err", err)
			} else if needsTranscode {
				jc.AttachTranscodeSubJob(ctx, itemID)
			}
		}

		slog.Debug("finished processing video file", "item_id", itemID)
		return nil
	}
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
func parseHomemovieFilename(basename string) *homemovieFilenameResult {
	m := homemovieFilenameRe.FindStringSubmatch(basename)
	if m == nil {
		return nil
	}

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

// extractBinaryTag runs exiftool with -b to extract a binary tag.
func extractBinaryTag(fsPath, tag string) ([]byte, error) {
	cmd := exec.Command("exiftool", "-b", tag, fsPath)
	return cmd.Output()
}

// parseVideoBitrate reads AvgBitrate from exifData and converts to kbps.
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
