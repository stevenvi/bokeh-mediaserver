package definitions

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	jobsutils "github.com/stevenvi/bokeh-mediaserver/internal/jobs/utils"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// ScanVideoMeta describes the scan_video sub-job type.
var ScanVideoMeta = jobs.JobMeta{
	Description: "Extract video metadata and generate thumbnail",
	TotalSteps:  1,
}


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
		exifData := extractExif(jc.Et, fsPath, "exiftool extract failed for video")

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
			strippedName, date, endDateParsed := utils.ExtractDatePrefix(basename)
			if date != nil {
				if finalTitle == nil && strippedName != "" {
					finalTitle = &strippedName
				}
				if finalDate == nil {
					finalDate = date
				}
				if endDateParsed != nil {
					endDate = endDateParsed
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
			coverPath := imaging.VariantPath(dataPath, fileHash, "cover", "webp")
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
