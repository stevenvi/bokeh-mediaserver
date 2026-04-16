package definitions

import (
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	jobsutils "github.com/stevenvi/bokeh-mediaserver/internal/jobs/utils"
)

// extractExif runs exiftool on fsPath using et. On failure or if et is nil,
// logs a warning and returns an empty (non-nil) map.
func extractExif(et *jobsutils.ExiftoolProcess, fsPath, warnMsg string) map[string]any {
	if et == nil {
		return map[string]any{}
	}
	data, err := et.Extract(fsPath)
	if err != nil {
		slog.Warn(warnMsg, "path", fsPath, "err", err)
		return map[string]any{}
	}
	return data
}

// parseTrackNumber parses a track/disc string like "3", "3/12", or "03" into a *int16.
func parseTrackNumber(s string) *int16 {
	s = strings.TrimSpace(s)
	if idx := strings.Index(s, "/"); idx >= 0 {
		s = s[:idx]
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 0 {
		return nil
	}
	v := int16(n)
	return &v
}

// parseDuration extracts the duration from exiftool data. Exiftool returns duration
// in various formats depending on the container: "225.34" (seconds as float),
// "3:45" (M:SS), "0:03:45" (H:MM:SS), or "225.34 s" (with unit suffix).
func parseDuration(exifData map[string]any) *float64 {
	v, ok := exifData["Duration"]
	if !ok || v == nil {
		return nil
	}

	switch t := v.(type) {
	case float64:
		if t > 0 {
			return &t
		}
		return nil
	case string:
		return parseDurationString(t)
	}
	return nil
}

func parseDurationString(s string) *float64 {
	s = strings.TrimSpace(s)
	// Strip trailing unit suffix like " s" or " sec"
	s = strings.TrimSuffix(s, " s")
	s = strings.TrimSuffix(s, " sec")
	s = strings.TrimSpace(s)

	// Try as a plain number first
	if f, err := strconv.ParseFloat(s, 64); err == nil && f > 0 {
		return &f
	}

	// Try as H:MM:SS or M:SS
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return nil
	}

	var total float64
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil
		}
		total += f * math.Pow(60, float64(len(parts)-1-i))
	}
	if total > 0 {
		return &total
	}
	return nil
}

// createdAt returns the best available timestamp for a media file.
// Preference order:
//  1. DateTimeOriginal — standard EXIF capture time
//  2. CreateDate — EXIF digitized time; used by Lightroom/Photoshop AVIF exports
//  3. ContentCreateDate — used by some video containers
//  4. Earliest of FileCreateDate, FileModifyDate (exiftool), and OS mod time
func createdAt(fsPath string, exifData map[string]any) *time.Time {
	if t := jobsutils.ExifTimeWithOffset(exifData, "DateTimeOriginal", "OffsetTimeOriginal"); t != nil {
		return t
	}
	if t := jobsutils.ExifTimeWithOffset(exifData, "CreateDate", "OffsetTimeDigitized"); t != nil {
		return t
	}
	if t := jobsutils.ExifTimeWithOffset(exifData, "ContentCreateDate", ""); t != nil {
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
