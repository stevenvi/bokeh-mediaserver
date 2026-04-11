package definitions

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/stevenvi/bokeh-mediaserver/internal/imaging"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	jobsutils "github.com/stevenvi/bokeh-mediaserver/internal/jobs/utils"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
)

// ScanAudioMeta describes the scan_audio sub-job type.
var ScanAudioMeta = jobs.JobMeta{
	Description: "Extract audio metadata, album art, and update library",
	TotalSteps:  1,
}

// HandleScanAudio returns a job handler that processes a single audio file.
func HandleScanAudio(mediaPath, dataPath string) jobs.JobHandler {
	return func(ctx context.Context, jc *jobs.JobContext) error {
		db, job := jc.DB, jc.Job
		if job.RelatedID == nil {
			return fmt.Errorf("scan_audio job %d has no related_id", job.ID)
		}
		itemID := *job.RelatedID

		relativePath, _, _, err := repository.MediaItemForProcessing(ctx, db, itemID)
		if err != nil {
			return fmt.Errorf("fetch media item %d: %w", itemID, err)
		}

		fsPath := filepath.Join(mediaPath, relativePath)

		// Extract tags via exiftool
		et := jc.Et
		var exifData map[string]any
		if et != nil {
			exifData, err = et.Extract(fsPath)
			if err != nil {
				slog.Warn("exiftool extract failed for audio", "path", fsPath, "err", err)
				exifData = map[string]any{}
			}
		} else {
			exifData = map[string]any{}
		}

		// Extract tag values
		title := jobsutils.ExifStr(exifData, "Title")
		artist := jobsutils.ExifStr(exifData, "Artist")
		albumArtist := jobsutils.ExifStr(exifData, "AlbumArtist")
		albumName := jobsutils.ExifStr(exifData, "Album")
		genre := jobsutils.ExifStr(exifData, "Genre")
		compilationTag := jobsutils.ExifInt(exifData, "Compilation")
		isCompilation := compilationTag != nil && *compilationTag == 1

		// Apply fallbacks
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

		// Parse track number
		var trackNumber *int16
		if t := jobsutils.ExifStr(exifData, "Track"); t != nil {
			trackNumber = parseTrackNumber(*t)
		}

		// Parse disc number
		var discNumber *int16
		if d := jobsutils.ExifStr(exifData, "PartOfSet"); d != nil {
			discNumber = parseTrackNumber(*d)
		}

		// Parse year
		var year *int16
		if y := jobsutils.ExifInt(exifData, "Year"); y != nil {
			v := int16(*y)
			year = &v
		}

		// Parse duration
		durationSeconds := parseDuration(exifData)

		// Check for embedded picture
		_, hasPicture := exifData["Picture"]
		if !hasPicture {
			_, hasPicture = exifData["CoverArt"]
		}

		// Upsert track artist
		artistID, err := repository.ArtistUpsert(ctx, db, effectiveArtist)
		if err != nil {
			slog.Warn("upsert artist", "name", effectiveArtist, "err", err)
		}

		// Upsert album artist
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

		// Upsert album
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
		if hasPicture && albumID != nil && !albumManualCover {
			needsArt := !imaging.AlbumThumbnailExists(dataPath, *albumID)
			if needsArt {
				if err := extractAndGenerateAlbumArt(et, fsPath, dataPath, *albumID); err != nil {
					slog.Warn("extract album art", "path", fsPath, "err", err)
				}
			}

			// Artist thumbnail: if manual_thumbnail = false AND no thumbnail exists
			artistRow, err := repository.ArtistGet(ctx, db, artistID)
			if err == nil && !artistRow.ManualThumbnail {
				if !imaging.ArtistThumbnailExists(dataPath, artistID) {
					if imaging.AlbumThumbnailExists(dataPath, *albumID) {
						srcPath := imaging.AlbumThumbnailPath(dataPath, *albumID, "avif")
						if err := imaging.GenerateArtistThumbnail(srcPath, dataPath, artistID); err != nil {
							slog.Warn("generate artist thumbnail from album art", "artist_id", artistID, "err", err)
						}
					}
				}
			}
		}

		slog.Debug("finished processing audio file", "item_id", itemID, "path", fsPath)
		_ = job
		return nil
	}
}

// extractAndGenerateAlbumArt uses exiftool to read embedded art from an audio file
// and generates both album thumbnail (400px) and cover (1280px).
func extractAndGenerateAlbumArt(et *jobsutils.ExiftoolProcess, fsPath, dataPath string, albumID int64) error {
	if et == nil {
		return fmt.Errorf("no exiftool process available")
	}

	imageBytes, err := et.ExtractBinary(fsPath, "Picture")
	if err != nil || len(imageBytes) == 0 {
		imageBytes, err = et.ExtractBinary(fsPath, "CoverArt")
		if err != nil {
			return fmt.Errorf("could not extract art for %s: %s", fsPath, err)
		} else if len(imageBytes) == 0 {
			return fmt.Errorf("no embedded art found in %s", fsPath)
		}
	}

	// Generate album thumbnail (400px)
	if err := imaging.GenerateAlbumThumbnailFromBytes(imageBytes, dataPath, albumID); err != nil {
		slog.Warn("generate album thumbnail", "album_id", albumID, "path", fsPath, "err", err)
	}

	// Generate album cover (1280px)
	if err := imaging.GenerateAlbumCoverFromBytes(imageBytes, dataPath, albumID); err != nil {
		slog.Warn("generate album cover", "album_id", albumID, "path", fsPath, "err", err)
	}

	return nil
}
