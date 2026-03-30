package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type MediaItemRepository struct {
	db utils.DBTX
}

func NewMediaItemRepository(db utils.DBTX) *MediaItemRepository {
	return &MediaItemRepository{db: db}
}

// GetByID returns a media item by ID with photo metadata, enforcing that the given user
// has access to the collection it belongs to. Returns an error (→ 404) if the item does
// not exist, is hidden, or the user lacks collection_access.
func (r *MediaItemRepository) GetByID(ctx context.Context, id, userID int64) (*models.MediaItemView, error) {
	var item models.MediaItemView
	var photo models.PhotoMetadata
	var hasPhoto bool
	err := r.db.QueryRow(ctx,
		`SELECT m.id, m.title, m.mime_type, m.ordinal,
		        pm.media_item_id IS NOT NULL,
		        pm.width_px, pm.height_px, pm.created_at,
		        pm.camera_make, pm.camera_model, pm.lens_model,
		        pm.shutter_speed, pm.aperture, pm.iso,
		        pm.focal_length_mm, pm.focal_length_35mm_equiv,
		        pm.color_space,
		        pm.placeholder
		 FROM media_items m
		 JOIN collection_access ca ON ca.collection_id = m.collection_id AND ca.user_id = $2
		 LEFT JOIN photo_metadata pm ON pm.media_item_id = m.id
		 WHERE m.id = $1 AND m.hidden_at IS NULL`,
		id, userID,
	).Scan(&item.ID, &item.Title, &item.MimeType, &item.Ordinal,
		&hasPhoto,
		&photo.WidthPx, &photo.HeightPx, &photo.CreatedAt,
		&photo.CameraMake, &photo.CameraModel, &photo.LensModel,
		&photo.ShutterSpeed, &photo.Aperture, &photo.ISO,
		&photo.FocalLengthMM, &photo.FocalLength35mmEquiv,
		&photo.ColorSpace,
		&photo.Placeholder,
	)
	if err != nil {
		return nil, err
	}
	if hasPhoto {
		item.Photo = &photo
	}
	return &item, nil
}

// GetForProcessing returns fields needed for media processing. Only returns non-missing items.
func (r *MediaItemRepository) GetForProcessing(ctx context.Context, id int64) (relativePath, mimeType, fileHash string, err error) {
	err = r.db.QueryRow(ctx,
		`SELECT relative_path, mime_type, file_hash FROM media_items WHERE id = $1 AND missing_since IS NULL`,
		id,
	).Scan(&relativePath, &mimeType, &fileHash)
	return
}

// GetFileHashAndPath returns the content hash and relative filesystem path for a media item,
// enforcing that the given user has collection_access to the root ancestor collection.
func (r *MediaItemRepository) GetFileHashAndPath(ctx context.Context, id, userID int64) (hash, relativePath string, err error) {
	err = r.db.QueryRow(ctx,
		`SELECT m.file_hash, m.relative_path FROM media_items m
		 JOIN collections c ON c.id = m.collection_id
		 WHERE m.id = $1 AND m.hidden_at IS NULL
		   AND EXISTS (
		       SELECT 1 FROM collection_access ca
		       WHERE ca.collection_id = c.root_collection_id
		         AND ca.user_id = $2
		   )`,
		id, userID,
	).Scan(&hash, &relativePath)
	return
}

// UpdateTitle sets the title for a media item.
func (r *MediaItemRepository) UpdateTitle(ctx context.Context, id int64, title string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE media_items SET title = $2 WHERE id = $1`, id, title,
	)
	return err
}

// ListByCollectionPaginated returns paginated media items for a collection.
// Access is checked against the root ancestor collection, so sub-collections
// are accessible if the user has access to the top-level parent.
func (r *MediaItemRepository) ListByCollectionPaginated(ctx context.Context, collectionID int64, userID int64, limit, offset int) ([]models.MediaItemView, error) {
	rows, err := r.db.Query(ctx,
		`SELECT m.id, m.title, m.mime_type, m.ordinal,
		        pm.placeholder, pm.variants_generated_at
		 FROM media_items m
		 LEFT JOIN photo_metadata pm ON pm.media_item_id = m.id
		 JOIN collections c ON c.id = m.collection_id
		 WHERE m.collection_id = $1 AND m.missing_since IS NULL AND m.hidden_at IS NULL
		   AND EXISTS (
		       SELECT 1 FROM collection_access ca
		       WHERE ca.collection_id = c.root_collection_id
		         AND ca.user_id = $4
		   )
		 ORDER BY m.ordinal ASC NULLS LAST, m.title ASC
		 LIMIT $2 OFFSET $3`,
		collectionID, limit, offset, userID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (models.MediaItemView, error) {
		var item models.MediaItemView
		var placeholder *string
		var variantsGeneratedAt *time.Time
		err := row.Scan(&item.ID, &item.Title, &item.MimeType, &item.Ordinal,
			&placeholder, &variantsGeneratedAt)
		if err != nil {
			return item, err
		}
		if placeholder != nil || variantsGeneratedAt != nil {
			item.Photo = &models.PhotoMetadata{
				Placeholder:         placeholder,
				VariantsGeneratedAt: variantsGeneratedAt,
			}
		}
		return item, nil
	})
}

// Upsert inserts or updates a media item, returning the ID and whether the file was unchanged.
// Uses a CTE to capture pre-existing state for change detection in a single round-trip.
func (r *MediaItemRepository) Upsert(ctx context.Context, collectionID int64, title, relativePath string, fileSizeBytes int64, fileHash, mimeType string) (id int64, wasUnchanged bool, err error) {
	err = r.db.QueryRow(ctx, `
		WITH prev AS (
			SELECT id, file_size_bytes, file_hash
			FROM media_items
			WHERE relative_path = $3
		),
		upserted AS (
			INSERT INTO media_items (collection_id, title, relative_path, file_size_bytes, file_hash, mime_type)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (relative_path, collection_id) DO UPDATE SET
				indexed_at      = now(),
				missing_since   = NULL,
				collection_id   = EXCLUDED.collection_id,
				file_size_bytes = EXCLUDED.file_size_bytes,
				file_hash       = EXCLUDED.file_hash,
				mime_type       = EXCLUDED.mime_type
			RETURNING id
		)
		SELECT
			u.id,
			(p.id IS NOT NULL AND p.file_size_bytes = $4 AND p.file_hash = $5) AS was_unchanged
		FROM upserted u
		LEFT JOIN prev p ON true`,
		collectionID, title, relativePath, fileSizeBytes, fileHash, mimeType,
	).Scan(&id, &wasUnchanged)
	return
}

// Delete removes a media item by ID. Note that it will come right back on next scan,
// so hiding is likely what you will actually want to be doing.
func (r *MediaItemRepository) Delete(ctx context.Context, id int64) error {
	_, err := r.db.Exec(ctx, `DELETE FROM media_items WHERE id = $1`, id)
	return err
}

// Marks an item as hidden or visible in the database. This is effectively a soft delete,
// which prevents removed items from showing back up after a collection scan.
func (r *MediaItemRepository) SetHidden(ctx context.Context, id int64, hidden bool) error {
	var err error
	if hidden {
		_, err = r.db.Exec(ctx, `UPDATE media_items SET hidden_at = now() WHERE id = $1`, id)
	} else {
		_, err = r.db.Exec(ctx, `UPDATE media_items SET hidden_at = NULL WHERE id = $1`, id)
	}
	return err
}

// FindHashesExisting returns the subset of provided hashes that exist in the DB.
func (r *MediaItemRepository) FindHashesExisting(ctx context.Context, hashes []string) (map[string]struct{}, error) {
	rows, err := r.db.Query(ctx,
		`SELECT file_hash FROM media_items WHERE file_hash = ANY($1)`,
		hashes,
	)
	if err != nil {
		return nil, err
	}
	found, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	existing := make(map[string]struct{}, len(found))
	for _, h := range found {
		existing[h] = struct{}{}
	}
	return existing, nil
}

// CountPendingVariants returns the number of photo_metadata rows awaiting variant generation.
func (r *MediaItemRepository) CountPendingVariants(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM photo_metadata
		 WHERE variants_generated_at IS NULL
		 AND media_item_id IN (
		     SELECT id FROM media_items WHERE missing_since IS NULL AND hidden_at IS NULL
		 )`,
	).Scan(&count)
	return count, err
}

// UpsertPhotoMetadata inserts or updates photo metadata for a media item.
func (r *MediaItemRepository) UpsertPhotoMetadata(ctx context.Context, itemID int64,
	widthPx, heightPx *int,
	createdAt *time.Time,
	cameraMake, cameraModel, lensModel, shutterSpeed *string,
	aperture *float64,
	iso *int,
	focalLengthMM, focalLength35mmEquiv *float64,
	colorSpace, description *string,
	exifRaw json.RawMessage,
) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO photo_metadata
		     (media_item_id, width_px, height_px, created_at,
		      camera_make, camera_model, lens_model,
		      shutter_speed, aperture, iso,
		      focal_length_mm, focal_length_35mm_equiv,
		      color_space, description, exif_raw)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		 ON CONFLICT (media_item_id) DO UPDATE SET
		     width_px                = EXCLUDED.width_px,
		     height_px               = EXCLUDED.height_px,
		     created_at              = EXCLUDED.created_at,
		     camera_make             = EXCLUDED.camera_make,
		     camera_model            = EXCLUDED.camera_model,
		     lens_model              = EXCLUDED.lens_model,
		     shutter_speed           = EXCLUDED.shutter_speed,
		     aperture                = EXCLUDED.aperture,
		     iso                     = EXCLUDED.iso,
		     focal_length_mm         = EXCLUDED.focal_length_mm,
		     focal_length_35mm_equiv = EXCLUDED.focal_length_35mm_equiv,
		     color_space             = EXCLUDED.color_space,
		     description             = EXCLUDED.description,
		     exif_raw                = EXCLUDED.exif_raw,
		     variants_generated_at   = NULL`,
		itemID,
		widthPx, heightPx, createdAt,
		cameraMake, cameraModel, lensModel,
		shutterSpeed, aperture, iso,
		focalLengthMM, focalLength35mmEquiv,
		colorSpace, description, exifRaw,
	)
	return err
}

// UpdatePhotoVariants marks variants as generated and stores the placeholder.
func (r *MediaItemRepository) UpdatePhotoVariants(ctx context.Context, itemID int64, placeholder *string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE photo_metadata
		 SET placeholder = $2, variants_generated_at = now()
		 WHERE media_item_id = $1`,
		itemID, placeholder,
	)
	return err
}

// GetExifRaw returns the raw EXIF JSON for a media item.
func (r *MediaItemRepository) GetExifRaw(ctx context.Context, itemID int64, userID int64) ([]byte, error) {
	var raw []byte
	err := r.db.QueryRow(ctx,
		`SELECT pm.exif_raw
		 FROM photo_metadata pm
		 JOIN media_items m ON m.id = pm.media_item_id AND m.hidden_at IS NULL
		 JOIN collections c ON c.id = m.collection_id
		 JOIN collection_access ca ON ca.collection_id = c.id AND ca.user_id = $2
		 WHERE pm.media_item_id = $1`, itemID, userID).Scan(&raw)
	return raw, err
}

// StaleItem represents a media item that has been missing long enough to prune.
type StaleItem struct {
	ID   int64
	Hash string
}

// ListStaleItems returns items missing for more than 90 days.
func (r *MediaItemRepository) ListStaleItems(ctx context.Context) ([]StaleItem, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, file_hash FROM media_items
		 WHERE missing_since IS NOT NULL
		   AND missing_since < now() - interval '90 days'`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[StaleItem])
}

// ListHashesByCollection returns all file hashes for non-missing items in a collection
// and its sub-collections (recursive).
func (r *MediaItemRepository) ListHashesByCollection(ctx context.Context, collectionID int64) ([]string, error) {
	rows, err := r.db.Query(ctx,
		`WITH RECURSIVE tree AS (
			SELECT id FROM collections WHERE id = $1
			UNION ALL
			SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
		)
		SELECT DISTINCT mi.file_hash
		FROM media_items mi
		JOIN tree t ON mi.collection_id = t.id
		WHERE mi.missing_since IS NULL`,
		collectionID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// ClearVariantsGenerated sets variants_generated_at to NULL for all items in a
// collection and its sub-collections, so they are re-queued for processing.
func (r *MediaItemRepository) ClearVariantsGenerated(ctx context.Context, collectionID int64) error {
	_, err := r.db.Exec(ctx,
		`WITH RECURSIVE tree AS (
			SELECT id FROM collections WHERE id = $1
			UNION ALL
			SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
		)
		UPDATE photo_metadata SET variants_generated_at = NULL, placeholder = NULL
		WHERE media_item_id IN (
			SELECT mi.id FROM media_items mi JOIN tree t ON mi.collection_id = t.id
		)`,
		collectionID,
	)
	return err
}

// GetRandomItemHashWithVariants picks a random item with generated variants,
// starting with the collection's direct items and expanding one depth level at a time.
// It searches up to 8 nested levels; returns pgx.ErrNoRows if nothing is found.
func (r *MediaItemRepository) GetRandomItemHashWithVariants(ctx context.Context, collectionID int64) (string, error) {
	collectionIDs := []int64{collectionID}

	for depth := 0; depth <= 8; depth++ {
		if len(collectionIDs) == 0 {
			break
		}

		var hash string
		err := r.db.QueryRow(ctx,
			`SELECT mi.file_hash
			 FROM media_items mi
			 JOIN photo_metadata pm ON pm.media_item_id = mi.id
			 WHERE mi.collection_id = ANY($1)
			   AND mi.missing_since IS NULL
			   AND mi.hidden_at IS NULL
			   AND pm.variants_generated_at IS NOT NULL
			 ORDER BY RANDOM()
			 LIMIT 1`,
			collectionIDs,
		).Scan(&hash)
		if err == nil {
			return hash, nil
		}
		if err != pgx.ErrNoRows {
			return "", err
		}

		// Nothing at this depth — fetch the next level of children.
		rows, err := r.db.Query(ctx,
			`SELECT id FROM collections WHERE parent_collection_id = ANY($1)`,
			collectionIDs,
		)
		if err != nil {
			return "", err
		}
		children, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return "", err
		}
		collectionIDs = children
	}

	return "", pgx.ErrNoRows
}

// GetCollectionID returns the collection_id for a media item.
func (r *MediaItemRepository) GetCollectionID(ctx context.Context, itemID int64) (int64, error) {
	var collID int64
	err := r.db.QueryRow(ctx,
		`SELECT collection_id FROM media_items WHERE id = $1`,
		itemID,
	).Scan(&collID)
	return collID, err
}

// GetRootCollectionID returns the root collection ID for the collection containing
// the given media item.
func (r *MediaItemRepository) GetRootCollectionID(ctx context.Context, itemID int64) (int64, error) {
	var rootID int64
	err := r.db.QueryRow(ctx,
		`SELECT c.root_collection_id
		 FROM media_items mi
		 JOIN collections c ON c.id = mi.collection_id
		 WHERE mi.id = $1`,
		itemID,
	).Scan(&rootID)
	return rootID, err
}

// UpsertAudioMetadata inserts or updates audio metadata for a media item.
func (r *MediaItemRepository) UpsertAudioMetadata(ctx context.Context, itemID int64,
	artistID, albumArtistID, albumID *int64,
	title *string,
	trackNumber, discNumber *int16,
	durationSeconds *float64,
	genre *string,
	year *int16,
	replayGainDB *float64,
	hasEmbeddedArt bool,
) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO audio_metadata
		     (media_item_id, artist_id, album_artist_id, album_id, title,
		      track_number, disc_number, duration_seconds,
		      genre, year, replay_gain_db, has_embedded_art, processed_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now())
		 ON CONFLICT (media_item_id) DO UPDATE SET
		     artist_id        = EXCLUDED.artist_id,
		     album_artist_id  = EXCLUDED.album_artist_id,
		     album_id         = EXCLUDED.album_id,
		     title            = EXCLUDED.title,
		     track_number     = EXCLUDED.track_number,
		     disc_number      = EXCLUDED.disc_number,
		     duration_seconds = EXCLUDED.duration_seconds,
		     genre            = EXCLUDED.genre,
		     year             = EXCLUDED.year,
		     replay_gain_db   = EXCLUDED.replay_gain_db,
		     has_embedded_art = EXCLUDED.has_embedded_art,
		     processed_at     = now()`,
		itemID, artistID, albumArtistID, albumID, title,
		trackNumber, discNumber, durationSeconds,
		genre, year, replayGainDB, hasEmbeddedArt,
	)
	return err
}

// ListTracksByAlbum returns all tracks for an album ordered by disc and track number.
// Access is verified against the album's root_collection_id.
func (r *MediaItemRepository) ListTracksByAlbum(ctx context.Context, albumID, userID int64) ([]models.TrackView, error) {
	rows, err := r.db.Query(ctx,
		`SELECT m.id, m.title, am.track_number, am.disc_number,
		        am.duration_seconds, a.name, m.mime_type
		 FROM audio_metadata am
		 JOIN media_items m ON m.id = am.media_item_id
		 LEFT JOIN artists a ON a.id = am.artist_id
		 JOIN audio_albums al ON al.id = am.album_id
		 WHERE am.album_id = $1
		   AND m.missing_since IS NULL AND m.hidden_at IS NULL
		   AND EXISTS (
		       SELECT 1 FROM collection_access ca
		       WHERE ca.collection_id = al.root_collection_id
		         AND ca.user_id = $2
		   )
		 ORDER BY am.disc_number ASC NULLS LAST, am.track_number ASC NULLS LAST, m.title ASC`,
		albumID, userID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.TrackView])
}

// GetAudioStreamInfo returns fields needed to stream an audio file, with access check.
func (r *MediaItemRepository) GetAudioStreamInfo(ctx context.Context, itemID int64, userID int64) (relativePath, mimeType string, err error) {
	err = r.db.QueryRow(ctx,
		`SELECT m.relative_path, m.mime_type FROM media_items m
		 JOIN collections c ON c.id = m.collection_id
		 WHERE m.id = $1 AND m.hidden_at IS NULL AND m.missing_since IS NULL
		   AND EXISTS (
		       SELECT 1 FROM collection_access ca
		       WHERE ca.collection_id = c.root_collection_id
		         AND ca.user_id = $2
		   )`,
		itemID, userID,
	).Scan(&relativePath, &mimeType)
	return
}

// GetRootCollectionType returns the type of the root collection that contains the
// given media item (e.g. "video:movie", "video:home_movie").
func (r *MediaItemRepository) GetRootCollectionType(ctx context.Context, itemID int64) (string, error) {
	var colType string
	err := r.db.QueryRow(ctx,
		`SELECT rc.type
		 FROM media_items mi
		 JOIN collections c  ON c.id  = mi.collection_id
		 JOIN collections rc ON rc.id = c.root_collection_id
		 WHERE mi.id = $1`,
		itemID,
	).Scan(&colType)
	return colType, err
}

// UpsertVideoMetadata inserts or updates video metadata for a media item.
// Does NOT reset transcoded_at on re-process — transcoding is expensive and
// must be explicitly re-triggered by clearing the field.
func (r *MediaItemRepository) UpsertVideoMetadata(ctx context.Context, itemID int64,
	durationSeconds *int,
	width, height *int,
	bitrateKbps *int,
	videoCodec, audioCodec *string,
	date, endDate *time.Time,
	author *string,
) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO video_metadata
		    (media_item_id, duration_seconds, width, height, bitrate_kbps,
		     video_codec, audio_codec, date, end_date, author)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (media_item_id) DO UPDATE SET
		    duration_seconds = EXCLUDED.duration_seconds,
		    width             = EXCLUDED.width,
		    height            = EXCLUDED.height,
		    bitrate_kbps      = EXCLUDED.bitrate_kbps,
		    video_codec       = EXCLUDED.video_codec,
		    audio_codec       = EXCLUDED.audio_codec,
		    date              = EXCLUDED.date,
		    end_date          = EXCLUDED.end_date,
		    author            = EXCLUDED.author`,
		itemID, durationSeconds, width, height, bitrateKbps,
		videoCodec, audioCodec, date, endDate, author,
	)
	return err
}

// GetVideoStreamInfo returns fields needed to stream a video file, with access check.
// Returns relativePath, mimeType, and file_hash.
func (r *MediaItemRepository) GetVideoStreamInfo(ctx context.Context, itemID int64, userID int64) (relativePath, mimeType, hash string, err error) {
	err = r.db.QueryRow(ctx,
		`SELECT m.relative_path, m.mime_type, m.file_hash FROM media_items m
		 JOIN collections c ON c.id = m.collection_id
		 WHERE m.id = $1 AND m.hidden_at IS NULL AND m.missing_since IS NULL
		   AND EXISTS (
		       SELECT 1 FROM collection_access ca
		       WHERE ca.collection_id = c.root_collection_id
		         AND ca.user_id = $2
		   )`,
		itemID, userID,
	).Scan(&relativePath, &mimeType, &hash)
	return
}

// GetVideoMetadataWithBookmark fetches video_metadata for an item, with the
// requesting user's bookmark position populated if one exists.
func (r *MediaItemRepository) GetVideoMetadataWithBookmark(ctx context.Context, itemID int64, userID int64) (*models.VideoMetadata, error) {
	var m models.VideoMetadata
	err := r.db.QueryRow(ctx,
		`SELECT vm.duration_seconds, vm.width, vm.height, vm.bitrate_kbps,
		        vm.video_codec, vm.audio_codec, vm.transcoded_at,
		        vm.date, vm.end_date, vm.author, vm.manual_cover,
		        vb.position_seconds
		 FROM video_metadata vm
		 LEFT JOIN video_bookmarks vb
		     ON vb.media_item_id = vm.media_item_id AND vb.user_id = $2
		 WHERE vm.media_item_id = $1`,
		itemID, userID,
	).Scan(
		&m.DurationSeconds, &m.Width, &m.Height, &m.BitrateKbps,
		&m.VideoCodec, &m.AudioCodec, &m.TranscodedAt,
		&m.Date, &m.EndDate, &m.Author, &m.ManualCover,
		&m.BookmarkSeconds,
	)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// UpsertVideoBookmark inserts or updates a user's playback position for a video.
func (r *MediaItemRepository) UpsertVideoBookmark(ctx context.Context, userID, itemID int64, positionSeconds int) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO video_bookmarks (user_id, media_item_id, position_seconds)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (user_id, media_item_id) DO UPDATE SET
		     position_seconds = EXCLUDED.position_seconds,
		     last_watched_at  = NOW()`,
		userID, itemID, positionSeconds,
	)
	return err
}

// DeleteVideoBookmark removes a user's bookmark for a video item.
func (r *MediaItemRepository) DeleteVideoBookmark(ctx context.Context, userID, itemID int64) error {
	_, err := r.db.Exec(ctx,
		`DELETE FROM video_bookmarks WHERE user_id = $1 AND media_item_id = $2`,
		userID, itemID,
	)
	return err
}

// VideoIntegrityItem holds the fields needed by the integrity checker for a single video item.
type VideoIntegrityItem struct {
	ItemID         int64
	FileHash       string
	TranscodedAt   *time.Time
	CollectionType string
}

// ListVideoItemsForIntegrity returns all non-missing video items with their
// file hash, transcoded_at timestamp, and root collection type.
func (r *MediaItemRepository) ListVideoItemsForIntegrity(ctx context.Context) ([]VideoIntegrityItem, error) {
	rows, err := r.db.Query(ctx,
		`SELECT vm.media_item_id, mi.file_hash, vm.transcoded_at, c.type AS collection_type
		 FROM video_metadata vm
		 JOIN media_items mi ON mi.id = vm.media_item_id
		 JOIN collections c ON c.id = mi.collection_id
		 WHERE mi.missing_since IS NULL`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[VideoIntegrityItem])
}

// ClearTranscodedAt sets transcoded_at to NULL for a video item, allowing it
// to be re-transcoded.
func (r *MediaItemRepository) ClearTranscodedAt(ctx context.Context, itemID int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE video_metadata SET transcoded_at = NULL WHERE media_item_id = $1`,
		itemID,
	)
	return err
}

// IsVideoManualCover returns true if manual_cover is false for the item,
// meaning auto-generated cover art is appropriate. Returns false (not manual)
// if no row exists yet (first processing run).
func (r *MediaItemRepository) IsVideoManualCover(ctx context.Context, itemID int64) (bool, error) {
	var manualCover bool
	err := r.db.QueryRow(ctx,
		`SELECT manual_cover FROM video_metadata WHERE media_item_id = $1`,
		itemID,
	).Scan(&manualCover)
	if err != nil {
		// Row may not exist yet (first run); treat as not manual
		return false, nil
	}
	return !manualCover, nil
}

// VideoNeedsTranscode returns true if the item has not yet been transcoded
// (transcoded_at IS NULL in video_metadata).
func (r *MediaItemRepository) VideoNeedsTranscode(ctx context.Context, itemID int64) (bool, error) {
	var count int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM video_metadata
		 WHERE media_item_id = $1 AND transcoded_at IS NULL`,
		itemID,
	).Scan(&count)
	return count > 0, err
}

// GetVideoMetaForTranscode returns the bitrate and transcoded_at fields for a
// media item's video_metadata. Used by the transcoder to decide whether to proceed.
func (r *MediaItemRepository) GetVideoMetaForTranscode(ctx context.Context, itemID int64) (*models.VideoMetadata, error) {
	var m models.VideoMetadata
	err := r.db.QueryRow(ctx,
		`SELECT bitrate_kbps, transcoded_at FROM video_metadata WHERE media_item_id = $1`,
		itemID,
	).Scan(&m.BitrateKbps, &m.TranscodedAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// SetTranscodedAt records the time a transcode completed for a media item.
func (r *MediaItemRepository) SetTranscodedAt(ctx context.Context, itemID int64, t time.Time) error {
	_, err := r.db.Exec(ctx,
		`UPDATE video_metadata SET transcoded_at = $2 WHERE media_item_id = $1`,
		itemID, t,
	)
	return err
}

// SetVideoManualCover sets the manual_cover flag on a video_metadata row.
func (r *MediaItemRepository) SetVideoManualCover(ctx context.Context, itemID int64, manual bool) error {
	_, err := r.db.Exec(ctx,
		`UPDATE video_metadata SET manual_cover = $2 WHERE media_item_id = $1`,
		itemID, manual,
	)
	return err
}

// ListVideoItemsByCollection returns media items in a video collection.
// For video:movie collections it recurses into sub-collections (ordered by title).
// For video:home_movie collections it returns only the direct collection (ordered by relative_path).
// BookmarkSeconds is populated per-user.
func (r *MediaItemRepository) ListVideoItemsByCollection(ctx context.Context, collectionID int64, userID int64, collectionType string, limit, offset int) ([]models.MediaItemView, error) {
	var rows pgx.Rows
	var err error

	if collectionType == "video:movie" {
		// Recursive CTE to include all descendant collections
		rows, err = r.db.Query(ctx,
			`WITH RECURSIVE descendants AS (
			     SELECT id FROM collections WHERE id = $1
			     UNION ALL
			     SELECT c.id FROM collections c
			     JOIN descendants d ON c.parent_collection_id = d.id
			 )
			 SELECT m.id, m.title, m.mime_type, m.ordinal,
			        vm.duration_seconds, vm.width, vm.height, vm.bitrate_kbps,
			        vm.video_codec, vm.audio_codec, vm.transcoded_at,
			        vm.date, vm.end_date, vm.author, vm.manual_cover,
			        vb.position_seconds
			 FROM media_items m
			 JOIN descendants d ON d.id = m.collection_id
			 LEFT JOIN video_metadata vm ON vm.media_item_id = m.id
			 LEFT JOIN video_bookmarks vb ON vb.media_item_id = m.id AND vb.user_id = $2
			 WHERE m.missing_since IS NULL AND m.hidden_at IS NULL
			   AND EXISTS (
			       SELECT 1 FROM collection_access ca
			       JOIN collections c ON c.id = $1
			       WHERE ca.collection_id = c.root_collection_id AND ca.user_id = $2
			   )
			 ORDER BY m.title ASC
			 LIMIT $3 OFFSET $4`,
			collectionID, userID, limit, offset,
		)
	} else {
		// video:home_movie — single collection, ordered by path
		rows, err = r.db.Query(ctx,
			`SELECT m.id, m.title, m.mime_type, m.ordinal,
			        vm.duration_seconds, vm.width, vm.height, vm.bitrate_kbps,
			        vm.video_codec, vm.audio_codec, vm.transcoded_at,
			        vm.date, vm.end_date, vm.author, vm.manual_cover,
			        vb.position_seconds
			 FROM media_items m
			 LEFT JOIN video_metadata vm ON vm.media_item_id = m.id
			 LEFT JOIN video_bookmarks vb ON vb.media_item_id = m.id AND vb.user_id = $2
			 WHERE m.collection_id = $1
			   AND m.missing_since IS NULL AND m.hidden_at IS NULL
			   AND EXISTS (
			       SELECT 1 FROM collection_access ca
			       JOIN collections c ON c.id = $1
			       WHERE ca.collection_id = c.root_collection_id AND ca.user_id = $2
			   )
			 ORDER BY m.relative_path ASC
			 LIMIT $3 OFFSET $4`,
			collectionID, userID, limit, offset,
		)
	}
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (models.MediaItemView, error) {
		var item models.MediaItemView
		var vm models.VideoMetadata
		err := row.Scan(
			&item.ID, &item.Title, &item.MimeType, &item.Ordinal,
			&vm.DurationSeconds, &vm.Width, &vm.Height, &vm.BitrateKbps,
			&vm.VideoCodec, &vm.AudioCodec, &vm.TranscodedAt,
			&vm.Date, &vm.EndDate, &vm.Author, &vm.ManualCover,
			&vm.BookmarkSeconds,
		)
		item.Video = &vm
		return item, err
	})
}
