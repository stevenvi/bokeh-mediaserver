package repository

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type MediaItemRepository struct {
	db utils.DBTX
}

func NewMediaItemRepository(db utils.DBTX) *MediaItemRepository {
	return &MediaItemRepository{db: db}
}

// GetByID returns a media item by ID, enforcing that the given user has access
// to the collection it belongs to. Returns an error (→ 404) if the item does
// not exist, is hidden, or the user lacks collection_access.
func (r *MediaItemRepository) GetByID(ctx context.Context, id, userID int64) (*models.MediaItem, error) {
	var item models.MediaItem
	err := r.db.QueryRow(ctx,
		`SELECT m.id, m.collection_id, m.title, m.mime_type, m.ordinal
		 FROM media_items m
		 JOIN collection_access ca ON ca.collection_id = m.collection_id AND ca.user_id = $2
		 WHERE m.id = $1 AND m.hidden_at IS NULL`,
		id, userID,
	).Scan(&item.ID, &item.CollectionID, &item.Title, &item.MimeType, &item.Ordinal)
	if err != nil {
		return nil, err
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

// GetRelativePath returns the filesystem relative path for a media item,
// enforcing that the given user has collection_access for it.
func (r *MediaItemRepository) GetRelativePath(ctx context.Context, id int64) (string, error) {
	var path string
	err := r.db.QueryRow(ctx, `SELECT relative_path FROM media_items WHERE id = $1`, id).Scan(&path)
	return path, err
}

// GetFileHash returns the content hash for a media item,
// enforcing that the given user has collection_access for it.
func (r *MediaItemRepository) GetFileHash(ctx context.Context, id int64) (string, error) {
	var hash string
	err := r.db.QueryRow(ctx, `SELECT file_hash FROM media_items WHERE id = $1`, id).Scan(&hash)
	return hash, err
}

// UpdateTitle sets the title for a media item.
func (r *MediaItemRepository) UpdateTitle(ctx context.Context, id int64, title string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE media_items SET title = $2 WHERE id = $1`, id, title,
	)
	return err
}

// ListByCollectionPaginated returns paginated media items for a collection.
func (r *MediaItemRepository) ListByCollectionPaginated(ctx context.Context, collectionID int64, userID int64, limit, offset int) ([]models.MediaItem, error) {
	rows, err := r.db.Query(ctx,
		`SELECT m.id, m.collection_id, m.title, m.mime_type, m.ordinal
		 FROM media_items m
		 JOIN collection_access ca ON ca.collection_id = m.collection_id AND ca.user_id = $4
		 WHERE m.collection_id = $1 AND m.missing_since IS NULL AND m.hidden_at IS NULL
		 ORDER BY m.ordinal ASC NULLS LAST, m.title ASC
		 LIMIT $2 OFFSET $3`,
		collectionID, limit, offset, userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []models.MediaItem
	for rows.Next() {
		var item models.MediaItem
		if err := rows.Scan(&item.ID, &item.CollectionID, &item.Title, &item.MimeType, &item.Ordinal); err != nil {
			slog.Warn("scan media item", "collection_id", collectionID, "err", err)
			continue
		}
		items = append(items, item)
	}
	return items, nil
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
			ON CONFLICT (relative_path) DO UPDATE SET
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
	defer rows.Close()

	existing := make(map[string]struct{}, len(hashes))
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err == nil {
			existing[h] = struct{}{}
		}
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

// GetPhotoMetadata returns photo metadata for a media item.
func (r *MediaItemRepository) GetPhotoMetadata(ctx context.Context, itemID int64) (*models.PhotoMetadata, error) {
	var p models.PhotoMetadata
	err := r.db.QueryRow(ctx,
		`SELECT width_px, height_px, created_at,
		        camera_make, camera_model, lens_model,
		        shutter_speed, aperture, iso,
		        focal_length_mm, focal_length_35mm_equiv,
		        color_space,
		        placeholder
		 FROM photo_metadata WHERE media_item_id = $1`,
		itemID,
	).Scan(
		&p.WidthPx, &p.HeightPx, &p.CreatedAt,
		&p.CameraMake, &p.CameraModel, &p.LensModel,
		&p.ShutterSpeed, &p.Aperture, &p.ISO,
		&p.FocalLengthMM, &p.FocalLength35mmEquiv,
		&p.ColorSpace,
		&p.Placeholder,
	)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// GetExifRaw returns the raw EXIF JSON for a media item.
func (r *MediaItemRepository) GetExifRaw(ctx context.Context, itemID int64, userID int64) ([]byte, error) {
	var raw []byte
	err := r.db.QueryRow(ctx,
		`SELECT pm.exif_raw 
		 FROM photo_metadata as pm
		 JOIN media_items m ON m.id = pm.media_item_id AND m.hidden_at IS NULL
		 JOIN collection c ON c.id = m.collection_id AND c.hidden_at IS NULL
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
	defer rows.Close()

	var items []StaleItem
	for rows.Next() {
		var item StaleItem
		if err := rows.Scan(&item.ID, &item.Hash); err != nil {
			continue
		}
		items = append(items, item)
	}
	return items, nil
}
