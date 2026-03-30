package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// PhotoUpsert inserts or updates photo metadata for a media item.
func PhotoUpsert(ctx context.Context, db utils.DBTX, itemID int64,
	widthPx, heightPx *int,
	createdAt *time.Time,
	cameraMake, cameraModel, lensModel, shutterSpeed *string,
	aperture *float64,
	iso *int,
	focalLengthMM, focalLength35mmEquiv *float64,
	colorSpace, description *string,
	exifRaw json.RawMessage,
) error {
	_, err := db.Exec(ctx,
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

// PhotoCountPendingVariants returns the number of photo_metadata rows awaiting variant generation.
func PhotoCountPendingVariants(ctx context.Context, db utils.DBTX) (int, error) {
	var count int
	err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM photo_metadata
		 WHERE variants_generated_at IS NULL
		 AND media_item_id IN (
		     SELECT id FROM media_items WHERE missing_since IS NULL AND hidden_at IS NULL
		 )`,
	).Scan(&count)
	return count, err
}

// PhotoUpdateVariants marks variants as generated and stores the placeholder.
func PhotoUpdateVariants(ctx context.Context, db utils.DBTX, itemID int64, placeholder *string) error {
	_, err := db.Exec(ctx,
		`UPDATE photo_metadata
		 SET placeholder = $2, variants_generated_at = now()
		 WHERE media_item_id = $1`,
		itemID, placeholder,
	)
	return err
}

// PhotoClearVariantsGenerated sets variants_generated_at to NULL for all items in a
// collection and its sub-collections, so they are re-queued for processing.
func PhotoClearVariantsGenerated(ctx context.Context, db utils.DBTX, collectionID int64) error {
	_, err := db.Exec(ctx,
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

// PhotoExifRaw returns the raw EXIF JSON for a media item.
func PhotoExifRaw(ctx context.Context, db utils.DBTX, itemID int64, userID int64) ([]byte, error) {
	var raw []byte
	err := db.QueryRow(ctx,
		`SELECT pm.exif_raw
		 FROM photo_metadata pm
		 JOIN media_items m ON m.id = pm.media_item_id AND m.hidden_at IS NULL
		 JOIN collections c ON c.id = m.collection_id
		 JOIN collection_access ca ON ca.collection_id = c.id AND ca.user_id = $2
		 WHERE pm.media_item_id = $1`, itemID, userID).Scan(&raw)
	return raw, err
}
