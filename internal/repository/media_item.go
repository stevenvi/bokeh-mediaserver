package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// collectionAccessExists is the EXISTS subquery fragment that checks whether a user
// has access to a media item's root collection. It assumes the outer query has joined
// collections as "c" and that userID is bound to $2.
const collectionAccessExists = `EXISTS (
	    SELECT 1 FROM collection_access ca
	    WHERE ca.collection_id = c.root_collection_id
	      AND ca.user_id = $2
	)`

// collectionAccessExistsFromParam is the EXISTS subquery for contexts where the
// collection ID comes from query parameter $1 (no outer join on "c"). userID is $2.
const collectionAccessExistsFromParam = `EXISTS (
	    SELECT 1 FROM collection_access ca
	    JOIN collections c ON c.id = $1
	    WHERE ca.collection_id = c.root_collection_id AND ca.user_id = $2
	)`

// MediaItemUpsert inserts or updates a media item, returning the ID and whether the file was unchanged.
// Uses a CTE to capture pre-existing state for change detection in a single round-trip.
func MediaItemUpsert(ctx context.Context, db utils.DBTX, collectionID int64, title, relativePath string, fileSizeBytes int64, fileHash, mimeType string) (id int64, wasUnchanged bool, err error) {
	err = db.QueryRow(ctx, `
		WITH prev AS (
			SELECT id, file_size_bytes, file_hash
			FROM media_items
			WHERE relative_path = $3 AND collection_id = $1
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


// MediaItemForProcessing returns fields needed for media processing. Only returns non-missing items.
func MediaItemForProcessing(ctx context.Context, db utils.DBTX, id int64) (relativePath, mimeType, fileHash string, err error) {
	err = db.QueryRow(ctx,
		`SELECT relative_path, mime_type, file_hash FROM media_items WHERE id = $1 AND missing_since IS NULL`,
		id,
	).Scan(&relativePath, &mimeType, &fileHash)
	return
}

// DeleteMediaItem removes a media item by ID. Note that it will come right back on next scan,
// so hiding is likely what you will actually want to be doing.
func DeleteMediaItem(ctx context.Context, db utils.DBTX, id int64) error {
	_, err := db.Exec(ctx, `DELETE FROM media_items WHERE id = $1`, id)
	return err
}

// MediaItemFileHashAndPath returns the content hash and relative filesystem path for a media item,
// enforcing that the given user has collection_access to the root ancestor collection.
func MediaItemFileHashAndPath(ctx context.Context, db utils.DBTX, id, userID int64) (hash, relativePath string, err error) {
	err = db.QueryRow(ctx,
		`SELECT m.file_hash, m.relative_path FROM media_items m
		 JOIN collections c ON c.id = m.collection_id
		 WHERE m.id = $1 AND m.hidden_at IS NULL AND m.missing_since IS NULL
		   AND `+collectionAccessExists,
		id, userID,
	).Scan(&hash, &relativePath)
	return
}

// MediaItemUpdateTitle sets the title for a media item.
func MediaItemUpdateTitle(ctx context.Context, db utils.DBTX, id int64, title string) error {
	_, err := db.Exec(ctx,
		`UPDATE media_items SET title = $2 WHERE id = $1`, id, title,
	)
	return err
}


// MediaItemSetHidden marks an item as hidden or visible in the database. This is effectively a soft delete,
// which prevents removed items from showing back up after a collection scan.
func MediaItemSetHidden(ctx context.Context, db utils.DBTX, id int64, hidden bool) error {
	var err error
	if hidden {
		_, err = db.Exec(ctx, `UPDATE media_items SET hidden_at = now() WHERE id = $1`, id)
	} else {
		_, err = db.Exec(ctx, `UPDATE media_items SET hidden_at = NULL WHERE id = $1`, id)
	}
	return err
}

// MediaItemFindExistingHashes returns the subset of provided hashes that exist in the DB.
func MediaItemFindExistingHashes(ctx context.Context, db utils.DBTX, hashes []string) (map[string]struct{}, error) {
	rows, err := db.Query(ctx,
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

// StaleMediaItem represents a media item that has been missing long enough to prune.
type StaleMediaItem struct {
	Hash string
	ID   int64
}

// MediaItemsStale returns items missing for more than 90 days.
func MediaItemsStale(ctx context.Context, db utils.DBTX) ([]StaleMediaItem, error) {
	rows, err := db.Query(ctx,
		`SELECT file_hash, id FROM media_items
		 WHERE missing_since IS NOT NULL
		   AND missing_since < now() - interval '90 days'`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[StaleMediaItem])
}

// PhotoItemIDsByCollection returns IDs of all image (MIME prefix "image/") media items
// in a collection and its sub-collections (recursive), excluding missing items.
func PhotoItemIDsByCollection(ctx context.Context, db utils.DBTX, collectionID int64) ([]int64, error) {
	rows, err := db.Query(ctx,
		`WITH RECURSIVE tree AS (
			SELECT id FROM collections WHERE id = $1
			UNION ALL
			SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
		)
		SELECT mi.id
		FROM media_items mi
		JOIN tree t ON mi.collection_id = t.id
		WHERE mi.missing_since IS NULL AND mi.mime_type LIKE 'image/%'`,
		collectionID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// MediaItemHashesByCollection returns all file hashes for non-missing items in a collection
// and its sub-collections (recursive).
func MediaItemHashesByCollection(ctx context.Context, db utils.DBTX, collectionID int64) ([]string, error) {
	rows, err := db.Query(ctx,
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

// MediaItemRandomHashWithVariants picks a random item with generated variants,
// starting with the collection's direct items and expanding one depth level at a time.
// It searches up to 8 nested levels; returns pgx.ErrNoRows if nothing is found.
func MediaItemRandomHashWithVariants(ctx context.Context, db utils.DBTX, collectionID int64) (string, error) {
	collectionIDs := []int64{collectionID}

	for depth := 0; depth <= 8; depth++ {
		if len(collectionIDs) == 0 {
			break
		}

		var hash string
		err := db.QueryRow(ctx,
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
		rows, err := db.Query(ctx,
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

// MediaItemCollectionID returns the collection_id for a media item.
func MediaItemCollectionID(ctx context.Context, db utils.DBTX, itemID int64) (int64, error) {
	var collID int64
	err := db.QueryRow(ctx,
		`SELECT collection_id FROM media_items WHERE id = $1`,
		itemID,
	).Scan(&collID)
	return collID, err
}

// MediaItemRootCollectionID returns the root collection ID for the collection containing
// the given media item.
func MediaItemRootCollectionID(ctx context.Context, db utils.DBTX, itemID int64) (int64, error) {
	var rootID int64
	err := db.QueryRow(ctx,
		`SELECT c.root_collection_id
		 FROM media_items mi
		 JOIN collections c ON c.id = mi.collection_id
		 WHERE mi.id = $1`,
		itemID,
	).Scan(&rootID)
	return rootID, err
}

// MediaItemRootCollectionType returns the type of the root collection that contains the
// given media item (e.g. "video:movie", "video:home_movie").
func MediaItemRootCollectionType(ctx context.Context, db utils.DBTX, itemID int64) (colType constants.CollectionType, err error) {
	err = db.QueryRow(ctx,
		`SELECT rc.type
		 FROM media_items mi
		 JOIN collections c  ON c.id  = mi.collection_id
		 JOIN collections rc ON rc.id = c.root_collection_id
		 WHERE mi.id = $1`,
		itemID,
	).Scan(&colType)
	return
}

// MediaItemGetAudioStreamInfo returns fields needed to stream an audio file, with access check.
func MediaItemGetAudioStreamInfo(ctx context.Context, db utils.DBTX, itemID int64, userID int64) (relativePath, mimeType string, err error) {
	err = db.QueryRow(ctx,
		`SELECT m.relative_path, m.mime_type FROM media_items m
		 JOIN collections c ON c.id = m.collection_id
		 WHERE m.id = $1 AND m.hidden_at IS NULL AND m.missing_since IS NULL
		   AND `+collectionAccessExists,
		itemID, userID,
	).Scan(&relativePath, &mimeType)
	return
}

// MediaItemGetVideoStreamInfo returns fields needed to stream a video file, with access check.
// Returns relativePath, mimeType, and file_hash.
func MediaItemGetVideoStreamInfo(ctx context.Context, db utils.DBTX, itemID int64, userID int64) (relativePath, mimeType, hash string, err error) {
	err = db.QueryRow(ctx,
		`SELECT m.relative_path, m.mime_type, m.file_hash FROM media_items m
		 JOIN collections c ON c.id = m.collection_id
		 WHERE m.id = $1 AND m.hidden_at IS NULL AND m.missing_since IS NULL
		   AND `+collectionAccessExists,
		itemID, userID,
	).Scan(&relativePath, &mimeType, &hash)
	return
}

// VideoIntegrityItem holds the fields needed by the integrity checker for a single video item.
type VideoIntegrityItem struct {
	TranscodedAt   *time.Time `json:"transcoded_at"`
	FileHash       string     `json:"file_hash"`
	CollectionType string     `json:"collection_type"`
	ItemID         int64      `json:"media_item_id"`
}

// MediaItemVideosByCollection returns media items in a video collection.
// For video:movie collections it recurses into sub-collections (ordered by title).
// For video:home_movie collections it returns only the direct collection (ordered by relative_path).
// BookmarkSeconds is populated per-user.
func MediaItemVideosByCollection(ctx context.Context, db utils.DBTX, collectionID int64, userID int64, collectionType constants.CollectionType, limit, offset int) ([]models.VideoItemView, error) {
	var rows pgx.Rows
	var err error

	if collectionType == constants.CollectionTypeMovie {
		// Recursive CTE to include all descendant collections
		rows, err = db.Query(ctx,
			`WITH RECURSIVE descendants AS (
			     SELECT id FROM collections WHERE id = $1
			     UNION ALL
			     SELECT c.id FROM collections c
			     JOIN descendants d ON c.parent_collection_id = d.id
			 )
			 SELECT m.id, m.title, m.mime_type, m.ordinal,
			        vm.duration_seconds, vm.width, vm.height, vm.bitrate_kbps,
			        vm.video_codec, vm.audio_codec, vm.transcoded_at,
			        vm.date, vm.end_date, vm.author, vm.manual_thumbnail,
			        vb.position_seconds
			 FROM media_items m
			 JOIN descendants d ON d.id = m.collection_id
			 LEFT JOIN video_metadata vm ON vm.media_item_id = m.id
			 LEFT JOIN video_bookmarks vb ON vb.media_item_id = m.id AND vb.user_id = $2
			 WHERE m.missing_since IS NULL AND m.hidden_at IS NULL
			   AND `+collectionAccessExistsFromParam+`
			 ORDER BY m.title ASC
			 LIMIT $3 OFFSET $4`,
			collectionID, userID, limit, offset,
		)
	} else {
		// video:home_movie — single collection, ordered by path
		rows, err = db.Query(ctx,
			`SELECT m.id, m.title, m.mime_type, m.ordinal,
			        vm.duration_seconds, vm.width, vm.height, vm.bitrate_kbps,
			        vm.video_codec, vm.audio_codec, vm.transcoded_at,
			        vm.date, vm.end_date, vm.author, vm.manual_thumbnail,
			        vb.position_seconds
			 FROM media_items m
			 LEFT JOIN video_metadata vm ON vm.media_item_id = m.id
			 LEFT JOIN video_bookmarks vb ON vb.media_item_id = m.id AND vb.user_id = $2
			 WHERE m.collection_id = $1
			   AND m.missing_since IS NULL AND m.hidden_at IS NULL
			   AND `+collectionAccessExistsFromParam+`
			 ORDER BY m.relative_path ASC
			 LIMIT $3 OFFSET $4`,
			collectionID, userID, limit, offset,
		)
	}
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (models.VideoItemView, error) {
		var item models.VideoItemView
		var dummy *int
		err := row.Scan(
			&item.ID, &item.Title, &item.MimeType, &dummy,
			&item.DurationSeconds, &item.Width, &item.Height, &item.BitrateKbps,
			&item.VideoCodec, &item.AudioCodec, &item.TranscodedAt,
			&item.Date, &item.EndDate, &item.Author, &item.ManualThumbnail,
			&item.BookmarkSeconds,
		)
		return item, err
	})
}

// MediaItemVideoGet returns a single video item by ID, scoped to a collection tree.
// Returns an error (→ 404) if the item does not exist, is hidden/missing, does not belong
// to the given collection's tree, or the user lacks collection_access.
func MediaItemVideoGet(ctx context.Context, db utils.DBTX, collectionID, itemID, userID int64) (*models.VideoItemView, error) {
	var item models.VideoItemView
	err := db.QueryRow(ctx,
		`WITH RECURSIVE tree AS (
		     SELECT id FROM collections WHERE id = $1
		     UNION ALL
		     SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
		 )
		 SELECT m.id, m.title, m.mime_type,
		        vm.duration_seconds, vm.width, vm.height, vm.bitrate_kbps,
		        vm.video_codec, vm.audio_codec, vm.transcoded_at,
		        vm.date, vm.end_date, vm.author, vm.manual_thumbnail,
		        vb.position_seconds
		 FROM media_items m
		 JOIN tree t ON t.id = m.collection_id
		 LEFT JOIN video_metadata vm ON vm.media_item_id = m.id
		 LEFT JOIN video_bookmarks vb ON vb.media_item_id = m.id AND vb.user_id = $3
		 WHERE m.id = $2
		   AND m.hidden_at IS NULL AND m.missing_since IS NULL
		   AND `+collectionAccessExistsFromParam,
		collectionID, itemID, userID,
	).Scan(
		&item.ID, &item.Title, &item.MimeType,
		&item.DurationSeconds, &item.Width, &item.Height, &item.BitrateKbps,
		&item.VideoCodec, &item.AudioCodec, &item.TranscodedAt,
		&item.Date, &item.EndDate, &item.Author, &item.ManualThumbnail,
		&item.BookmarkSeconds,
	)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// PhotoQuery holds parameters for a photo items page fetch.
type PhotoQuery struct {
	CollectionID int64
	Offset       int
	Limit        int
	Ascending    bool
	Recursive    bool
}

// PhotoItems returns a page of photo items with full EXIF data, ordered by created_at.
// When Recursive is true, items from all descendant collections are included.
func PhotoItems(ctx context.Context, db utils.DBTX, q PhotoQuery) ([]models.PhotoItem, error) {
	dir := "ASC"
	if !q.Ascending {
		dir = "DESC"
	}

	var cte, collectionFilter string
	if q.Recursive {
		cte = `WITH RECURSIVE collection_tree AS (
		    SELECT id FROM collections WHERE id = $1
		    UNION ALL
		    SELECT c.id FROM collections c
		    INNER JOIN collection_tree ct ON c.parent_collection_id = ct.id
		)`
		collectionFilter = "mi.collection_id = ANY(SELECT id FROM collection_tree)"
	} else {
		cte = ""
		collectionFilter = "mi.collection_id = $1"
	}

	query := fmt.Sprintf(`
		%s
		SELECT mi.id, mi.title, mi.mime_type,
		       pm.created_at, pm.width_px, pm.height_px,
		       pm.camera_make, pm.camera_model, pm.lens_model,
		       pm.shutter_speed, pm.aperture, pm.iso,
		       pm.focal_length_mm, pm.focal_length_35mm_equiv,
		       pm.variants_generated_at
		FROM media_items mi
		JOIN photo_metadata pm ON pm.media_item_id = mi.id
		WHERE %s
		  AND mi.missing_since IS NULL
		  AND mi.hidden_at IS NULL
		ORDER BY pm.created_at %s NULLS LAST, mi.id %s
		LIMIT $2 OFFSET $3`,
		cte, collectionFilter, dir, dir,
	)

	rows, err := db.Query(ctx, query, q.CollectionID, q.Limit, q.Offset)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (models.PhotoItem, error) {
		var item models.PhotoItem
		err := row.Scan(
			&item.ID, &item.Title, &item.MimeType,
			&item.CreatedAt, &item.WidthPx, &item.HeightPx,
			&item.CameraMake, &item.CameraModel, &item.LensModel,
			&item.ShutterSpeed, &item.Aperture, &item.ISO,
			&item.FocalLengthMM, &item.FocalLength35mmEquiv,
			&item.VariantsGeneratedAt,
		)
		return item, err
	})
}

// PhotoStatistics returns per-month item counts for a photo collection.
// Items with NULL created_at are excluded.
func PhotoStatistics(ctx context.Context, db utils.DBTX, collectionID int64, recursive bool) ([]models.SlideshowMonthCount, error) {
	var cte, collectionFilter string
	if recursive {
		cte = `WITH RECURSIVE collection_tree AS (
		    SELECT id FROM collections WHERE id = $1
		    UNION ALL
		    SELECT c.id FROM collections c
		    INNER JOIN collection_tree ct ON c.parent_collection_id = ct.id
		)`
		collectionFilter = "mi.collection_id = ANY(SELECT id FROM collection_tree)"
	} else {
		cte = ""
		collectionFilter = "mi.collection_id = $1"
	}

	query := fmt.Sprintf(`
		%s
		SELECT
		    EXTRACT(YEAR FROM pm.created_at)::int  AS year,
		    EXTRACT(MONTH FROM pm.created_at)::int AS month,
		    COUNT(*)::int                           AS count
		FROM media_items mi
		JOIN photo_metadata pm ON pm.media_item_id = mi.id
		WHERE %s
		  AND mi.missing_since IS NULL
		  AND mi.hidden_at IS NULL
		  AND pm.created_at IS NOT NULL
		GROUP BY 1, 2
		ORDER BY 1, 2`,
		cte, collectionFilter,
	)

	rows, err := db.Query(ctx, query, collectionID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.SlideshowMonthCount])
}



// ScanItem holds the fields needed to check a media item against the filesystem.
type ScanItem struct {
	ID            int64
	RelativePath  string
	FileSizeBytes int64
	FileHash      string
	IsMissing     bool
}

// KnownScanItem holds the scan-relevant fields for a known media item.
type KnownScanItem struct {
	FileHash    string
	ID          int64
	HasMetadata bool
	IsMissing   bool
}

// MediaItemsKnownForScan returns all media items (including missing) in a collection
// tree, keyed by relative path, with a flag indicating whether the type-appropriate
// metadata record exists. mimeCategory should be "audio", "video", or "image".
func MediaItemsKnownForScan(ctx context.Context, db utils.DBTX, collectionID int64, mimeCategory string) (map[string]KnownScanItem, error) {
	var metaTable string
	switch mimeCategory {
	case "audio":
		metaTable = "audio_metadata"
	case "video":
		metaTable = "video_metadata"
	case "image":
		metaTable = "photo_metadata"
	}

	var query string
	if metaTable != "" {
		query = fmt.Sprintf(`
			WITH RECURSIVE tree AS (
				SELECT id FROM collections WHERE id = $1
				UNION ALL
				SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
			)
			SELECT mi.relative_path, mi.id, (meta.media_item_id IS NOT NULL) AS has_metadata,
			       (mi.missing_since IS NOT NULL) AS is_missing, mi.file_hash
			FROM media_items mi
			JOIN tree t ON mi.collection_id = t.id
			LEFT JOIN %s meta ON meta.media_item_id = mi.id`, metaTable)
	} else {
		// Unknown category — treat all existing items as complete so we don't re-queue them.
		query = `
			WITH RECURSIVE tree AS (
				SELECT id FROM collections WHERE id = $1
				UNION ALL
				SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
			)
			SELECT mi.relative_path, mi.id, true AS has_metadata,
			       (mi.missing_since IS NOT NULL) AS is_missing, mi.file_hash
			FROM media_items mi
			JOIN tree t ON mi.collection_id = t.id`
	}

	rows, err := db.Query(ctx, query, collectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make(map[string]KnownScanItem)
	for rows.Next() {
		var path string
		var item KnownScanItem
		if err := rows.Scan(&path, &item.ID, &item.HasMetadata, &item.IsMissing, &item.FileHash); err != nil {
			return nil, err
		}
		items[path] = item
	}
	return items, rows.Err()
}

// MediaItemsForScan returns all media items (missing and non-missing) in a
// collection tree with the fields needed for filesystem sync and change detection.
func MediaItemsForScan(ctx context.Context, db utils.DBTX, collectionID int64) ([]ScanItem, error) {
	rows, err := db.Query(ctx,
		`WITH RECURSIVE tree AS (
			SELECT id FROM collections WHERE id = $1
			UNION ALL
			SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
		)
		SELECT mi.id, mi.relative_path, mi.file_size_bytes, mi.file_hash,
		       (mi.missing_since IS NOT NULL) AS is_missing
		FROM media_items mi
		JOIN tree t ON mi.collection_id = t.id`,
		collectionID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ScanItem])
}

// MediaItemUpdateFileInfo updates the file_size_bytes and file_hash for an existing
// media item when the scan detects that the file content has changed.
func MediaItemUpdateFileInfo(ctx context.Context, db utils.DBTX, itemID int64, fileSize int64, fileHash string) error {
	_, err := db.Exec(ctx,
		`UPDATE media_items SET file_size_bytes = $2, file_hash = $3 WHERE id = $1`,
		itemID, fileSize, fileHash,
	)
	return err
}

// MediaItemMarkMissing marks a single item as missing (sets missing_since = now()).
// No-op if already marked missing.
func MediaItemMarkMissing(ctx context.Context, db utils.DBTX, itemID int64) error {
	_, err := db.Exec(ctx,
		`UPDATE media_items SET missing_since = now() WHERE id = $1 AND missing_since IS NULL`,
		itemID,
	)
	return err
}

// MediaItemClearMissing restores a previously-missing item (clears missing_since,
// updates indexed_at to now).
func MediaItemClearMissing(ctx context.Context, db utils.DBTX, itemID int64) error {
	_, err := db.Exec(ctx,
		`UPDATE media_items SET missing_since = NULL, indexed_at = now() WHERE id = $1`,
		itemID,
	)
	return err
}

// MediaItemUpdateSizeAndHash updates the stored file size and hash for an item
// and refreshes indexed_at. Used by the metadata scan when a changed file is detected.
func MediaItemUpdateSizeAndHash(ctx context.Context, db utils.DBTX, itemID int64, fileSize int64, fileHash string) error {
	_, err := db.Exec(ctx,
		`UPDATE media_items SET file_size_bytes = $2, file_hash = $3, indexed_at = now() WHERE id = $1`,
		itemID, fileSize, fileHash,
	)
	return err
}

// MediaItemMarkMissingSince marks items in a collection that haven't been indexed since `before`.
// Returns number of rows affected.
func MediaItemMarkMissingSince(ctx context.Context, db utils.DBTX, collectionID int64, before time.Time) (int64, error) {
	tag, err := db.Exec(ctx,
		`UPDATE media_items
		 SET missing_since = now()
		 WHERE collection_id = $1
		   AND missing_since IS NULL
		   AND indexed_at < $2`,
		collectionID, before,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
