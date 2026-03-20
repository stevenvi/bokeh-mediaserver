package repository

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type CollectionRepository struct {
	db utils.DBTX
}

func NewCollectionRepository(db utils.DBTX) *CollectionRepository {
	return &CollectionRepository{db: db}
}

// ExistsByRelativePath returns true if any collection already uses the given relative_path.
func (r *CollectionRepository) ExistsByRelativePath(ctx context.Context, relativePath string) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM collections WHERE relative_path = $1)`,
		relativePath,
	).Scan(&exists)
	return exists, err
}

// Create inserts a new top-level collection and returns its ID.
func (r *CollectionRepository) Create(ctx context.Context, name, colType, relativePath string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`INSERT INTO collections (name, type, relative_path)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		name, colType, relativePath,
	).Scan(&id)
	return id, err
}

// GetByID returns an enabled collection by ID.
func (r *CollectionRepository) GetByID(ctx context.Context, id int64) (*models.Collection, error) {
	var c models.Collection
	err := r.db.QueryRow(ctx,
		`SELECT id, parent_collection_id, name, type
		 FROM collections WHERE id = $1 AND is_enabled = true`,
		id,
	).Scan(&c.ID, &c.ParentCollectionID, &c.Name, &c.Type)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// GetEnabled returns whether a collection is enabled.
func (r *CollectionRepository) GetEnabled(ctx context.Context, id int64) (bool, error) {
	var isEnabled bool
	err := r.db.QueryRow(ctx,
		`SELECT is_enabled FROM collections WHERE id = $1`, id,
	).Scan(&isEnabled)
	return isEnabled, err
}

// GetRelativePath returns the relative path for a collection.
func (r *CollectionRepository) GetRelativePath(ctx context.Context, id int64) (string, error) {
	var relativePath string
	err := r.db.QueryRow(ctx,
		`SELECT COALESCE(relative_path, '') FROM collections WHERE id = $1`, id,
	).Scan(&relativePath)
	return relativePath, err
}

// ListTopLevel returns all top-level collections (admin view).
func (r *CollectionRepository) ListTopLevel(ctx context.Context) ([]models.Collection, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, name, type, relative_path,
		        is_enabled, last_scanned_at, created_at
		 FROM collections WHERE parent_collection_id IS NULL ORDER BY name`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	collections := []models.Collection{}
	for rows.Next() {
		var c models.Collection
		if err := rows.Scan(
			&c.ID, &c.Name, &c.Type, &c.RelativePath,
			&c.IsEnabled, &c.LastScannedAt, &c.CreatedAt,
		); err != nil {
			continue
		}
		collections = append(collections, c)
	}
	return collections, nil
}

// ListTopLevelEnabled returns IDs of all enabled top-level collections.
func (r *CollectionRepository) ListTopLevelEnabled(ctx context.Context) ([]int64, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id FROM collections
		 WHERE parent_collection_id IS NULL AND is_enabled`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// ListAccessibleByUser returns enabled top-level collections the user has access to.
func (r *CollectionRepository) ListAccessibleByUser(ctx context.Context, userID int64) ([]models.CollectionSummary, error) {
	rows, err := r.db.Query(ctx,
		`SELECT c.id, c.name, c.type
		 FROM collections c
		 JOIN collection_access ca ON ca.collection_id = c.id AND ca.user_id = $1
		 WHERE c.parent_collection_id IS NULL AND c.is_enabled = true
		 ORDER BY c.name`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.CollectionSummary])
}

// ListChildren returns direct enabled children of a collection.
func (r *CollectionRepository) ListChildren(ctx context.Context, parentID int64) ([]models.Collection, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, parent_collection_id, name, type
		 FROM collections
		 WHERE parent_collection_id = $1 AND is_enabled = true
		 ORDER BY name`,
		parentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	collections := []models.Collection{}
	for rows.Next() {
		var c models.Collection
		if err := rows.Scan(&c.ID, &c.ParentCollectionID, &c.Name, &c.Type); err != nil {
			continue
		}
		collections = append(collections, c)
	}
	return collections, nil
}

// ExistsEnabled returns true if a collection with the given ID exists and is enabled.
func (r *CollectionRepository) ExistsEnabled(ctx context.Context, id int64) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM collections WHERE id = $1 AND is_enabled = true)`,
		id,
	).Scan(&exists)
	return exists, err
}

// GetByIDForUser returns a collection by ID, enforcing that the user has collection_access
// to the root ancestor of the collection. Returns an error (→ 404) if not found or no access.
func (r *CollectionRepository) GetByIDForUser(ctx context.Context, id, userID int64) (*models.CollectionView, error) {
	var c models.CollectionView
	err := r.db.QueryRow(ctx,
		`WITH RECURSIVE ancestors AS (
		     SELECT id, parent_collection_id FROM collections WHERE id = $1 AND is_enabled = true
		     UNION ALL
		     SELECT c.id, c.parent_collection_id FROM collections c
		     INNER JOIN ancestors a ON c.id = a.parent_collection_id
		 )
		 SELECT c.id, c.parent_collection_id, c.name, c.type
		 FROM collections c
		 WHERE c.id = $1 AND c.is_enabled = true
		   AND EXISTS (
		       SELECT 1 FROM collection_access ca
		       WHERE ca.collection_id = (SELECT id FROM ancestors WHERE parent_collection_id IS NULL)
		         AND ca.user_id = $2
		   )`,
		id, userID,
	).Scan(&c.ID, &c.ParentCollectionID, &c.Name, &c.Type)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// HasAccessToCollection returns true if the user has collection_access for the root
// ancestor of the given collection.
func (r *CollectionRepository) HasAccessToCollection(ctx context.Context, collectionID, userID int64) (bool, error) {
	var hasAccess bool
	err := r.db.QueryRow(ctx,
		`WITH RECURSIVE ancestors AS (
		     SELECT id, parent_collection_id FROM collections WHERE id = $1 AND is_enabled = true
		     UNION ALL
		     SELECT c.id, c.parent_collection_id FROM collections c
		     INNER JOIN ancestors a ON c.id = a.parent_collection_id
		 )
		 SELECT EXISTS (
		     SELECT 1 FROM collection_access ca
		     WHERE ca.collection_id = (SELECT id FROM ancestors WHERE parent_collection_id IS NULL)
		       AND ca.user_id = $2
		 )`,
		collectionID, userID,
	).Scan(&hasAccess)
	return hasAccess, err
}

// ListAccessForUser returns the collection IDs the user has been explicitly granted access to.
func (r *CollectionRepository) ListAccessForUser(ctx context.Context, userID int64) ([]int64, error) {
	rows, err := r.db.Query(ctx,
		`SELECT collection_id FROM collection_access WHERE user_id = $1 ORDER BY collection_id`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if ids == nil {
		ids = []int64{}
	}
	return ids, err
}

// UpsertSubCollection upserts a sub-collection (directory) during scanning.
// rootCollectionID is used to inherit the collection type.
func (r *CollectionRepository) UpsertSubCollection(ctx context.Context, parentID, rootCollectionID int64, name, relativePath string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`INSERT INTO collections (parent_collection_id, name, type, relative_path)
		 VALUES ($1, $2,
		     (SELECT type FROM collections WHERE id = $3),
		     $4)
		 ON CONFLICT (relative_path) WHERE relative_path IS NOT NULL
		     DO UPDATE SET name                 = EXCLUDED.name,
		                   parent_collection_id = EXCLUDED.parent_collection_id
		 RETURNING id`,
		parentID, name, rootCollectionID, relativePath,
	).Scan(&id)
	return id, err
}

// TouchLastScanned updates the last_scanned_at timestamp for a collection.
func (r *CollectionRepository) TouchLastScanned(ctx context.Context, id int64) {
	_, _ = r.db.Exec(ctx,
		`UPDATE collections SET last_scanned_at = now() WHERE id = $1`, id,
	)
}

// ValidateTopLevel checks that all IDs exist and are top-level collections.
// Returns a user-facing error message and HTTP status, or ("", 0) if all valid.
func (r *CollectionRepository) ValidateTopLevel(ctx context.Context, ids []int64) (string, int) {
	rows, err := r.db.Query(ctx,
		`SELECT id, parent_collection_id IS NOT NULL AS is_sub
		 FROM collections WHERE id = ANY($1::bigint[])`,
		ids,
	)
	if err != nil {
		return "db error", http.StatusInternalServerError
	}
	defer rows.Close()

	found := make(map[int64]bool)
	for rows.Next() {
		var id int64
		var isSub bool
		if err := rows.Scan(&id, &isSub); err != nil {
			continue
		}
		if isSub {
			return fmt.Sprintf("collection %d is a sub-collection; access can only be granted to top-level collections", id), http.StatusBadRequest
		}
		found[id] = true
	}
	rows.Close()

	for _, id := range ids {
		if !found[id] {
			return fmt.Sprintf("collection %d does not exist", id), http.StatusBadRequest
		}
	}
	return "", 0
}

// GrantAccess grants a user access to the given top-level collections (idempotent).
func (r *CollectionRepository) GrantAccess(ctx context.Context, userID int64, collectionIDs []int64) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO collection_access (user_id, collection_id)
		 SELECT $1, unnest($2::bigint[])
		 ON CONFLICT DO NOTHING`,
		userID, collectionIDs,
	)
	return err
}

// DeleteAllAccess removes all collection access for a user.
func (r *CollectionRepository) DeleteAllAccess(ctx context.Context, userID int64) error {
	_, err := r.db.Exec(ctx,
		`DELETE FROM collection_access WHERE user_id = $1`, userID,
	)
	return err
}

// RevokeAccess removes a user's access to a specific collection.
func (r *CollectionRepository) RevokeAccess(ctx context.Context, userID, collectionID int64) error {
	_, err := r.db.Exec(ctx,
		`DELETE FROM collection_access WHERE user_id = $1 AND collection_id = $2`,
		userID, collectionID,
	)
	return err
}

// GetSlideshowItems returns all descendant photo items via recursive CTE.
func (r *CollectionRepository) GetSlideshowItems(ctx context.Context, collectionID int64) ([]models.SlideshowItem, error) {
	rows, err := r.db.Query(ctx,
		`WITH RECURSIVE collection_tree AS (
		     SELECT id FROM collections WHERE id = $1
		     UNION ALL
		     SELECT c.id FROM collections c
		     INNER JOIN collection_tree ct ON c.parent_collection_id = ct.id
		 )
		 SELECT
		     mi.id,
		     mi.title,
		     mi.mime_type,
		     pm.taken_at,
		     pm.placeholder,
		     pm.width_px,
		     pm.height_px
		 FROM media_items mi
		 JOIN photo_metadata pm ON pm.media_item_id = mi.id
		 WHERE mi.collection_id = ANY(SELECT id FROM collection_tree)
		   AND mi.missing_since IS NULL
		 ORDER BY pm.taken_at ASC NULLS LAST, mi.id ASC`,
		collectionID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.SlideshowItem])
}

// MarkMissingSince marks items in a collection that haven't been indexed since `before`.
// Returns number of rows affected.
func (r *CollectionRepository) MarkMissingSince(ctx context.Context, collectionID int64, before time.Time) (int64, error) {
	tag, err := r.db.Exec(ctx,
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
