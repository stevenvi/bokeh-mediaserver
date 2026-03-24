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
		        is_enabled, manual_cover, last_scanned_at, created_at
		 FROM collections WHERE parent_collection_id IS NULL ORDER BY name`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (models.Collection, error) {
		var c models.Collection
		err := row.Scan(&c.ID, &c.Name, &c.Type, &c.RelativePath,
			&c.IsEnabled, &c.ManualCover, &c.LastScannedAt, &c.CreatedAt)
		return c, err
	})
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
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (models.Collection, error) {
		var c models.Collection
		err := row.Scan(&c.ID, &c.ParentCollectionID, &c.Name, &c.Type)
		return c, err
	})
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

// ExistsEnabledWithAccess checks both that a collection exists+enabled and that the user
// has access to its root ancestor, in a single query. Returns false if either check fails.
func (r *CollectionRepository) ExistsEnabledWithAccess(ctx context.Context, collectionID, userID int64) (bool, error) {
	var ok bool
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
	).Scan(&ok)
	return ok, err
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

// ListUsersWithAccess returns the user IDs that have been explicitly granted access to a collection.
func (r *CollectionRepository) ListUsersWithAccess(ctx context.Context, collectionID int64) ([]int64, error) {
	rows, err := r.db.Query(ctx,
		`SELECT user_id FROM collection_access WHERE collection_id = $1 ORDER BY user_id`,
		collectionID,
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
			return "db error", http.StatusInternalServerError
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

// GrantAccessToUsers grants a set of users access to the given collection (idempotent).
func (r *CollectionRepository) GrantAccessToUsers(ctx context.Context, collectionID int64, userIDs []int64) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO collection_access (user_id, collection_id)
		 SELECT unnest($1::bigint[]), $2
		 ON CONFLICT DO NOTHING`,
		userIDs, collectionID,
	)
	return err
}

// DeleteAllAccess removes all collection access for a user.
// Delete removes a collection by ID and returns the number of rows affected.
// Dependent rows (child collections, media_items, collection_access) cascade automatically.
func (r *CollectionRepository) Delete(ctx context.Context, collectionID int64) (int64, error) {
	tag, err := r.db.Exec(ctx, `DELETE FROM collections WHERE id = $1`, collectionID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (r *CollectionRepository) DeleteAllAccess(ctx context.Context, userID int64) error {
	_, err := r.db.Exec(ctx,
		`DELETE FROM collection_access WHERE user_id = $1`, userID,
	)
	return err
}

// ListDescendantIDs returns the IDs of all descendant collections (children, grandchildren, etc.)
// for the given collection, not including the collection itself.
func (r *CollectionRepository) ListDescendantIDs(ctx context.Context, collectionID int64) ([]int64, error) {
	rows, err := r.db.Query(ctx, `
		WITH RECURSIVE tree AS (
			SELECT id FROM collections WHERE parent_collection_id = $1
			UNION ALL
			SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
		)
		SELECT id FROM tree`, collectionID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// RevokeAccess removes a user's access to a specific collection.
func (r *CollectionRepository) RevokeAccess(ctx context.Context, userID, collectionID int64) error {
	_, err := r.db.Exec(ctx,
		`DELETE FROM collection_access WHERE user_id = $1 AND collection_id = $2`,
		userID, collectionID,
	)
	return err
}

// SlideshowQuery holds parameters for a slideshow page fetch.
type SlideshowQuery struct {
	CollectionID int64
	PageSize     int // the repository adds +1 internally
	Ascending    bool
	Recursive    bool

	// Cursor fields
	HasCursor     bool
	CursorTime    *time.Time
	CursorID      int64
	IncludeCursor bool // include the item the cursor points to in results (inclusive/exclusive)
}

// GetSlideshowItems returns a page of photo items via keyset pagination.
// When Recursive is true, items from all descendant collections are included.
// The caller receives pageSize+1 rows and must trim to detect whether a next page exists.
func (r *CollectionRepository) GetSlideshowItems(ctx context.Context, q SlideshowQuery) ([]models.SlideshowItem, error) {
	args := []any{q.CollectionID, q.PageSize + 1}
	addArg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	// Build cursor clause
	var cursorClause string
	if q.HasCursor {
		gt, lt := ">", "<"
		if q.IncludeCursor {
			gt, lt = ">=", "<="
		}
		if q.Ascending {
			if q.CursorTime != nil {
				ts, id := addArg(*q.CursorTime), addArg(q.CursorID)
				cursorClause = fmt.Sprintf(
					`AND (pm.created_at > %s OR (pm.created_at = %s AND mi.id %s %s) OR pm.created_at IS NULL)`,
					ts, ts, gt, id)
			} else {
				id := addArg(q.CursorID)
				cursorClause = fmt.Sprintf(`AND (pm.created_at IS NULL AND mi.id %s %s)`, gt, id)
			}
		} else {
			if q.CursorTime != nil {
				ts, id := addArg(*q.CursorTime), addArg(q.CursorID)
				cursorClause = fmt.Sprintf(
					`AND (pm.created_at < %s OR (pm.created_at = %s AND mi.id %s %s) OR pm.created_at IS NULL)`,
					ts, ts, lt, id)
			} else {
				id := addArg(q.CursorID)
				cursorClause = fmt.Sprintf(`AND (pm.created_at IS NULL AND mi.id %s %s)`, lt, id)
			}
		}
	}

	dir := "ASC"
	if !q.Ascending {
		dir = "DESC"
	}

	// Build collection filter: recursive CTE or direct match
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
		SELECT
		    mi.id,
		    mi.title,
		    mi.mime_type,
		    pm.created_at,
		    pm.placeholder,
		    pm.width_px,
		    pm.height_px
		FROM media_items mi
		JOIN photo_metadata pm ON pm.media_item_id = mi.id
		WHERE %s
		  AND mi.missing_since IS NULL
		  AND mi.hidden_at IS NULL
		  %s
		ORDER BY pm.created_at %s NULLS LAST, mi.id %s
		LIMIT $2`,
		cte, collectionFilter, cursorClause, dir, dir,
	)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.SlideshowItem])
}

// SlideshowItemPosition holds the sort-key fields needed to construct a cursor for a given item.
type SlideshowItemPosition struct {
	ID           int64
	CollectionID int64
	CreatedAt    *time.Time
}

// GetSlideshowItemPosition returns the sort-key fields for a media item, used to construct
// a cursor when the client specifies a start item. Returns nil if the item does not exist
// or is hidden/missing.
func (r *CollectionRepository) GetSlideshowItemPosition(ctx context.Context, itemID int64) (*SlideshowItemPosition, error) {
	var pos SlideshowItemPosition
	err := r.db.QueryRow(ctx, `
		SELECT mi.id, mi.collection_id, pm.created_at
		FROM media_items mi
		JOIN photo_metadata pm ON pm.media_item_id = mi.id
		WHERE mi.id = $1
		  AND mi.missing_since IS NULL
		  AND mi.hidden_at IS NULL`,
		itemID,
	).Scan(&pos.ID, &pos.CollectionID, &pos.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &pos, nil
}

// IsDescendantCollection returns true if childID is a descendant of (or equal to) ancestorID.
func (r *CollectionRepository) IsDescendantCollection(ctx context.Context, ancestorID int64, childID int64) (bool, error) {
	var ok bool
	err := r.db.QueryRow(ctx, `
		WITH RECURSIVE collection_tree AS (
		    SELECT id FROM collections WHERE id = $1
		    UNION ALL
		    SELECT c.id FROM collections c
		    INNER JOIN collection_tree ct ON c.parent_collection_id = ct.id
		)
		SELECT EXISTS(SELECT 1 FROM collection_tree WHERE id = $2)`,
		ancestorID, childID,
	).Scan(&ok)
	return ok, err
}

// GetSlideshowMetadata returns per-month item counts for the slideshow scrollbar.
// Items with NULL created_at are excluded.
func (r *CollectionRepository) GetSlideshowMetadata(ctx context.Context, collectionID int64, recursive bool) ([]models.SlideshowMonthCount, error) {
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

	rows, err := r.db.Query(ctx, query, collectionID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.SlideshowMonthCount])
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

// ListCollectionswithNonManualCoverIDs returns IDs of all enabled collections that don't have manual_cover set.
func (r *CollectionRepository) ListCollectionswithNonManualCoverIDs(ctx context.Context) ([]int64, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id FROM collections
		 WHERE manual_cover = false AND is_enabled = true`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// SetManualCover sets or clears the manual_cover flag on a collection.
func (r *CollectionRepository) SetManualCover(ctx context.Context, collectionID int64, manual bool) error {
	_, err := r.db.Exec(ctx,
		`UPDATE collections SET manual_cover = $2 WHERE id = $1`,
		collectionID, manual,
	)
	return err
}
