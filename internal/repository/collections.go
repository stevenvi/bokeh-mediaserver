package repository

import (
	"context"
	"fmt"
	"net/http"

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

// Create inserts a new top-level collection and returns its ID.
// root_collection_id must equal the new row's own id, which isn't known until after
// the insert. We pre-fetch the next sequence value so we can supply both id and
// root_collection_id in a single statement, using OVERRIDING SYSTEM VALUE to bypass
// the GENERATED ALWAYS constraint.
func (r *CollectionRepository) Create(ctx context.Context, name, colType, relativePath string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`WITH nid AS (
		     SELECT nextval(pg_get_serial_sequence('collections', 'id')) AS v
		 )
		 INSERT INTO collections (id, root_collection_id, name, type, relative_path)
		 OVERRIDING SYSTEM VALUE
		 SELECT v, v, $1, $2, $3 FROM nid
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
		`SELECT c.id, c.parent_collection_id, c.name, c.type
		 FROM collections c
		 WHERE c.id = $1 AND c.is_enabled = true
		   AND EXISTS (
		       SELECT 1 FROM collection_access ca
		       WHERE ca.collection_id = c.root_collection_id
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
		`SELECT EXISTS (
		     SELECT 1 FROM collections c
		     JOIN collection_access ca ON ca.collection_id = c.root_collection_id
		     WHERE c.id = $1 AND c.is_enabled = true AND ca.user_id = $2
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
// rootCollectionID is used to inherit the collection type and set root_collection_id.
func (r *CollectionRepository) UpsertSubCollection(ctx context.Context, parentID, rootCollectionID int64, name, relativePath string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`INSERT INTO collections (parent_collection_id, root_collection_id, name, type, relative_path)
		 VALUES ($1, $2,
		     $3,
		     (SELECT type FROM collections WHERE id = $2),
		     $4)
		 RETURNING id`,
		parentID, rootCollectionID, name, relativePath,
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
