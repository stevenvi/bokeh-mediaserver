package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// CollectionCreate inserts a new top-level collection and returns its ID.
// root_collection_id must equal the new row's own id, which isn't known until after
// the insert. We pre-fetch the next sequence value so we can supply both id and
// root_collection_id in a single statement, using OVERRIDING SYSTEM VALUE to bypass
// the GENERATED ALWAYS constraint.
func CollectionCreate(ctx context.Context, db utils.DBTX, name string, colType constants.CollectionType, relativePath string) (int64, error) {
	var id int64
	err := db.QueryRow(ctx,
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

// CollectionUpsertSubCollection upserts a sub-collection (directory) during scanning.
// rootCollectionID is used to inherit the collection type and set root_collection_id.
// Conflicts on (root_collection_id, relative_path) — the same directory scanned again
// returns the existing row's ID without modification.
func CollectionUpsertSubCollection(ctx context.Context, db utils.DBTX, parentID, rootCollectionID int64, name, relativePath string) (int64, error) {
	var id int64
	err := db.QueryRow(ctx,
		`INSERT INTO collections (parent_collection_id, root_collection_id, name, type, relative_path)
		 VALUES ($1, $2,
		     $3,
		     (SELECT type FROM collections WHERE id = $2),
		     $4)
		 ON CONFLICT (root_collection_id, relative_path) WHERE relative_path IS NOT NULL
		 DO UPDATE SET parent_collection_id = EXCLUDED.parent_collection_id
		 RETURNING id`,
		parentID, rootCollectionID, name, relativePath,
	).Scan(&id)
	return id, err
}

// CollectionGet returns an enabled collection by ID.
func CollectionGet(ctx context.Context, db utils.DBTX, id int64) (*models.Collection, error) {
	var c models.Collection
	err := db.QueryRow(ctx,
		`SELECT id, parent_collection_id, root_collection_id, relative_path, name, type
		 FROM collections WHERE id = $1 AND is_enabled = true`,
		id,
	).Scan(&c.ID, &c.ParentCollectionID, &c.RootCollectionID, &c.RelativePath, &c.Name, &c.Type)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// CollectionGetForUser returns a collection by ID, enforcing that the user has collection_access
// to the root ancestor of the collection. Returns an error (→ 404) if not found or no access.
func CollectionGetForUser(ctx context.Context, db utils.DBTX, id, userID int64) (*models.CollectionView, error) {
	var c models.CollectionView
	err := db.QueryRow(ctx,
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

// CollectionDelete removes a collection by ID and returns the number of rows affected.
// Dependent rows (child collections, media_items, collection_access) cascade automatically.
func CollectionDelete(ctx context.Context, db utils.DBTX, collectionID int64) (int64, error) {
	tag, err := db.Exec(ctx, `DELETE FROM collections WHERE id = $1`, collectionID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// CollectionIsEnabled returns whether a collection is enabled.
func CollectionIsEnabled(ctx context.Context, db utils.DBTX, id int64) (bool, error) {
	var isEnabled bool
	err := db.QueryRow(ctx,
		`SELECT is_enabled FROM collections WHERE id = $1`, id,
	).Scan(&isEnabled)
	return isEnabled, err
}

// CollectionGetRelativePath returns the relative path for a collection.
func CollectionGetRelativePath(ctx context.Context, db utils.DBTX, id int64) (string, error) {
	var relativePath string
	err := db.QueryRow(ctx,
		`SELECT COALESCE(relative_path, '') FROM collections WHERE id = $1`, id,
	).Scan(&relativePath)
	return relativePath, err
}

// CollectionsTopLevel returns all top-level collections (admin view).
func CollectionsTopLevel(ctx context.Context, db utils.DBTX) ([]models.Collection, error) {
	rows, err := db.Query(ctx,
		`SELECT
			created_at,
			parent_collection_id,
			relative_path,
			last_scanned_at,
			missing_since,
			NULL::timestamptz,
			name,
			type,
			id,
			root_collection_id,
			is_enabled,
			manual_thumbnail
		 FROM collections WHERE parent_collection_id IS NULL ORDER BY name`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.Collection])
}

// CollectionsTopLevelEnabled returns IDs of all enabled top-level collections.
func CollectionsTopLevelEnabled(ctx context.Context, db utils.DBTX) ([]int64, error) {
	rows, err := db.Query(ctx,
		`SELECT id FROM collections
		 WHERE parent_collection_id IS NULL AND is_enabled`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// CollectionsListAccessibleByUser returns enabled top-level collections the user has access to.
func CollectionsListAccessibleByUser(ctx context.Context, db utils.DBTX, userID int64) ([]models.CollectionView, error) {
	rows, err := db.Query(ctx,
		`SELECT c.parent_collection_id, c.name, c.type, c.id
		 FROM collections c
		 JOIN collection_access ca ON ca.collection_id = c.id AND ca.user_id = $1
		 WHERE c.parent_collection_id IS NULL AND c.is_enabled = true
		 ORDER BY c.name`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectionView)
}

// CollectionGetChildCollections returns direct enabled children of a collection.
func CollectionGetChildCollections(ctx context.Context, db utils.DBTX, parentID int64) ([]models.CollectionView, error) {
	rows, err := db.Query(ctx,
		`SELECT parent_collection_id, name, type, id
		 FROM collections
		 WHERE parent_collection_id = $1 AND is_enabled = true
		 ORDER BY name`,
		parentID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectionView)
}

// scanCollectionView is a custom row collector for CollectionView that maps only
// the columns that exist in the DB (Date is derived at the API layer from the name).
func scanCollectionView(row pgx.CollectableRow) (models.CollectionView, error) {
	var c models.CollectionView
	err := row.Scan(&c.ParentCollectionID, &c.Name, &c.Type, &c.ID)
	return c, err
}

// CollectionExistsAndAccessible checks both that a collection exists+enabled and that the user
// has access to its root ancestor, in a single query. Returns false if either check fails.
func CollectionExistsAndAccessible(ctx context.Context, db utils.DBTX, collectionID, userID int64) (bool, error) {
	var ok bool
	err := db.QueryRow(ctx,
		`SELECT EXISTS (
		     SELECT 1 FROM collections c
		     JOIN collection_access ca ON ca.collection_id = c.root_collection_id
		     WHERE c.id = $1 AND c.is_enabled = true AND ca.user_id = $2
		 )`,
		collectionID, userID,
	).Scan(&ok)
	return ok, err
}

// CollectionsAccessibleByUser returns the collection IDs the user has been explicitly granted access to.
func CollectionsAccessibleByUser(ctx context.Context, db utils.DBTX, userID int64) ([]int64, error) {
	rows, err := db.Query(ctx,
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

// CollectionGetUsersWithAccess returns the user IDs that have been explicitly granted access to a collection.
func CollectionGetUsersWithAccess(ctx context.Context, db utils.DBTX, collectionID int64) ([]int64, error) {
	rows, err := db.Query(ctx,
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

// CollectionTouchLastScanned updates the last_scanned_at timestamp for a collection.
func CollectionTouchLastScanned(ctx context.Context, db utils.DBTX, id int64) {
	_, _ = db.Exec(ctx,
		`UPDATE collections SET last_scanned_at = now() WHERE id = $1`, id,
	)
}

// CollectionGrantAccessToUser grants a user access to the given top-level collections (idempotent).
// Only collections with parent_collection_id IS NULL are inserted; sub-collections are silently skipped.
func CollectionGrantAccessToUser(ctx context.Context, db utils.DBTX, userID int64, collectionIDs []int64) error {
	_, err := db.Exec(ctx,
		`INSERT INTO collection_access (user_id, collection_id)
		 SELECT $1, c.id
		 FROM unnest($2::bigint[]) AS cid(id)
		 JOIN collections c ON c.id = cid.id AND c.parent_collection_id IS NULL
		 ON CONFLICT DO NOTHING`,
		userID, collectionIDs,
	)
	return err
}

// CollectionGrantAccessToUsers grants a set of users access to the given collection (idempotent).
// Only succeeds if collectionID is a top-level collection (parent_collection_id IS NULL).
func CollectionGrantAccessToUsers(ctx context.Context, db utils.DBTX, collectionID int64, userIDs []int64) error {
	_, err := db.Exec(ctx,
		`INSERT INTO collection_access (user_id, collection_id)
		 SELECT uid.id, $2
		 FROM unnest($1::bigint[]) AS uid(id)
		 WHERE EXISTS (SELECT 1 FROM collections WHERE id = $2 AND parent_collection_id IS NULL)
		 ON CONFLICT DO NOTHING`,
		userIDs, collectionID,
	)
	return err
}

// UserDeleteCollectionAccess removes a user's access to a specific collection.
func UserDeleteCollectionAccess(ctx context.Context, db utils.DBTX, userID, collectionID int64) error {
	_, err := db.Exec(ctx,
		`DELETE FROM collection_access WHERE user_id = $1 AND collection_id = $2`,
		userID, collectionID,
	)
	return err
}

// UserDeleteAllCollectionAccess removes all collection access for a user.
func UserDeleteAllCollectionAccess(ctx context.Context, db utils.DBTX, userID int64) error {
	_, err := db.Exec(ctx,
		`DELETE FROM collection_access WHERE user_id = $1`, userID,
	)
	return err
}

// CollectionGetDescendantCollectionIDs returns the IDs of all descendant collections (children, grandchildren, etc.)
// for the given collection, not including the collection itself.
func CollectionGetDescendantCollectionIDs(ctx context.Context, db utils.DBTX, collectionID int64) ([]int64, error) {
	rows, err := db.Query(ctx, `
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

// CollectionIsDescendantOf returns true if childID is a descendant of (or equal to) ancestorID.
func CollectionIsDescendantOf(ctx context.Context, db utils.DBTX, ancestorID int64, childID int64) (bool, error) {
	var ok bool
	err := db.QueryRow(ctx, `
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

// CollectionsWithNonManualThumbnailIDs returns IDs of all enabled collections that don't have manual_thumbnail set.
func CollectionsWithNonManualThumbnailIDs(ctx context.Context, db utils.DBTX) ([]int64, error) {
	rows, err := db.Query(ctx,
		`SELECT id FROM collections
		 WHERE manual_thumbnail = false AND is_enabled = true`,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// CollectionSetManualThumbnail sets or clears the manual_thumbnail flag on a collection.
func CollectionSetManualThumbnail(ctx context.Context, db utils.DBTX, collectionID int64, manual bool) error {
	_, err := db.Exec(ctx,
		`UPDATE collections SET manual_thumbnail = $2 WHERE id = $1`,
		collectionID, manual,
	)
	return err
}
