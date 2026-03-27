package repository

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

type ArtistRepository struct {
	db utils.DBTX
}

func NewArtistRepository(db utils.DBTX) *ArtistRepository {
	return &ArtistRepository{db: db}
}

// Upsert inserts a new artist or returns the existing ID if the name already exists.
func (r *ArtistRepository) Upsert(ctx context.Context, name string) (int64, error) {
	sortName := GenerateSortName(name)
	var id int64
	err := r.db.QueryRow(ctx,
		`INSERT INTO artists (name, sort_name)
		 VALUES ($1, $2)
		 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name  -- No-op update to make RETURNING work on conflict
		 RETURNING id`,
		name, sortName,
	).Scan(&id)
	return id, err
}

// GetByID returns an artist by ID.
func (r *ArtistRepository) GetByID(ctx context.Context, id int64) (*models.Artist, error) {
	var artist models.Artist
	err := r.db.QueryRow(ctx,
		`SELECT id, name, sort_name, manual_image, created_at
		 FROM artists WHERE id = $1`,
		id,
	).Scan(&artist.ID, &artist.Name, &artist.SortName, &artist.ManualImage, &artist.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &artist, nil
}

// ListByCollection returns artists who own at least one album in the given root
// collection. "Various Artists" appears here when compilation albums exist.
// Results are ordered by sort_name.
func (r *ArtistRepository) ListByCollection(ctx context.Context, collectionID int64, limit, offset int, search string) ([]models.ArtistSummary, int, error) {
	baseQuery := `
		WITH artist_ids AS (
			SELECT DISTINCT al.artist_id AS aid
			FROM audio_albums al
			WHERE al.root_collection_id = $1
			  AND al.artist_id IS NOT NULL
		)`

	// Count and list queries have different parameter positions for search ($2 vs $4),
	// so they require separate args slices.
	var countSearchClause, listSearchClause string
	countArgs := []any{collectionID}
	listArgs := []any{collectionID, limit, offset}

	if search != "" {
		countSearchClause = ` AND a.search_vector @@ plainto_tsquery('simple', $2)`
		countArgs = append(countArgs, search)
		listSearchClause = ` AND a.search_vector @@ plainto_tsquery('simple', $4)`
		listArgs = append(listArgs, search)
	}

	// Get total count
	countQuery := baseQuery + `
		SELECT COUNT(*) FROM artist_ids s
		JOIN artists a ON a.id = s.aid
		WHERE s.aid IS NOT NULL` + countSearchClause

	var total int
	err := r.db.QueryRow(ctx, countQuery, countArgs...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Get page
	listQuery := baseQuery + `
		SELECT a.id, a.name, a.sort_name
		FROM artist_ids s
		JOIN artists a ON a.id = s.aid
		WHERE s.aid IS NOT NULL` + listSearchClause + `
		ORDER BY a.sort_name ASC
		LIMIT $2 OFFSET $3`

	rows, err := r.db.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, 0, err
	}
	artists, err := pgx.CollectRows(rows, pgx.RowToStructByPos[models.ArtistSummary])
	if err != nil {
		return nil, 0, err
	}
	return artists, total, nil
}

// ListAlbumsByArtist returns album summaries for a given artist within a collection, ordered by year.
// Includes albums the artist owns (al.artist_id = artistID) and any album they appear on as a
// track artist (e.g. compilation albums), so a band's page shows compilations they contributed to.
func (r *ArtistRepository) ListAlbumsByArtist(ctx context.Context, artistID, collectionID int64) ([]models.AlbumSummary, error) {
	rows, err := r.db.Query(ctx,
		`SELECT
			al.id,
			al.name,
			al.year,
			COUNT(am.media_item_id),
			COALESCE(SUM(am.duration_seconds), 0)
		FROM audio_albums al
		JOIN audio_metadata am ON am.album_id = al.id
		JOIN media_items mi ON mi.id = am.media_item_id
		WHERE al.root_collection_id = $2
		  AND mi.missing_since IS NULL AND mi.hidden_at IS NULL
		  AND (
		      al.artist_id = $1
		      OR EXISTS (
		          SELECT 1 FROM audio_metadata am2
		          WHERE am2.album_id = al.id AND am2.artist_id = $1
		      )
		  )
		GROUP BY al.id, al.name, al.year
		ORDER BY al.year ASC NULLS LAST, al.name ASC`,
		artistID, collectionID,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[models.AlbumSummary])
}

// SetManualImage marks an artist as having a manually uploaded image.
func (r *ArtistRepository) SetManualImage(ctx context.Context, id int64, manual bool) error {
	_, err := r.db.Exec(ctx,
		`UPDATE artists SET manual_image = $2 WHERE id = $1`,
		id, manual,
	)
	return err
}

// ListArtistIDsWithoutManualImage returns IDs of artists that have manual_image = false
// and have at least one non-compilation album where they are the album artist.
// Artists who only appear on compilation albums are excluded.
func (r *ArtistRepository) ListArtistIDsWithoutManualImage(ctx context.Context) ([]int64, error) {
	rows, err := r.db.Query(ctx,
		`SELECT DISTINCT a.id
		 FROM artists a
		 JOIN audio_albums al ON al.artist_id = a.id
		 WHERE a.manual_image = false
		   AND al.is_compilation = false`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// GenerateSortName creates a sort-friendly name by moving common articles to the end.
func GenerateSortName(name string) string {
	lower := strings.ToLower(name)
	for _, article := range []string{"the ", "a ", "an "} {
		if strings.HasPrefix(lower, article) {
			prefix := name[:len(article)]
			rest := name[len(article):]
			return rest + ", " + strings.TrimSpace(prefix)
		}
	}
	return name
}
