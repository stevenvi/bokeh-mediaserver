// Package repository search.go: full-text search across photos, videos, and audio.
//
// All endpoints share the same shape:
//   - websearch_to_tsquery for query parsing (handles quotes, OR, -term, and
//     never errors on garbage input)
//   - filter via pre-existing GIN tsvector indexes (idx_media_items_search,
//     idx_photo_metadata_search, idx_artists_search, idx_audio_albums_search)
//   - sort by ts_rank DESC with media_items.id (or table id) ASC tiebreaker so
//     offset pagination is stable when ranks tie
//   - access-scope every query through collection_access
package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
)

// SearchParams is the common request shape parsed by the API layer and passed
// to every search repository function.
type SearchParams struct {
	Query  string // raw user query, non-empty (the API layer rejects empty)
	Offset int
	Limit  int
}

// ─── Video search ────────────────────────────────────────────────────────────

// VideoSearchHit is one row in the video search response.
type VideoSearchHit struct {
	Date           *string `json:"date,omitempty"`
	Title          string  `json:"title"`
	CollectionName string  `json:"collection_name"`
	collectionType string  // not exposed; used for partitioning in Go
	ID             int64   `json:"id"`
	CollectionID   int64   `json:"collection_id"`
}

// SearchVideos searches video titles across both video:movie and video:home_movie
// collections the user can access. Returns the two slices already partitioned by
// collection type.
func SearchVideos(ctx context.Context, db utils.DBTX, userID int64, p SearchParams) (movies, homeMovies []VideoSearchHit, err error) {
	rows, err := db.Query(ctx, `
		SELECT mi.id, mi.title, mi.collection_id, c.name, c.type, vm.date_string
		FROM media_items mi
		JOIN collections c ON c.id = mi.collection_id
		LEFT JOIN video_metadata vm ON vm.media_item_id = mi.id
		WHERE mi.search_vector @@ websearch_to_tsquery('english', $1)
		  AND c.type IN ($4, $5)
		  AND EXISTS (
		      SELECT 1 FROM collection_access ca
		      WHERE ca.collection_id = c.root_collection_id AND ca.user_id = $2
		  )
		  AND mi.missing_since IS NULL AND mi.hidden_at IS NULL
		ORDER BY ts_rank(mi.search_vector, websearch_to_tsquery('english', $1)) DESC, mi.id ASC
		LIMIT $3 OFFSET $6`,
		p.Query, userID, p.Limit,
		string(constants.CollectionTypeMovie), string(constants.CollectionTypeHomeMovie),
		p.Offset,
	)
	if err != nil {
		return nil, nil, err
	}
	hits, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (VideoSearchHit, error) {
		var h VideoSearchHit
		err := row.Scan(&h.ID, &h.Title, &h.CollectionID, &h.CollectionName, &h.collectionType, &h.Date)
		return h, err
	})
	if err != nil {
		return nil, nil, err
	}
	movies = make([]VideoSearchHit, 0)
	homeMovies = make([]VideoSearchHit, 0)
	for _, h := range hits {
		switch h.collectionType {
		case string(constants.CollectionTypeMovie):
			movies = append(movies, h)
		case string(constants.CollectionTypeHomeMovie):
			homeMovies = append(homeMovies, h)
		}
	}
	return movies, homeMovies, nil
}

// ─── Photo search ────────────────────────────────────────────────────────────

// SearchPhotos returns photo items matching `p.Query` across all image:photo collections
// the user can access, ranked by combined relevance of the title FTS (on
// media_items.search_vector) and the metadata FTS (on photo_metadata.search_vector).
// Result struct is identical to repository.PhotoItems so clients reuse their grid.
func SearchPhotos(ctx context.Context, db utils.DBTX, userID int64, p SearchParams) ([]models.PhotoItem, error) {
	rows, err := db.Query(ctx, `
		SELECT mi.id, mi.title, mi.mime_type,
		       pm.created_at, pm.width_px, pm.height_px,
		       pm.camera_make, pm.camera_model, pm.lens_model,
		       pm.shutter_speed, pm.aperture, pm.iso,
		       pm.focal_length_mm, pm.focal_length_35mm_equiv,
		       pm.variants_generated_at
		FROM media_items mi
		JOIN photo_metadata pm ON pm.media_item_id = mi.id
		JOIN collections c ON c.id = mi.collection_id
		WHERE (mi.search_vector @@ websearch_to_tsquery('english', $1)
		    OR pm.search_vector @@ websearch_to_tsquery('english', $1))
		  AND c.type = $4
		  AND EXISTS (
		      SELECT 1 FROM collection_access ca
		      WHERE ca.collection_id = c.root_collection_id AND ca.user_id = $2
		  )
		  AND mi.missing_since IS NULL AND mi.hidden_at IS NULL
		ORDER BY (
		    ts_rank(mi.search_vector, websearch_to_tsquery('english', $1)) +
		    ts_rank(pm.search_vector, websearch_to_tsquery('english', $1))
		) DESC, mi.id ASC
		LIMIT $3 OFFSET $5`,
		p.Query, userID, p.Limit, string(constants.CollectionTypePhoto), p.Offset,
	)
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

// ─── Audio: artists & shows ──────────────────────────────────────────────────

// AudioArtistSearchHit is one row in the artists/shows search response.
type AudioArtistSearchHit struct {
	Name           string `json:"name"`
	collectionType string // not exposed; partitioning only
	ID             int64  `json:"id"`
	CollectionID   int64  `json:"collection_id"`
}

// SearchAudioArtists searches artists by name across both audio:music (returned
// as "artists") and audio:show (returned as "shows") collections the user can
// access. An artist is bucketed by the type of the collection that owns them —
// detected via their albums (music) or via their tracks' collection (show).
func SearchAudioArtists(ctx context.Context, db utils.DBTX, userID int64, p SearchParams) (artists, shows []AudioArtistSearchHit, err error) {
	rows, err := db.Query(ctx, fmt.Sprintf(`
		WITH artist_collection AS (
		    -- music: derive from albums
		    SELECT DISTINCT al.artist_id AS id, al.root_collection_id AS coll_id, %[1]s AS coll_type
		    FROM audio_albums al
		    WHERE al.artist_id IS NOT NULL
		      AND EXISTS (
		          SELECT 1 FROM collection_access ca
		          WHERE ca.collection_id = al.root_collection_id AND ca.user_id = $2
		      )
		    UNION
		    -- shows: derive from tracks (audio:show collections have no albums)
		    SELECT DISTINCT am.artist_id AS id, c.root_collection_id AS coll_id, c.type AS coll_type
		    FROM audio_metadata am
		    JOIN media_items mi ON mi.id = am.media_item_id
		    JOIN collections c ON c.id = mi.collection_id
		    WHERE am.artist_id IS NOT NULL
		      AND c.type = %[2]s
		      AND mi.missing_since IS NULL AND mi.hidden_at IS NULL
		      AND EXISTS (
		          SELECT 1 FROM collection_access ca
		          WHERE ca.collection_id = c.root_collection_id AND ca.user_id = $2
		      )
		)
		-- An artist could conceivably belong to several root collections (e.g. the
		-- same artist scanned into two separate music libraries). Collapse to one
		-- row per artist; pick any (MIN) collection_id/type — partition by type is
		-- what matters for the response shape, and that is consistent within the
		-- same artist in practice.
		SELECT a.id, a.name, MIN(ac.coll_id), MIN(ac.coll_type),
		       ts_rank(a.search_vector, websearch_to_tsquery('simple', $1)) AS rank
		FROM artists a
		JOIN artist_collection ac ON ac.id = a.id
		WHERE a.search_vector @@ websearch_to_tsquery('simple', $1)
		GROUP BY a.id, a.name, a.search_vector
		ORDER BY rank DESC, a.id ASC
		LIMIT $3 OFFSET $4`,
		"'"+string(constants.CollectionTypeMusic)+"'",
		"'"+string(constants.CollectionTypeAudioShow)+"'",
	),
		p.Query, userID, p.Limit, p.Offset,
	)
	if err != nil {
		return nil, nil, err
	}
	hits, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (AudioArtistSearchHit, error) {
		var h AudioArtistSearchHit
		var rank float64
		err := row.Scan(&h.ID, &h.Name, &h.CollectionID, &h.collectionType, &rank)
		return h, err
	})
	if err != nil {
		return nil, nil, err
	}
	artists = make([]AudioArtistSearchHit, 0)
	shows = make([]AudioArtistSearchHit, 0)
	for _, h := range hits {
		switch h.collectionType {
		case string(constants.CollectionTypeMusic):
			artists = append(artists, h)
		case string(constants.CollectionTypeAudioShow):
			shows = append(shows, h)
		}
	}
	return artists, shows, nil
}

// ─── Audio: albums ───────────────────────────────────────────────────────────

// AudioAlbumSearchHit is one row in the album search response.
type AudioAlbumSearchHit struct {
	Year         *int16 `json:"year,omitempty"`
	Name         string `json:"name"`
	ID           int64  `json:"id"`
	CollectionID int64  `json:"collection_id"`
}

// SearchAudioAlbums searches album names. Albums only exist in audio:music
// collections, so no partitioning is needed.
func SearchAudioAlbums(ctx context.Context, db utils.DBTX, userID int64, p SearchParams) ([]AudioAlbumSearchHit, error) {
	rows, err := db.Query(ctx, `
		SELECT al.year, al.name, al.id, al.root_collection_id
		FROM audio_albums al
		WHERE al.search_vector @@ websearch_to_tsquery('simple', $1)
		  AND EXISTS (
		      SELECT 1 FROM collection_access ca
		      WHERE ca.collection_id = al.root_collection_id AND ca.user_id = $2
		  )
		ORDER BY ts_rank(al.search_vector, websearch_to_tsquery('simple', $1)) DESC, al.id ASC
		LIMIT $3 OFFSET $4`,
		p.Query, userID, p.Limit, p.Offset,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[AudioAlbumSearchHit])
}

// ─── Audio: tracks ───────────────────────────────────────────────────────────

// AudioTrackSearchHit is one row in the track search response.
type AudioTrackSearchHit struct {
	AlbumID         *int64   `json:"album_id,omitempty"`
	DurationSeconds *float64 `json:"duration_seconds,omitempty"`
	ArtistName      *string  `json:"artist_name,omitempty"`
	AlbumName       *string  `json:"album_name,omitempty"`
	Title           string   `json:"title"`
	CollectionType  string   `json:"collection_type"`
	ID              int64    `json:"id"`
	CollectionID    int64    `json:"collection_id"`
}

// SearchAudioTracks searches track titles (via media_items.search_vector) across
// audio:music and audio:show collections the user can access.
func SearchAudioTracks(ctx context.Context, db utils.DBTX, userID int64, p SearchParams) ([]AudioTrackSearchHit, error) {
	rows, err := db.Query(ctx, `
		SELECT am.album_id, am.duration_seconds, ar.name, al.name,
		       mi.title, c.type, mi.id, mi.collection_id
		FROM media_items mi
		JOIN audio_metadata am ON am.media_item_id = mi.id
		JOIN collections c ON c.id = mi.collection_id
		LEFT JOIN artists ar ON ar.id = am.artist_id
		LEFT JOIN audio_albums al ON al.id = am.album_id
		WHERE mi.search_vector @@ websearch_to_tsquery('english', $1)
		  AND c.type IN ($4, $5)
		  AND EXISTS (
		      SELECT 1 FROM collection_access ca
		      WHERE ca.collection_id = c.root_collection_id AND ca.user_id = $2
		  )
		  AND mi.missing_since IS NULL AND mi.hidden_at IS NULL
		ORDER BY ts_rank(mi.search_vector, websearch_to_tsquery('english', $1)) DESC, mi.id ASC
		LIMIT $3 OFFSET $6`,
		p.Query, userID, p.Limit,
		string(constants.CollectionTypeMusic), string(constants.CollectionTypeAudioShow),
		p.Offset,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[AudioTrackSearchHit])
}
