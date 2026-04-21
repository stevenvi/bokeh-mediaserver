package models

import (
	"encoding/json"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
)

type Collection struct {
	CreatedAt          time.Time  `json:"created_at"`
	ParentCollectionID *int64     `json:"parent_collection_id,omitempty"`
	RelativePath       *string    `json:"relative_path,omitempty"`
	LastScannedAt      *time.Time `json:"last_scanned_at,omitempty"`
	MissingSince       *time.Time `json:"missing_since,omitempty"`
	Date               *time.Time `json:"date,omitempty"`
	Name               string     `json:"name"`
	Type               string     `json:"type"`
	ID                 int64      `json:"id"`
	IsEnabled          bool       `json:"is_enabled"`
	ManualThumbnail    bool       `json:"manual_thumbnail"`
}

// CollectionView is the user-facing detail view of a single collection.
type CollectionView struct {
	ParentCollectionID *int64                   `json:"parent_collection_id,omitempty"`
	Date               *time.Time               `json:"date,omitempty"`
	Name               string                   `json:"name"`
	Type               constants.CollectionType `json:"type"`
	ID                 int64                    `json:"id"`
}

type MediaItem struct {
	IndexedAt     time.Time      `json:"indexed_at"`
	CreatedAt     time.Time      `json:"created_at"`
	Ordinal       *int           `json:"ordinal,omitempty"`
	MissingSince  *time.Time     `json:"missing_since,omitempty"`
	Photo         *PhotoMetadata `json:"photo,omitempty"` // Populated when fetching item detail
	Title         string         `json:"title"`
	RelativePath  string         `json:"-"` // never expose filesystem paths to clients
	FileHash      string         `json:"-"`
	MimeType      string         `json:"mime_type"`
	ID            int64          `json:"id"`
	CollectionID  int64          `json:"collection_id"`
	FileSizeBytes int64          `json:"file_size_bytes"`
}

type PhotoMetadata struct {
	WidthPx              *int       `json:"width_px,omitempty"`
	HeightPx             *int       `json:"height_px,omitempty"`
	CreatedAt            *time.Time `json:"created_at,omitempty"`
	CameraMake           *string    `json:"camera_make,omitempty"`
	CameraModel          *string    `json:"camera_model,omitempty"`
	LensModel            *string    `json:"lens_model,omitempty"`
	ShutterSpeed         *string    `json:"shutter_speed,omitempty"`
	Aperture             *float64   `json:"aperture,omitempty"`
	ISO                  *int       `json:"iso,omitempty"`
	FocalLengthMM        *float64   `json:"focal_length_mm,omitempty"`
	FocalLength35mmEquiv *float64   `json:"focal_length_35mm_equiv,omitempty"`
	ColorSpace           *string    `json:"color_space,omitempty"`
	Description          *string    `json:"description,omitempty"`
	VariantsGeneratedAt  *time.Time `json:"variants_generated_at,omitempty"`
}

func (pm *PhotoMetadata) RemapLensModel() {
	if pm.LensModel == nil {
		return
	}

	// handle specific lens model names to specific remapped values
	var model string = *pm.LensModel
	switch *pm.LensModel {
	// Ambiguous
	// TODO: This is not strong enough to reliably identify these lenses, look at more exif to verify!!
	case "18.0-55.0 mm f/3.5-5.6":
		if *pm.CameraMake == "NIKON CORPORATION" {
			model = "NIKKOR 18-55mm F3.5-5.6 DC Macro OS HSM"
		}
	case "18.0-300.0 mm f/3.5-6.3":
		if *pm.CameraMake == "NIKON CORPORATION" {
			model = "Sigma Contemporary 18-300mm F3.5-6.3 DC Macro OS HSM"
		}

	// Apple
	case "iPad mini back camera 3mm f/1.8":
		model = "Wide Camera 29mm F1.8"
	case "iPhone 13 mini back dual wide camera 1.54mm f/2.4":
		model = "Ultra Wide Camera 14mm F2.4"
	case "iPhone 13 mini back dual wide camera 5.1mm f/1.6":
		model = "Wide Camera 26mm F1.6"
	case "iPhone 13 mini front camera 2.71mm f/2.2":
		model = "Front Camera 23mm F2.2"
	case "iPhone 14 Pro back triple camera 2.22mm f/2.2":
		model = "Ultra Wide Camera 14mm F2.2"
	case "iPhone 14 Pro back triple camera 6.86mm f/1.78",
		"iPhone 14 Pro back camera 6.86mm f/1.78",
		"iPhone 15 Pro Max back triple camera 6.86mm f/1.78":
		model = "Wide Camera 24mm F1.8"
	case "iPhone 14 Pro back triple camera 9mm f/2.8":
		model = "Telephoto Camera 77mm F2.8"
	case "iPhone 14 Pro front camera 2.69mm f/1.9":
		model = "Front Camera 30mm F1.9"

	// Nikon/Nikkor
	case "AF-S DX Nikkor 35mm f/1.8G":
		// This is a mismatched identification from exiftool
		model = "Nikkor 28-70mm F??"

	// Rokinon

	// Sigma
	case "YYY":
		model = "Sigma Art 18-35mm F1.8 DC HSM"
	case "14-24mm F2.8 DG DN | Art 019":
		model = "Sigma Art 14-24mm F2.8 DG DN"
	case "Sigma 35mm F1.4 DG DN | A (Sony E)":
		model = "Sigma Art 35mm F1.4 DG DN"
	case "85mm F1.4 DG DN | Art 020",
		"Sigma 85mm F1.4 DG DN | A (Sony E)":
		model = "Sigma Art 85mm F1.4 DG DN"

	// Sony
	case "Sony FE 200–600mm F5.6–6.3 G OSS (SEL200600G)", 
		"FE 200-600mm F5.6-6.3 G OSS":
		model = "Sony 200-600mm F5.6-6.3 G OSS"

	// TAMRON
	case "E 28-200mm F2.8-5.6 A071":
		model = "TAMRON 28-200mm F2.8-5.6 Di III RXD"
	case "E 70-180mm F2.8 A065":
		model = "TAMRON 70-180mm F2.8 Di III VC VXD G2"
	default:
		return
	}

	pm.LensModel = &model
}

func (pm *PhotoMetadata) RemapCameraModel() {
	if (pm.CameraModel == nil) {
		return
	}

	var model string
	switch *pm.CameraModel {
	// Sony/Alpha
	case "ILCE-7R3":
		model = "Sony A7 III"
	case "ILCE-7RM3":
		model = "Sony A7R III"
	case "ILCE-7C":
		model = "Sony A7C"
	default:
		model = *pm.CameraModel
	}

	pm.CameraModel = &model
}

type Job struct {
	QueuedAt        time.Time  `json:"queued_at"`
	RelatedID       *int64     `json:"related_id,omitempty"`
	RelatedType     *string    `json:"related_type,omitempty"`
	ParentJobID     *int64     `json:"parent_job_id,omitempty"`
	Log             *string    `json:"log,omitempty"`
	ErrorMessage    *string    `json:"error_message,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	Type            string     `json:"type"`
	Status          string     `json:"status"`
	ID              int64      `json:"id"`
	CurrentStep     int        `json:"current_step"`
	SubjobsEnqueued int        `json:"subjobs_enqueued"`
}

type User struct {
	CreatedAt time.Time `json:"created_at"`
	Name      string    `json:"name"`
	ID        int64     `json:"id"`
	IsAdmin   bool      `json:"is_admin"`
}

// VideoMetadata stores technical and descriptive data for a video media item.
type VideoMetadata struct {
	DurationSeconds *int       `json:"duration_seconds,omitempty"`
	Width           *int       `json:"width,omitempty"`
	Height          *int       `json:"height,omitempty"`
	BitrateKbps     *int       `json:"bitrate_kbps,omitempty"`
	VideoCodec      *string    `json:"video_codec,omitempty"`
	AudioCodec      *string    `json:"audio_codec,omitempty"`
	TranscodedAt    *time.Time `json:"transcoded_at,omitempty"`
	Date            *time.Time `json:"date,omitempty"`
	EndDate         *time.Time `json:"end_date,omitempty"`
	Author          *string    `json:"author,omitempty"`
	BookmarkSeconds *int       `json:"bookmark_seconds,omitempty"` // populated per-user at query time; nil means no bookmark
	ManualThumbnail bool       `json:"manual_thumbnail"`
}

// VideoBookmark stores a user's playback position for a video item.
type VideoBookmark struct {
	LastWatchedAt   time.Time `json:"last_watched_at"`
	UserID          int64     `json:"user_id"`
	MediaItemID     int64     `json:"media_item_id"`
	PositionSeconds int       `json:"position_seconds"`
}

// MediaItemView is the user-facing projection of a media item in a collection listing.
type MediaItemView struct {
	Ordinal *int `json:"ordinal,omitempty"`
	// Populated when fetching item detail
	Photo *PhotoMetadata `json:"photo,omitempty"`
	Video *VideoMetadata `json:"video,omitempty"`

	Title    string `json:"title"`
	MimeType string `json:"mime_type"`
	ID       int64  `json:"id"`
}

// SlideshowItem is a projection used by the slideshow endpoint.
type SlideshowItem struct {
	CreatedAt *time.Time `json:"created_at,omitempty"`
	WidthPx   *int       `json:"width_px,omitempty"`
	HeightPx  *int       `json:"height_px,omitempty"`
	Title     string     `json:"title"`
	MimeType  string     `json:"mime_type"`
	ID        int64      `json:"id"`
}

// SlideshowMonthCount is one row of the slideshow metadata aggregation.
type SlideshowMonthCount struct {
	Year  int `json:"year"`
	Month int `json:"month"`
	Count int `json:"count"`
}

// Device is the full internal representation of a device row.
// device_uuid is intentionally excluded from all API responses.
type Device struct {
	CreatedAt                time.Time
	LastSeenAt               time.Time
	RefreshTokenHash         *string
	PreviousRefreshTokenHash *string
	ExpiresAt                *time.Time
	BannedAt                 *time.Time
	DeviceUUID               string
	DeviceName               string
	AccessHistory            json.RawMessage
	ID                       int64
	UserID                   int64
}

// DeviceView is the API-facing projection of a device (no device_uuid, no token fields).
// BannedAt non-nil means the device is banned.
type DeviceView struct {
	LastSeenAt    time.Time       `json:"last_seen_at"`
	CreatedAt     time.Time       `json:"created_at"`
	BannedAt      *time.Time      `json:"banned_at,omitempty"`
	DeviceName    string          `json:"device_name"`
	AccessHistory json.RawMessage `json:"access_history"`
	ID            int64           `json:"id"`
}

// AccessHistoryEntry is one entry in a device's access_history JSONB array.
type AccessHistoryEntry struct {
	LastSeen time.Time `json:"last_seen"`
	IP       string    `json:"ip"`
	Agent    string    `json:"agent"`
}

// Artist represents a music artist (materialized from audio metadata).
type Artist struct {
	CreatedAt       time.Time `json:"created_at"`
	Name            string    `json:"name"`
	SortName        string    `json:"sort_name"`
	ID              int64     `json:"id"`
	ManualThumbnail bool      `json:"manual_thumbnail"`
}

// ArtistSummary is the user-facing view of an artist in a listing.
type ArtistSummary struct {
	Name     string `json:"name"`
	SortName string `json:"sort_name"`
	ID       int64  `json:"id"`
}

// AudioAlbum is the full representation of an audio album, keyed by tag identity.
type AudioAlbum struct {
	CreatedAt        time.Time `json:"created_at"`
	ArtistID         *int64    `json:"artist_id,omitempty"`
	Year             *int16    `json:"year,omitempty"`
	Genre            *string   `json:"genre,omitempty"`
	Name             string    `json:"name"`
	ID               int64     `json:"id"`
	RootCollectionID int64     `json:"root_collection_id"`
	IsCompilation    bool      `json:"is_compilation"`
	ManualCover      bool      `json:"manual_cover"`
}

// AudioMetadata stores ID3/tag data for a single audio media item.
type AudioMetadata struct {
	ArtistID        *int64   `json:"artist_id,omitempty"`
	AlbumArtistID   *int64   `json:"album_artist_id,omitempty"`
	AlbumID         *int64   `json:"album_id,omitempty"`
	Title           *string  `json:"title,omitempty"`
	TrackNumber     *int16   `json:"track_number,omitempty"`
	DiscNumber      *int16   `json:"disc_number,omitempty"`
	DurationSeconds *float64 `json:"duration_seconds,omitempty"`
	Genre           *string  `json:"genre,omitempty"`
	Year            *int16   `json:"year,omitempty"`
	ReplayGainDB    *float64 `json:"replay_gain_db,omitempty"`
	MediaItemID     int64    `json:"-"`
	HasEmbeddedArt  bool     `json:"has_embedded_art"`
}

// AlbumSummary is the user-facing view of an album in an artist listing.
type AlbumSummary struct {
	Year          *int16  `json:"year,omitempty"`
	Name          string  `json:"name"`
	AlbumID       int64   `json:"album_id"`
	TrackCount    int     `json:"track_count"`
	TotalDuration float64 `json:"total_duration"`
}

// ShowSummary is the lightweight listing row returned when browsing an audio:show collection.
// ShowID is the artist ID — the artist represents the show, albums represent groupings.
// Field order must match ShowsInCollection SELECT for pgx.RowToStructByPos.
type ShowSummary struct {
	ShowID          int64  `json:"show_id"`
	Name            string `json:"name"`
	ManualThumbnail bool   `json:"manual_thumbnail"`
}

// EpisodeView is the user-facing projection of a show episode.
// It extends TrackView with the album (grouping) name so the client can render
// multi-level grouping (album → disc → track) without a separate request.
// Field order must match ShowEpisodesByArtist SELECT for pgx.RowToStructByPos.
type EpisodeView struct {
	TrackNumber     *int16   `json:"track_number,omitempty"`
	DiscNumber      *int16   `json:"disc_number,omitempty"`
	DurationSeconds *float64 `json:"duration_seconds,omitempty"`
	ArtistName      *string  `json:"artist_name,omitempty"`
	Title           string   `json:"title"`
	MimeType        string   `json:"mime_type"`
	ID              int64    `json:"id"`
	AlbumName       string   `json:"album_name"`
}

// ShowBookmark records a user's current position within an audio show.
type ShowBookmark struct {
	LastListenedAt  time.Time `json:"last_listened_at"`
	MediaItemID     int64     `json:"media_item_id"`
	PositionSeconds int       `json:"position_seconds"`
}

// TrackView is the user-facing projection of a track in an album listing.
type TrackView struct {
	TrackNumber     *int16   `json:"track_number,omitempty"`
	DiscNumber      *int16   `json:"disc_number,omitempty"`
	DurationSeconds *float64 `json:"duration_seconds,omitempty"`
	ArtistName      *string  `json:"artist_name,omitempty"`
	Title           string   `json:"title"`
	MimeType        string   `json:"mime_type"`
	ID              int64    `json:"id"`
}
