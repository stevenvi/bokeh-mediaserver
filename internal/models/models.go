package models

import (
	"encoding/json"
	"time"
)

type Collection struct {
	ID                 int64      `json:"id"`
	ParentCollectionID *int64     `json:"parent_collection_id,omitempty"`
	Name               string     `json:"name"`
	Type               string     `json:"type"`
	RelativePath       *string    `json:"relative_path,omitempty"`
	IsEnabled          bool       `json:"is_enabled"`
	LastScannedAt      *time.Time `json:"last_scanned_at,omitempty"`
	MissingSince       *time.Time `json:"missing_since,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
}

type MediaItem struct {
	ID            int64      `json:"id"`
	CollectionID  int64      `json:"collection_id"`
	Title         string     `json:"title"`
	RelativePath  string     `json:"-"` // never expose filesystem paths to clients
	FileSizeBytes int64      `json:"file_size_bytes"`
	FileHash      string     `json:"-"`
	MimeType      string     `json:"mime_type"`
	Ordinal       *int       `json:"ordinal,omitempty"`
	MissingSince  *time.Time `json:"missing_since,omitempty"`
	IndexedAt     time.Time  `json:"indexed_at"`
	CreatedAt     time.Time  `json:"created_at"`

	// Populated when fetching item detail
	Photo *PhotoMetadata `json:"photo,omitempty"`
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
	Placeholder          *string    `json:"placeholder,omitempty"` // base64 32x32 AVIF
	VariantsGeneratedAt  *time.Time `json:"variants_generated_at,omitempty"`
}

type Job struct {
	ID           int64      `json:"id"`
	Type         string     `json:"type"`
	Status       string     `json:"status"`
	RelatedID    *int64     `json:"related_id,omitempty"`
	RelatedType  *string    `json:"related_type,omitempty"`
	Log          *string    `json:"log,omitempty"`
	ErrorMessage *string    `json:"error_message,omitempty"`
	QueuedAt     time.Time  `json:"queued_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
}

type User struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	IsAdmin   bool      `json:"is_admin"`
	CreatedAt time.Time `json:"created_at"`
}

// CollectionSummary is a lightweight view of a collection for user-facing lists.
type CollectionSummary struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// CollectionView is the user-facing detail view of a single collection.
type CollectionView struct {
	ID                 int64  `json:"id"`
	ParentCollectionID *int64 `json:"parent_collection_id,omitempty"`
	Name               string `json:"name"`
	Type               string `json:"type"`
}

// MediaItemView is the user-facing projection of a media item in a collection listing.
type MediaItemView struct {
	ID       int64  `json:"id"`
	Title    string `json:"title"`
	MimeType string `json:"mime_type"`
	Ordinal  *int   `json:"ordinal,omitempty"`

	// Populated when fetching item detail
	Photo *PhotoMetadata `json:"photo,omitempty"`
}

// SlideshowItem is a projection used by the slideshow endpoint.
type SlideshowItem struct {
	ID          int64      `json:"id"`
	Title       string     `json:"title"`
	MimeType    string     `json:"mime_type"`
	CreatedAt   *time.Time `json:"created_at,omitempty"`
	Placeholder *string    `json:"placeholder,omitempty"`
	WidthPx     *int       `json:"width_px,omitempty"`
	HeightPx    *int       `json:"height_px,omitempty"`
}

// Device is the full internal representation of a device row.
// device_uuid is intentionally excluded from all API responses.
type Device struct {
	ID                         int64
	DeviceUUID                 string
	UserID                     int64
	RefreshTokenHash           *string
	PreviousRefreshTokenHash   *string
	ExpiresAt                  *time.Time
	DeviceName                 string
	BannedAt                   *time.Time
	AccessHistory              json.RawMessage
	CreatedAt                  time.Time
	LastSeenAt                 time.Time
}

// DeviceView is the API-facing projection of a device (no device_uuid, no token fields).
// BannedAt non-nil means the device is banned.
type DeviceView struct {
	ID            int64           `json:"id"`
	DeviceName    string          `json:"device_name"`
	BannedAt      *time.Time      `json:"banned_at,omitempty"`
	LastSeenAt    time.Time       `json:"last_seen_at"`
	CreatedAt     time.Time       `json:"created_at"`
	AccessHistory json.RawMessage `json:"access_history"`
}

// AccessHistoryEntry is one entry in a device's access_history JSONB array.
type AccessHistoryEntry struct {
	IP       string    `json:"ip"`
	Agent    string    `json:"agent"`
	LastSeen time.Time `json:"last_seen"`
}
