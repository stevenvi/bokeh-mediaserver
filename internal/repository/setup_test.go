package repository_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/models"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stevenvi/bokeh-mediaserver/internal/utils"
	"github.com/stretchr/testify/require"
)

var testPool *pgxpool.Pool

// Atomic counters ensure uniqueness across parallel subtests.
var (
	userCounter       int64
	itemCounter       int64
	deviceCounter     int64
	collectionCounter int64
	artistCounter     int64
	albumCounter      int64
)

func TestMain(m *testing.M) {
	var cleanup func()
	testPool, cleanup = testutil.Setup()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func bg() context.Context { return context.Background() }

// ─── User helpers ─────────────────────────────────────────────────────────────

func createUser(t *testing.T, db utils.DBTX) int64 {
	t.Helper()
	n := atomic.AddInt64(&userCounter, 1)
	authData, _ := json.Marshal(map[string]string{"password_hash": "testhash"})
	id, err := repository.UserCreate(bg(), db, fmt.Sprintf("testuser-%d", n), "local", authData)
	require.NoError(t, err)
	return id
}

// ─── Collection helpers ───────────────────────────────────────────────────────

func createCollection(t *testing.T, db utils.DBTX, colType constants.CollectionType) int64 {
	t.Helper()
	n := atomic.AddInt64(&collectionCounter, 1)
	return testutil.InsertCollection(t, db,
		fmt.Sprintf("Test Collection %d", n), colType,
		fmt.Sprintf("test/path-%d", n),
	)
}

func createSubCollection(t *testing.T, db utils.DBTX, parentID int64, colType constants.CollectionType) int64 {
	t.Helper()
	n := atomic.AddInt64(&collectionCounter, 1)
	id, err := repository.CollectionUpsertSubCollection(bg(), db, parentID, parentID,
		fmt.Sprintf("Sub Collection %d", n),
		fmt.Sprintf("test/path-%d/sub", n),
	)
	require.NoError(t, err)
	return id
}

func grantAccess(t *testing.T, db utils.DBTX, userID, collectionID int64) {
	t.Helper()
	err := repository.CollectionGrantAccessToUser(bg(), db, userID, []int64{collectionID})
	require.NoError(t, err)
}

// ─── Media item helpers ───────────────────────────────────────────────────────

func createMediaItem(t *testing.T, db utils.DBTX, collectionID int64) int64 {
	t.Helper()
	n := atomic.AddInt64(&itemCounter, 1)
	id, _, err := repository.MediaItemUpsert(bg(), db, collectionID,
		fmt.Sprintf("Test Item %d", n),
		fmt.Sprintf("test/item-%d.jpg", n),
		1024, fmt.Sprintf("hash-%d", n), "image/jpeg", time.Time{},
	)
	require.NoError(t, err)
	return id
}

func createVideoMediaItem(t *testing.T, db utils.DBTX, collectionID int64) int64 {
	t.Helper()
	n := atomic.AddInt64(&itemCounter, 1)
	id, _, err := repository.MediaItemUpsert(bg(), db, collectionID,
		fmt.Sprintf("Test Video %d", n),
		fmt.Sprintf("test/video-%d.mp4", n),
		1024*1024, fmt.Sprintf("vhash-%d", n), "video/mp4", time.Time{},
	)
	require.NoError(t, err)
	return id
}

func createAudioMediaItem(t *testing.T, db utils.DBTX, collectionID int64) int64 {
	t.Helper()
	n := atomic.AddInt64(&itemCounter, 1)
	id, _, err := repository.MediaItemUpsert(bg(), db, collectionID,
		fmt.Sprintf("Test Track %d", n),
		fmt.Sprintf("test/track-%d.mp3", n),
		5*1024*1024, fmt.Sprintf("ahash-%d", n), "audio/mpeg", time.Time{},
	)
	require.NoError(t, err)
	return id
}

// ─── Metadata helpers ─────────────────────────────────────────────────────────

func createPhotoMetadata(t *testing.T, db utils.DBTX, itemID int64) {
	t.Helper()
	w, h := 1920, 1080
	err := repository.PhotoUpsert(bg(), db, itemID,
		&w, &h, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
}

func createPhotoMetadataWithDate(t *testing.T, db utils.DBTX, itemID int64, createdAt time.Time) {
	t.Helper()
	w, h := 1920, 1080
	err := repository.PhotoUpsert(bg(), db, itemID,
		&w, &h, &createdAt,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
}

func createVideoMetadata(t *testing.T, db utils.DBTX, itemID int64) {
	t.Helper()
	dur, w, h, bitrate := 120, 1920, 1080, 4000
	vc, ac := "h264", "aac"
	err := repository.VideoUpsert(bg(), db, itemID, &dur, &w, &h, &bitrate, &vc, &ac, nil, nil)
	require.NoError(t, err)
}

// ─── Artist / Album helpers ───────────────────────────────────────────────────

func createArtist(t *testing.T, db utils.DBTX) int64 {
	t.Helper()
	n := atomic.AddInt64(&artistCounter, 1)
	id, err := repository.ArtistUpsert(bg(), db, fmt.Sprintf("Test Artist %d", n))
	require.NoError(t, err)
	return id
}

func createAlbum(t *testing.T, db utils.DBTX, artistID *int64, rootCollectionID int64) int64 {
	t.Helper()
	n := atomic.AddInt64(&albumCounter, 1)
	id, _, err := repository.AlbumUpsert(bg(), db,
		fmt.Sprintf("Test Album %d", n), artistID, nil, nil, rootCollectionID, false,
	)
	require.NoError(t, err)
	return id
}

// setupAudioData creates a full audio hierarchy for tests that need it.
// Returns (collectionID, artistID, albumID, itemID).
func setupAudioData(t *testing.T, db utils.DBTX) (int64, int64, int64, int64) {
	t.Helper()
	collID := createCollection(t, db, constants.CollectionTypeMusic)
	artistID := createArtist(t, db)
	albumID := createAlbum(t, db, &artistID, collID)
	itemID := createAudioMediaItem(t, db, collID)
	trackNum := int16(1)
	discNum := int16(1)
	duration := float64(213.5)
	genre := "Metal"
	year := int16(2005)
	replayGain := float64(-8.3)
	err := repository.AudioTrackUpsert(bg(), db, itemID,
		&artistID, nil, &albumID,
		&trackNum, &discNum, &duration, &genre, &year, &replayGain, true,
	)
	require.NoError(t, err)
	return collID, artistID, albumID, itemID
}

// ─── Device helpers ───────────────────────────────────────────────────────────

// createDevice inserts a device for userID and returns (id, uuid, tokenHash).
func createDevice(t *testing.T, db utils.DBTX, userID int64) (int64, string, string) {
	t.Helper()
	n := atomic.AddInt64(&deviceCounter, 1)
	uuid := fmt.Sprintf("test-uuid-%d", n)
	tokenHash := fmt.Sprintf("token-hash-%d", n)
	entry := models.AccessHistoryEntry{
		LastSeen: time.Now(),
		IP:       "127.0.0.1",
		Agent:    "test-agent",
	}
	id, err := repository.DeviceCreate(bg(), db, userID, uuid, "Test Device", tokenHash,
		time.Now().Add(90*24*time.Hour), entry,
	)
	require.NoError(t, err)
	return id, uuid, tokenHash
}

// ─── Pointer helpers ──────────────────────────────────────────────────────────

func int16Ptr(v int16) *int16 { return &v }
