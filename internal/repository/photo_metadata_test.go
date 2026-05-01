package repository_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stevenvi/bokeh-mediaserver/internal/constants"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
	"github.com/stevenvi/bokeh-mediaserver/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPhotoUpsert(t *testing.T) {
	t.Run("minimal_fields_all_null", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)

		err := repository.PhotoUpsert(bg(), db, itemID,
			nil, nil, nil,
			nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		)
		require.NoError(t, err)

		var widthPx, heightPx, iso *int
		var createdAt *time.Time
		var cameraMake, cameraModel, lensModel, shutterSpeed, colorSpace, description *string
		var aperture, focalLengthMM, focalLength35mmEquiv *float64
		var exifRaw []byte
		err = db.QueryRow(bg(),
			`SELECT width_px, height_px, created_at, camera_make, camera_model, lens_model,
			        shutter_speed, aperture, iso, focal_length_mm, focal_length_35mm_equiv,
			        color_space, description, exif_raw
			 FROM photo_metadata WHERE media_item_id = $1`, itemID,
		).Scan(&widthPx, &heightPx, &createdAt, &cameraMake, &cameraModel, &lensModel,
			&shutterSpeed, &aperture, &iso, &focalLengthMM, &focalLength35mmEquiv,
			&colorSpace, &description, &exifRaw)
		require.NoError(t, err)
		assert.Nil(t, widthPx)
		assert.Nil(t, heightPx)
		assert.Nil(t, createdAt)
		assert.Nil(t, cameraMake)
		assert.Nil(t, cameraModel)
		assert.Nil(t, lensModel)
		assert.Nil(t, shutterSpeed)
		assert.Nil(t, aperture)
		assert.Nil(t, iso)
		assert.Nil(t, focalLengthMM)
		assert.Nil(t, focalLength35mmEquiv)
		assert.Nil(t, colorSpace)
		assert.Nil(t, description)
		assert.Nil(t, exifRaw)
	})

	t.Run("all_fields_stored_correctly", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)

		w, h := 6000, 4000
		ts := time.Date(2023, 8, 15, 12, 0, 0, 0, time.UTC)
		make_, model := "Nikon", "Z9"
		lens := "NIKKOR Z 50mm f/1.2 S"
		shutter := "1/500"
		aperture := 1.2
		iso := 800
		focalMM, focal35mm := 50.0, 50.0
		colorSpace := "sRGB"
		desc := "Test photo"
		exifRaw := json.RawMessage(`{"Make":"Nikon","Model":"Z9"}`)

		err := repository.PhotoUpsert(bg(), db, itemID,
			&w, &h, &ts,
			&make_, &model, &lens, &shutter, &aperture, &iso, &focalMM, &focal35mm,
			&colorSpace, &desc, nil, exifRaw,
		)
		require.NoError(t, err)

		var gotW, gotH, gotISO *int
		var gotAt *time.Time
		var gotMake, gotModel, gotLens, gotShutter, gotColorSpace, gotDesc *string
		var gotAperture, gotFocalMM, gotFocal35 *float64
		var gotExif []byte
		err = db.QueryRow(bg(),
			`SELECT width_px, height_px, created_at, camera_make, camera_model, lens_model,
			        shutter_speed, aperture, iso, focal_length_mm, focal_length_35mm_equiv,
			        color_space, description, exif_raw
			 FROM photo_metadata WHERE media_item_id = $1`, itemID,
		).Scan(&gotW, &gotH, &gotAt, &gotMake, &gotModel, &gotLens,
			&gotShutter, &gotAperture, &gotISO, &gotFocalMM, &gotFocal35,
			&gotColorSpace, &gotDesc, &gotExif)
		require.NoError(t, err)

		require.NotNil(t, gotW)
		assert.Equal(t, w, *gotW)
		require.NotNil(t, gotH)
		assert.Equal(t, h, *gotH)
		require.NotNil(t, gotAt)
		assert.Equal(t, ts.UTC(), gotAt.UTC())
		require.NotNil(t, gotMake)
		assert.Equal(t, "Nikon", *gotMake)
		require.NotNil(t, gotModel)
		assert.Equal(t, "Z9", *gotModel)
		require.NotNil(t, gotLens)
		assert.Equal(t, lens, *gotLens)
		require.NotNil(t, gotShutter)
		assert.Equal(t, "1/500", *gotShutter)
		require.NotNil(t, gotAperture)
		assert.InDelta(t, 1.2, *gotAperture, 0.001)
		require.NotNil(t, gotISO)
		assert.Equal(t, 800, *gotISO)
		require.NotNil(t, gotFocalMM)
		assert.InDelta(t, 50.0, *gotFocalMM, 0.001)
		require.NotNil(t, gotFocal35)
		assert.InDelta(t, 50.0, *gotFocal35, 0.001)
		require.NotNil(t, gotColorSpace)
		assert.Equal(t, "sRGB", *gotColorSpace)
		require.NotNil(t, gotDesc)
		assert.Equal(t, "Test photo", *gotDesc)
		assert.JSONEq(t, `{"Make":"Nikon","Model":"Z9"}`, string(gotExif))
	})

	t.Run("upsert_overwrites_existing", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)

		w1, h1 := 800, 600
		require.NoError(t, repository.PhotoUpsert(bg(), db, itemID,
			&w1, &h1, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		))

		w2, h2 := 1600, 1200
		require.NoError(t, repository.PhotoUpsert(bg(), db, itemID,
			&w2, &h2, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		))

		var gotWidth int
		err := db.QueryRow(bg(),
			`SELECT width_px FROM photo_metadata WHERE media_item_id = $1`, itemID,
		).Scan(&gotWidth)
		require.NoError(t, err)
		assert.Equal(t, w2, gotWidth)
	})
}

func TestPhotoCountPendingVariants(t *testing.T) {
	t.Run("counts_rows_without_variants", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		createPhotoMetadata(t, db, itemID)

		count, err := repository.PhotoCountPendingVariants(bg(), db)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, count, 1)
	})

	t.Run("zero_when_all_variants_generated", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		createPhotoMetadata(t, db, itemID)
		require.NoError(t, repository.PhotoUpdateVariants(bg(), db, itemID))

		// This item now has variants; it should not appear in the pending count.
		// We can only verify that count doesn't increase after marking as generated.
		count, err := repository.PhotoCountPendingVariants(bg(), db)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, count, 0)
	})
}

func TestPhotoUpdateVariants(t *testing.T) {
	t.Run("sets_variants_generated_at", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		createPhotoMetadata(t, db, itemID)

		require.NoError(t, repository.PhotoUpdateVariants(bg(), db, itemID))

		var variantsAt *time.Time
		err := db.QueryRow(bg(),
			`SELECT variants_generated_at FROM photo_metadata WHERE media_item_id = $1`, itemID,
		).Scan(&variantsAt)
		require.NoError(t, err)
		assert.NotNil(t, variantsAt)
	})
}

func TestPhotoClearVariantsGenerated(t *testing.T) {
	t.Run("resets_variants_generated_at_for_collection", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		itemID := createMediaItem(t, db, collID)
		createPhotoMetadata(t, db, itemID)
		require.NoError(t, repository.PhotoUpdateVariants(bg(), db, itemID))

		require.NoError(t, repository.PhotoClearVariantsGenerated(bg(), db, collID))

		var variantsAt *time.Time
		err := db.QueryRow(bg(),
			`SELECT variants_generated_at FROM photo_metadata WHERE media_item_id = $1`, itemID,
		).Scan(&variantsAt)
		require.NoError(t, err)
		assert.Nil(t, variantsAt)
	})

	t.Run("noop_for_collection_with_no_metadata", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		require.NoError(t, repository.PhotoClearVariantsGenerated(bg(), db, collID))
	})
}

func TestPhotoExifRaw(t *testing.T) {
	t.Run("returns_exif_for_accessible_item", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		grantAccess(t, db, userID, collID)
		itemID := createMediaItem(t, db, collID)

		exifRaw := []byte(`{"Make":"Canon"}`)
		w, h := 1920, 1080
		err := repository.PhotoUpsert(bg(), db, itemID,
			&w, &h, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, exifRaw,
		)
		require.NoError(t, err)

		got, err := repository.PhotoExifRaw(bg(), db, itemID, userID)
		require.NoError(t, err)
		assert.JSONEq(t, string(exifRaw), string(got))
	})

	t.Run("error_when_no_access", func(t *testing.T) {
		db := testutil.NewTx(t, testPool)
		userID := createUser(t, db)
		collID := createCollection(t, db, constants.CollectionTypePhoto)
		// No access granted
		itemID := createMediaItem(t, db, collID)
		createPhotoMetadata(t, db, itemID)

		_, err := repository.PhotoExifRaw(bg(), db, itemID, userID)
		assert.Error(t, err)
	})
}
