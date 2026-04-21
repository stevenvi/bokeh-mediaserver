package imaging

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/cshum/vipsgen/vips"
)

// Variant names served by the image endpoint.
const (
	VariantThumb   = "thumb"   // 400px longest edge — WebP
	VariantSmall   = "small"   // 1280px longest edge — WebP
	VariantPreview = "preview" // 1920px longest edge — WebP
)

type variantSpec struct {
	name    string
	size    int
	quality int
}

// variants defines the image variants to generate, in order.
// Thumb is last because it is the final file written and serves as the sentinel
// checked by VariantsExist — if it exists, all variants completed.
var variants = []variantSpec{
	{name: VariantPreview, size: 1920, quality: 75},
	{name: VariantSmall, size: 1280, quality: 70},
	{name: VariantThumb, size: 400, quality: 60},
}

// Startup initialises the vips library. Must be called once at server start
// before any imaging functions are used. Shutdown should be deferred alongside it.
//
//	imaging.Startup()
//	defer imaging.Shutdown()
func Startup() {
	// Suppress operational noise (resize decisions, mask sizes, vector paths, etc.).
	// Only forward warnings and above to slog so real problems surface.
	vips.SetLogging(func(domain string, level vips.LogLevel, msg string) {
		slog.Warn("vips", "domain", domain, "msg", msg)
	}, vips.LogLevelWarning)

	vips.Startup(nil)
}

func Shutdown() {
	vips.Shutdown()
}

// ItemDataPath returns the base directory for a media item's derived data,
// addressed by the item's BLAKE2b-256 content hash.
// Three-level hex sharding: {base}/{hash[0:2]}/{hash[2:4]}/{hash[4:]}/
//
// Using the hash as the key means renamed or moved files reuse existing
// derived data without re-processing.
func ItemDataPath(basePath string, hash string) string {
	return filepath.Join(basePath, "derived_media", hash[0:2], hash[2:4], hash[4:])
}

// VariantPath returns the full path for a named variant file.
func VariantPath(basePath string, hash string, variant string, ext string) string {
	return filepath.Join(ItemDataPath(basePath, hash), variant+"."+ext)
}

// TilesPath returns the DZI directory for a media item.
func TilesPath(basePath string, hash string) string {
	return filepath.Join(ItemDataPath(basePath, hash), "tiles")
}

// CollectionThumbnailDir returns the directory for a collection's thumbnail images.
func CollectionThumbnailDir(dataPath string, collectionID int64) string {
	return filepath.Join(dataPath, "collection_images", fmt.Sprintf("%d", collectionID))
}

// CollectionThumbnailPath returns the path for a collection's thumbnail image.
func CollectionThumbnailPath(dataPath string, collectionID int64, ext string) string {
	return filepath.Join(CollectionThumbnailDir(dataPath, collectionID), "cover."+ext)
}

// CollectionThumbnailExists returns true if a collection thumbnail image exists on disk.
func CollectionThumbnailExists(dataPath string, collectionID int64) bool {
	_, err := os.Stat(CollectionThumbnailPath(dataPath, collectionID, "webp"))
	return err == nil
}

// atomicWrite writes data to a temp file in the same directory as finalPath,
// sets permissions to 0644, then renames it into place. This prevents partial
// file writes — the final path either has the complete content or doesn't exist.
func atomicWrite(finalPath string, data []byte) error {
	dir := filepath.Dir(finalPath)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	_, werr := tmp.Write(data)
	cerr := tmp.Close()
	if werr != nil || cerr != nil {
		os.Remove(tmpPath)
		if werr != nil {
			return werr
		}
		return cerr
	}
	if err := os.Chmod(tmpPath, 0o644); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, finalPath)
}

// coverFromFile loads srcPath, auto-rotates, center-crops to widthRatio:heightRatio,
// resizes to 400px wide, and writes WebP to the given path.
// Use widthRatio=1, heightRatio=1 for a square crop.
func coverFromFile(srcPath, webpPath string, widthRatio, heightRatio int) error {
	if err := os.MkdirAll(filepath.Dir(webpPath), 0o755); err != nil {
		return fmt.Errorf("mkdir cover dir: %w", err)
	}

	img, err := vips.NewImageFromFile(srcPath, nil)
	if err != nil {
		return fmt.Errorf("load image: %w", err)
	}
	defer img.Close()

	if err := img.Autorot(nil); err != nil {
		return fmt.Errorf("auto-rotate: %w", err)
	}

	// Center-crop to widthRatio:heightRatio
	w, h := img.Width(), img.Height()
	targetH := w * heightRatio / widthRatio
	if targetH > h {
		// Image is more portrait than the target ratio — crop width instead
		targetW := h * widthRatio / heightRatio
		left := (w - targetW) / 2
		if err := img.ExtractArea(left, 0, targetW, h); err != nil {
			return fmt.Errorf("crop cover: %w", err)
		}
	} else if targetH < h {
		top := (h - targetH) / 2
		if err := img.ExtractArea(0, top, w, targetH); err != nil {
			return fmt.Errorf("crop cover: %w", err)
		}
	}

	// Resize to 400px wide
	if img.Width() != 400 {
		scale := 400.0 / float64(img.Width())
		if err := img.Resize(scale, &vips.ResizeOptions{Kernel: vips.KernelLanczos3}); err != nil {
			return fmt.Errorf("resize cover: %w", err)
		}
	}

	webpBytes, err := img.WebpsaveBuffer(&vips.WebpsaveBufferOptions{Q: 60, Keep: vips.KeepNone})
	if err != nil {
		return fmt.Errorf("export webp: %w", err)
	}
	if err := atomicWrite(webpPath, webpBytes); err != nil {
		return fmt.Errorf("write webp: %w", err)
	}

	return nil
}

// squareCoverFromFile loads an image file, auto-rotates it,
// center-crops to square, resizes to 400px, and writes WebP to the given path.
func squareCoverFromFile(srcPath, webpPath string) error {
	return coverFromFile(srcPath, webpPath, 1, 1)
}

// largeCoverFromFile loads srcPath, auto-rotates, center-crops to square,
// resizes to at most 1280px wide (no upscale), and writes WebP.
func largeCoverFromFile(srcPath, webpPath string) error {
	if err := os.MkdirAll(filepath.Dir(webpPath), 0o755); err != nil {
		return fmt.Errorf("mkdir cover dir: %w", err)
	}

	img, err := vips.NewImageFromFile(srcPath, nil)
	if err != nil {
		return fmt.Errorf("load image: %w", err)
	}
	defer img.Close()

	if err := img.Autorot(nil); err != nil {
		return fmt.Errorf("auto-rotate: %w", err)
	}

	// Center-crop to square
	w, h := img.Width(), img.Height()
	if w > h {
		left := (w - h) / 2
		if err := img.ExtractArea(left, 0, h, h); err != nil {
			return fmt.Errorf("crop to square: %w", err)
		}
	} else if h > w {
		top := (h - w) / 2
		if err := img.ExtractArea(0, top, w, w); err != nil {
			return fmt.Errorf("crop to square: %w", err)
		}
	}

	// Resize to at most 1280px (no upscale)
	const maxSize = 1280
	if img.Width() > maxSize {
		scale := float64(maxSize) / float64(img.Width())
		if err := img.Resize(scale, &vips.ResizeOptions{Kernel: vips.KernelLanczos3}); err != nil {
			return fmt.Errorf("resize cover: %w", err)
		}
	}

	webpBytes, err := img.WebpsaveBuffer(&vips.WebpsaveBufferOptions{Q: 70, Keep: vips.KeepNone})
	if err != nil {
		return fmt.Errorf("export webp: %w", err)
	}
	if err := atomicWrite(webpPath, webpBytes); err != nil {
		return fmt.Errorf("write webp: %w", err)
	}

	return nil
}

// largeCoverFromBytes writes raw image bytes to a temp file and calls largeCoverFromFile.
func largeCoverFromBytes(imageBytes []byte, webpPath string) error {
	tmp, err := os.CreateTemp("", "bokeh-large-cover-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	if _, err := tmp.Write(imageBytes); err != nil {
		return fmt.Errorf("write to temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}

	return largeCoverFromFile(tmp.Name(), webpPath)
}

// squareCoverFromBytes writes raw image bytes to a temp file and calls squareCoverFromFile.
func squareCoverFromBytes(imageBytes []byte, webpPath string) error {
	tmp, err := os.CreateTemp("", "bokeh-cover-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	if _, err := tmp.Write(imageBytes); err != nil {
		return fmt.Errorf("write to temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}

	return squareCoverFromFile(tmp.Name(), webpPath)
}

// GenerateCollectionThumbnail loads a thumb variant, center-crops it to a square,
// and writes a WebP version to the collection_images directory.
func GenerateCollectionThumbnail(srcThumbPath string, dataPath string, collectionID int64) error {
	return squareCoverFromFile(srcThumbPath,
		CollectionThumbnailPath(dataPath, collectionID, "webp"),
	)
}

// GenerateCollectionThumbnailFromUpload loads an arbitrary image file, auto-rotates,
// center-crops to square, resizes to 400x400, and writes a WebP cover.
func GenerateCollectionThumbnailFromUpload(srcPath string, dataPath string, collectionID int64) error {
	return squareCoverFromFile(srcPath,
		CollectionThumbnailPath(dataPath, collectionID, "webp"),
	)
}

// ArtistThumbnailDir returns the directory for an artist's thumbnail images.
func ArtistThumbnailDir(dataPath string, artistID int64) string {
	return filepath.Join(dataPath, "artist_images", fmt.Sprintf("%d", artistID))
}

// ArtistThumbnailPath returns the path for an artist's thumbnail image.
func ArtistThumbnailPath(dataPath string, artistID int64, ext string) string {
	return filepath.Join(ArtistThumbnailDir(dataPath, artistID), "cover."+ext)
}

// ArtistThumbnailExists returns true if an artist thumbnail image exists on disk.
func ArtistThumbnailExists(dataPath string, artistID int64) bool {
	_, err := os.Stat(ArtistThumbnailPath(dataPath, artistID, "webp"))
	return err == nil
}

// GenerateArtistThumbnailFromUpload loads an arbitrary image file, auto-rotates,
// center-crops to square, resizes to 400x400, and writes a WebP cover.
func GenerateArtistThumbnailFromUpload(srcPath string, dataPath string, artistID int64) error {
	return squareCoverFromFile(srcPath,
		ArtistThumbnailPath(dataPath, artistID, "webp"),
	)
}

// GenerateArtistThumbnail loads an album thumbnail and writes it as the artist thumbnail.
func GenerateArtistThumbnail(srcPath string, dataPath string, artistID int64) error {
	return squareCoverFromFile(srcPath,
		ArtistThumbnailPath(dataPath, artistID, "webp"),
	)
}

// AlbumThumbnailDir returns the directory for an album's thumbnail images (400px).
func AlbumThumbnailDir(dataPath string, albumID int64) string {
	return filepath.Join(dataPath, "album_images", fmt.Sprintf("%d", albumID))
}

// AlbumThumbnailPath returns the path for an album thumbnail image with the given extension (400px).
func AlbumThumbnailPath(dataPath string, albumID int64, ext string) string {
	return filepath.Join(AlbumThumbnailDir(dataPath, albumID), "thumb."+ext)
}

// AlbumThumbnailExists returns true if an album thumbnail exists on disk (400px).
func AlbumThumbnailExists(dataPath string, albumID int64) bool {
	_, err := os.Stat(AlbumThumbnailPath(dataPath, albumID, "webp"))
	return err == nil
}

// GenerateAlbumThumbnailFromUpload loads an arbitrary image file, auto-rotates,
// center-crops to square, resizes to 400x400, and writes a WebP thumbnail.
func GenerateAlbumThumbnailFromUpload(srcPath string, dataPath string, albumID int64) error {
	return squareCoverFromFile(srcPath,
		AlbumThumbnailPath(dataPath, albumID, "webp"),
	)
}

// GenerateAlbumThumbnailFromBytes takes raw image bytes (e.g. extracted from embedded
// audio art) and generates an album thumbnail (400px) from them.
func GenerateAlbumThumbnailFromBytes(imageBytes []byte, dataPath string, albumID int64) error {
	return squareCoverFromBytes(imageBytes,
		AlbumThumbnailPath(dataPath, albumID, "webp"),
	)
}

// AlbumCoverDir returns the directory for an album's cover images (1280px).
func AlbumCoverDir(dataPath string, albumID int64) string {
	return filepath.Join(dataPath, "album_images", fmt.Sprintf("%d", albumID))
}

// AlbumCoverPath returns the path for an album cover image with the given extension (1280px).
func AlbumCoverPath(dataPath string, albumID int64, ext string) string {
	return filepath.Join(AlbumCoverDir(dataPath, albumID), "cover."+ext)
}

// AlbumCoverExists returns true if an album cover exists on disk (1280px).
func AlbumCoverExists(dataPath string, albumID int64) bool {
	_, err := os.Stat(AlbumCoverPath(dataPath, albumID, "webp"))
	return err == nil
}

// GenerateAlbumCoverFromUpload loads an arbitrary image file, auto-rotates,
// center-crops to square, resizes up to 1280px, and writes a WebP cover.
func GenerateAlbumCoverFromUpload(srcPath string, dataPath string, albumID int64) error {
	return largeCoverFromFile(srcPath,
		AlbumCoverPath(dataPath, albumID, "webp"),
	)
}

// GenerateAlbumCoverFromBytes takes raw image bytes (e.g. extracted from embedded
// audio art) and generates an album cover (1280px, WebP) from them.
func GenerateAlbumCoverFromBytes(imageBytes []byte, dataPath string, albumID int64) error {
	return largeCoverFromBytes(imageBytes,
		AlbumCoverPath(dataPath, albumID, "webp"),
	)
}

// GenerateThumbnailFromBytes takes raw image bytes (e.g. extracted album art)
// and generates a collection thumbnail from them.
func GenerateThumbnailFromBytes(imageBytes []byte, dataPath string, collectionID int64) error {
	return squareCoverFromBytes(imageBytes,
		CollectionThumbnailPath(dataPath, collectionID, "webp"),
	)
}

// VariantsExist returns true if the thumb WebP file exists on disk.
// Thumb is the very last variant written during generation,
// so its presence implies all variants completed successfully.
func VariantsExist(dataPath string, hash string) bool {
	_, err := os.Stat(VariantPath(dataPath, hash, VariantThumb, "webp"))
	return err == nil
}

// generateVariant writes one WebP image variant into outDir.
// Thumb is always last in the variants slice, making thumb.webp the final
// file written and therefore the completion sentinel for the entire variant set.
func generateVariant(outDir string, spec variantSpec, src *vips.Image, srcLongestEdge int) error {
	// Skip variants at or above source resolution — no point upscaling.
	if spec.size >= srcLongestEdge {
		slog.Debug("skipping variant — source too small",
			"variant", spec.name,
			"variant_size", spec.size,
			"src_longest_edge", srcLongestEdge,
		)
		return nil
	}

	// Copy source so we can resize independently for each variant.
	img, err := src.Copy(nil)
	if err != nil {
		return fmt.Errorf("copy source for %s: %w", spec.name, err)
	}
	defer img.Close()

	// Scale by longest edge, preserving aspect ratio.
	scale := float64(spec.size) / float64(srcLongestEdge)
	if err := img.Resize(scale, &vips.ResizeOptions{Kernel: vips.KernelLanczos3}); err != nil {
		return fmt.Errorf("resize %s: %w", spec.name, err)
	}

	webpBytes, err := img.WebpsaveBuffer(&vips.WebpsaveBufferOptions{
		Q:    spec.quality,
		Keep: vips.KeepNone,
	})
	if err != nil {
		return fmt.Errorf("export %s webp: %w", spec.name, err)
	}
	if err := atomicWrite(filepath.Join(outDir, spec.name+".webp"), webpBytes); err != nil {
		return fmt.Errorf("write %s webp: %w", spec.name, err)
	}

	return nil
}

// generateAllVariants writes all WebP variants into outDir.
// outDir must already exist.
func generateAllVariants(srcPath string, outDir string) error {
	src, err := vips.NewImageFromFile(srcPath, nil)
	if err != nil {
		return fmt.Errorf("load source: %w", err)
	}
	defer src.Close()

	// Apply EXIF orientation before any resizing so all variants are correctly rotated.
	if err := src.Autorot(nil); err != nil {
		return fmt.Errorf("auto-rotate: %w", err)
	}

	srcLongestEdge := max(src.Width(), src.Height())
	for _, v := range variants {
		if err := generateVariant(outDir, v, src, srcLongestEdge); err != nil {
			return err
		}
	}
	return nil
}

// generateDZI creates a Deep Zoom Image tile pyramid using vipsgen's native Dzsave.
// Tiles are encoded as WebP at quality 85.
// Output: {tilesDir}/image.dzi + {tilesDir}/image_files/
// tilesDir must already exist.
func generateDZI(srcPath string, tilesDir string) error {
	img, err := vips.NewImageFromFile(srcPath, nil)
	if err != nil {
		return fmt.Errorf("load source for dzi: %w", err)
	}
	defer img.Close()

	// Apply EXIF orientation before tiling so tiles are correctly rotated.
	if err := img.Autorot(nil); err != nil {
		return fmt.Errorf("auto-rotate for dzi: %w", err)
	}

	// Dzsave writes two outputs relative to the base name:
	//   {output}.dzi          — the manifest
	//   {output}_files/       — the tile pyramid
	output := filepath.Join(tilesDir, "image")
	if err := img.Dzsave(output, &vips.DzsaveOptions{
		TileSize: 252,
		Overlap:  2,
		Suffix:   ".webp",
		Q:        98,
		Keep:     vips.KeepNone,
	}); err != nil {
		return fmt.Errorf("dzsave: %w", err)
	}
	return nil
}

// GenerateItemVariants generates WebP image variants for a photo item atomically.
//
// All output is written to a temporary directory adjacent to the final
// ItemDataPath. On success the temp directory is renamed to the final
// path in a single syscall, so the derived-data directory either contains
// the complete set of files or does not exist at all. Any partial output
// from an interrupted prior run is discarded before the rename.
func GenerateItemVariants(srcPath, dataPath, hash string) error {
	finalDir := ItemDataPath(dataPath, hash)
	parentDir := filepath.Dir(finalDir)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return fmt.Errorf("mkdir parent: %w", err)
	}

	// Temp dir is in the same parent so os.Rename is always within one filesystem.
	tmpDir, err := os.MkdirTemp(parentDir, ".bokeh-item-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	// Clean up on failure; no-op after a successful rename.
	success := false
	defer func() {
		if !success {
			os.RemoveAll(tmpDir)
		}
	}()

	if err := generateAllVariants(srcPath, tmpDir); err != nil {
		return err
	}

	// Discard any stale partial directory from a previous failed run, then
	// atomically replace it with the newly generated content.
	if err := os.RemoveAll(finalDir); err != nil {
		return fmt.Errorf("remove stale dir: %w", err)
	}
	if err := os.Rename(tmpDir, finalDir); err != nil {
		return fmt.Errorf("rename to final: %w", err)
	}
	success = true
	return nil
}

// GenerateDZIIfNotPresent generates DZI tiles for a photo item into its existing
// ItemDataPath directory. The DZI manifest (image.dzi) serves as the
// completion sentinel — if it exists, generation is skipped.
// Returns true if tiles were generated, false if they already existed.
func GenerateDZIIfNotPresent(srcPath, dataPath, hash string) (bool, error) {
	tilesDir := TilesPath(dataPath, hash)
	manifestPath := filepath.Join(tilesDir, "image.dzi")

	// Already generated — nothing to do.
	if _, err := os.Stat(manifestPath); err == nil {
		return false, nil
	}

	if err := os.MkdirAll(tilesDir, 0o755); err != nil {
		return false, fmt.Errorf("mkdir tiles: %w", err)
	}
	if err := generateDZI(srcPath, tilesDir); err != nil {
		// Clean up partial output on failure.
		os.RemoveAll(tilesDir)
		return false, err
	}
	return true, nil
}

// VideoHLSDir returns the directory for a video item's stored HLS transcode.
func VideoHLSDir(dataPath, hash string) string {
	return filepath.Join(ItemDataPath(dataPath, hash), "hls")
}

// VideoHLSManifest returns the path to the HLS manifest for a video item.
func VideoHLSManifest(dataPath, hash string) string {
	return filepath.Join(VideoHLSDir(dataPath, hash), "manifest.m3u8")
}

// GenerateVideoCoverFromBytes takes raw image bytes, center-crops to the given
// aspect ratio (widthRatio:heightRatio), resizes to 400px wide, and writes
// WebP to VariantPath(dataPath, hash, "cover", "webp").
//
// Standard ratios:
//   - Movie posters: widthRatio=2, heightRatio=3  (portrait, 400×600)
//   - Home movies:   widthRatio=3, heightRatio=4  (near-square, 400×533)
func GenerateVideoCoverFromBytes(imageBytes []byte, dataPath, hash string, widthRatio, heightRatio int) error {
	tmp, err := os.CreateTemp("", "bokeh-video-cover-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	if _, err := tmp.Write(imageBytes); err != nil {
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}

	return coverFromFile(tmp.Name(),
		VariantPath(dataPath, hash, "cover", "webp"),
		widthRatio, heightRatio,
	)
}

// GenerateVideoCoverFromFrame uses ffmpeg to extract a single frame at a deterministic
// random offset (between 5% and 95% of duration) and generates a cover image from it,
// applying the same widthRatio:heightRatio center-crop as GenerateVideoCoverFromBytes.
// Output: cover.webp at 400px wide.
func GenerateVideoCoverFromFrame(srcPath, dataPath, hash string, durationSecs, widthRatio, heightRatio int) error {
	// Pick a random offset in [5%, 95%] of duration using a deterministic
	// seed derived from the hash to avoid relying on global random state.
	var seed uint64
	for i, c := range []byte(hash) {
		if i >= 8 {
			break
		}
		seed = seed*256 + uint64(c)
	}
	frac := 0.05 + (float64(seed%1000)/1000.0)*0.90
	offsetSecs := frac * float64(durationSecs)

	cmd := exec.Command("ffmpeg",
		"-ss", fmt.Sprintf("%.3f", offsetSecs),
		"-i", srcPath,
		"-vframes", "1",
		"-f", "image2pipe",
		"-vcodec", "mjpeg",
		"-q:v", "2",
		"pipe:1",
	)
	jpegBytes, err := cmd.Output()
	if err != nil || len(jpegBytes) == 0 {
		return fmt.Errorf("ffmpeg frame extract: %w", err)
	}

	return GenerateVideoCoverFromBytes(jpegBytes, dataPath, hash, widthRatio, heightRatio)
}

// GenerateJPEG transcodes a WebP file to JPEG on the fly.
// Not cached — called per-request for legacy clients that don't support WebP.
func GenerateJPEG(webpPath string) ([]byte, error) {
	img, err := vips.NewImageFromFile(webpPath, nil)
	if err != nil {
		return nil, fmt.Errorf("load webp: %w", err)
	}
	defer img.Close()

	jpegBytes, err := img.JpegsaveBuffer(&vips.JpegsaveBufferOptions{
		Q:    80,
		Keep: vips.KeepNone,
	})
	if err != nil {
		return nil, fmt.Errorf("transcode to jpeg: %w", err)
	}

	return jpegBytes, nil
}
