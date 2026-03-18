package imaging

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/davidbyttow/govips/v2/vips"
)

// Variant names served by the image endpoint.
const (
	VariantThumb   = "thumb"   // 400px longest edge — AVIF + JPEG pre-generated
	VariantSmall   = "small"   // 1280px longest edge — AVIF only
	VariantPreview = "preview" // 1920px longest edge — AVIF only
)

type variantSpec struct {
	size    int
	quality int
}

// TODO: defer to DZI as much as possible. Maybe load 1080p preview then start up dzi?
// On clients lacking AVIF, only preview and full image are available maybe?
// (And always show the preview unless the full image is explicitly requested?)
var variants = map[string]variantSpec{
	VariantThumb:   {size: 400,  quality: 60},
	VariantSmall:   {size: 1280, quality: 70},
	VariantPreview: {size: 1920, quality: 75},
}

// Startup initialises the govips library. Must be called once at server start
// before any imaging functions are used. Shutdown should be deferred alongside it.
//
//	imaging.Startup()
//	defer imaging.Shutdown()
func Startup() {
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
	return filepath.Join(basePath, hash[0:2], hash[2:4], hash[4:])
}

// VariantPath returns the full path for a named variant file.
func VariantPath(basePath string, hash string, variant string, ext string) string {
	return filepath.Join(ItemDataPath(basePath, hash), variant+"."+ext)
}

// TilesPath returns the DZI directory for a media item.
func TilesPath(basePath string, hash string) string {
	return filepath.Join(ItemDataPath(basePath, hash), "tiles")
}

func GenerateVariant(srcPath string, dataPath string, hash string, variantName string, spec variantSpec, src *vips.ImageRef, srcLongestEdge int) error {
	// Skip variants at or above source resolution — no point upscaling.
	if spec.size >= srcLongestEdge {
		slog.Debug("skipping variant — source too small",
			"variant", variantName,
			"variant_size", spec.size,
			"src_longest_edge", srcLongestEdge,
		)
		return nil
	}

	// Copy source so we can resize independently for each variant.
	img, err := src.Copy()
	if err != nil {
		return fmt.Errorf("copy source for %s: %w", variantName, err)
	}
	defer img.Close()

	// Scale by longest edge, preserving aspect ratio.
	scale := float64(spec.size) / float64(srcLongestEdge)
	if err := img.Resize(scale, vips.KernelLanczos3); err != nil {
		return fmt.Errorf("resize %s: %w", variantName, err)
	}

	// Export AVIF
	avifParams := vips.AvifExportParams{
		Quality:       spec.quality,
		Lossless:      false,
		StripMetadata: true,
	}
	avifBytes, _, err := img.ExportAvif(&avifParams)
	if err != nil {
		return fmt.Errorf("export %s avif: %w", variantName, err)
	}
	avifPath := VariantPath(dataPath, hash, variantName, "avif")
	if err := os.WriteFile(avifPath, avifBytes, 0o644); err != nil {
		return fmt.Errorf("write %s avif: %w", variantName, err)
	}

	// Pre-generate JPEG thumb for Roku and legacy clients.
	// Re-uses the already-resized img — no second resize needed.
	if variantName == VariantThumb {
		jpegParams := vips.JpegExportParams{
			Quality:       spec.quality,
			StripMetadata: true,
		}
		jpegBytes, _, err := img.ExportJpeg(&jpegParams)
		if err != nil {
			return fmt.Errorf("export thumb jpeg: %w", err)
		}
		jpegPath := VariantPath(dataPath, hash, variantName, "jpg")
		if err := os.WriteFile(jpegPath, jpegBytes, 0o644); err != nil {
			return fmt.Errorf("write thumb jpeg: %w", err)
		}
	}
	
	return nil
}

// GenerateAllVariants creates all AVIF variants and the pre-generated JPEG thumb.
// Called by the indexer worker pool for each new or changed photo.
func GenerateAllVariants(srcPath string, dataPath string, hash string) error {
	outDir := ItemDataPath(dataPath, hash)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	// Load the source image
	src, err := vips.NewImageFromFile(srcPath)
	if err != nil {
		return fmt.Errorf("load source: %w", err)
	}
	defer src.Close()

	// Determine the longest edge for resizing decisions
	srcLongestEdge := max(src.Width(), src.Height())

	for variantName, spec := range variants {
		GenerateVariant(srcPath, dataPath, hash, variantName, spec, src, srcLongestEdge)
	}

	return nil
}

// GenerateDZI creates a Deep Zoom Image tile pyramid via the vips CLI.
// Tiles are encoded as AVIF at quality 85.
// Output: {tilesDir}/image.dzi + {tilesDir}/image_files/
// NOTE: govips does not support dzsave, so we shell out to the CLI. 
//       It's janky but is sufficient and is only called once per photo.
// TODO: Implement this into govips and remove the CLI dependency.
func GenerateDZI(srcPath string, dataPath string, hash string) error {
	tilesDir := TilesPath(dataPath, hash)
	if err := os.MkdirAll(tilesDir, 0o755); err != nil {
		return fmt.Errorf("mkdir tiles: %w", err)
	}

	// vips dzsave writes two outputs:
	//   {output}.dzi          — the manifest
	//   {output}_files/       — the tile pyramid
	output := filepath.Join(tilesDir, "image")

	cmd := exec.Command("vips", "dzsave",
		srcPath,
		output,
		"--tile-size", "252",
		"--overlap", "2",
		"--suffix", ".avif[Q=85]",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("vips dzsave: %w\n%s", err, out)
	}

	return nil
}

// GeneratePlaceholder creates a tiny 32x32 JPEG and returns it as a
// base64-encoded string for embedding directly in API responses.
// The client renders it as: <img src="data:image/jpeg;base64,{value}" />
// No extra HTTP request needed and no external dependency required.
func GeneratePlaceholder(srcPath string) (string, error) {
	img, err := vips.NewImageFromFile(srcPath)
	if err != nil {
		return "", fmt.Errorf("load source for placeholder: %w", err)
	}
	defer img.Close()

	// Scale to fit within 32x32, preserving aspect ratio.
	srcLongest := max(img.Height(), img.Width())
	scale := float64(32) / float64(srcLongest)
	if err := img.Resize(scale, vips.KernelNearest); err != nil {
		return "", fmt.Errorf("resize placeholder: %w", err)
	}

	jpegBytes, _, err := img.ExportJpeg(&vips.JpegExportParams{
		Quality:       10,
		StripMetadata: true,
	})
	if err != nil {
		return "", fmt.Errorf("export placeholder: %w", err)
	}

	return base64.StdEncoding.EncodeToString(jpegBytes), nil
}

// GenerateJPEGOnTheFly transcodes an AVIF variant to JPEG for legacy platforms.
// Not cached — called per-request. Fast enough for Roku slideshow use.
func GenerateJPEGOnTheFly(avifPath string) ([]byte, error) {
	img, err := vips.NewImageFromFile(avifPath)
	if err != nil {
		return nil, fmt.Errorf("load avif: %w", err)
	}
	defer img.Close()

	jpegBytes, _, err := img.ExportJpeg(&vips.JpegExportParams{
		Quality:       80,
		StripMetadata: true,
	})
	if err != nil {
		return nil, fmt.Errorf("transcode to jpeg: %w", err)
	}

	return jpegBytes, nil
}