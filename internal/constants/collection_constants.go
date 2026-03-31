package constants

var SupportedExtensions = map[string]string{
	// Common image formats
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".jpe":  "image/jpeg",
	".jfif": "image/jpeg",
	".png":  "image/png",
	".heic": "image/heic",
	".heif": "image/heif",
	".tiff": "image/tiff",
	".tif":  "image/tiff",
	".webp": "image/webp",
	".avif": "image/avif",
	".jxl":  "image/jxl",
	".gif":  "image/gif",
	".jp2":  "image/jp2",
	".j2k":  "image/jp2",

	// Camera RAW formats (vips reads these via libraw/rawloader)
	// A user _shouldn't_ be using these formats for long-term storage,
	// but ¯\_(ツ)_/¯
	".dng": "image/x-adobe-dng",
	".cr2": "image/x-canon-cr2",
	".cr3": "image/x-canon-cr3",
	".crw": "image/x-canon-crw",
	".nef": "image/x-nikon-nef",
	".nrw": "image/x-nikon-nrw",
	".arw": "image/x-sony-arw",
	".sr2": "image/x-sony-sr2",
	".srf": "image/x-sony-srf",
	".orf": "image/x-olympus-orf",
	".raf": "image/x-fuji-raf",
	".rw2": "image/x-panasonic-rw2",
	".rwl": "image/x-leica-rwl",
	".pef": "image/x-pentax-pef",
	".erf": "image/x-epson-erf",
	".mrw": "image/x-minolta-mrw",
	".3fr": "image/x-hasselblad-3fr",
	".x3f": "image/x-sigma-x3f",
	".kdc": "image/x-kodak-kdc",
	".k25": "image/x-kodak-k25",
	".dcr": "image/x-kodak-dcr",
	".iiq": "image/x-phaseone-iiq",
	".cap": "image/x-phaseone-cap",
	".srw": "image/x-samsung-srw",
	".fff": "image/x-imacon-fff",
	".mos": "image/x-leaf-mos",
	".mdc": "image/x-minolta-mdc",
	".pxn": "image/x-logitech-pxn",

	// Audio formats
	".mp3":  "audio/mpeg",
	".flac": "audio/flac",
	".m4a":  "audio/mp4",
	".aac":  "audio/aac",
	".ogg":  "audio/ogg",
	".oga":  "audio/ogg",
	".opus": "audio/opus",
	".wav":  "audio/wav",

	// Video formats
	".mp4": "video/mp4",
	".mov": "video/quicktime",
	".mkv": "video/x-matroska",
}

type CollectionType string

const (
	CollectionTypeMovie     CollectionType = "video:movie"
	CollectionTypeHomeMovie CollectionType = "video:home_movie"
	CollectionTypeMusic     CollectionType = "audio:music"
	CollectionTypeAudioShow CollectionType = "audio:show"
	CollectionTypePhoto     CollectionType = "image:photo"
)

func (s CollectionType) String() string {
	return string(s)
}
