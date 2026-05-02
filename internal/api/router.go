package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/cors"
	authpkg "github.com/stevenvi/bokeh-mediaserver/internal/auth"
	"github.com/stevenvi/bokeh-mediaserver/internal/config"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
)

// NewRouter builds and returns the fully configured Chi router.
func NewRouter(db *pgxpool.Pool, guard *DeviceGuard, dispatcher *jobs.Dispatcher, scheduler *jobs.Scheduler, cfg *config.Config, jwtSecret, mediaPath, dataPath, clientOrigin string, production bool) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(requestLogger)

	// CORS — only needed when the client is served from a different origin (dev).
	// In production the client and API share an origin via Caddy, so this is a no-op.
	if clientOrigin != "" {
		c := cors.New(cors.Options{
			AllowedOrigins:   []string{clientOrigin},
			AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
			AllowedHeaders:   []string{"Content-Type", "Authorization"},
			AllowCredentials: true,
			MaxAge:           300,
		})
		r.Use(c.Handler)
	}

	rateLimiter := newLoginRateLimiter()

	authPlugins := authpkg.DefaultPlugins()

	authHandler := newAuthHandler(db, guard, rateLimiter, jwtSecret, authPlugins, production)
	collections := &collectionsHandler{db: db}
	music := &musicHandler{db: db, dataPath: dataPath, mediaPath: mediaPath}
	radio := &radioHandler{db: db, dataPath: dataPath, mediaPath: mediaPath}
	photos := &photosHandler{db: db, dataPath: dataPath, mediaPath: mediaPath}
	video := &videoHandler{db: db, dataPath: dataPath, mediaPath: mediaPath, cfg: cfg, dispatcher: dispatcher}
	search := &searchHandler{db: db}
	admin := &adminHandler{
		db:          db,
		guard:       guard,
		authPlugins: authPlugins,
		authHandler: authHandler,
		dispatcher:  dispatcher,
		scheduler:   scheduler,
		mediaPath:   mediaPath,
		dataPath:    dataPath,
	}

	// ── Public ────────────────────────────────────────────────────────────────
	r.Get("/api/v1/system/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		// TODO: Mange the version here
		_, _ = w.Write([]byte("0.0.0"))
	})

	r.Get("/api/v1/system/health", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "database unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	r.Get("/api/v1/auth/providers", authHandler.listProviders)
	r.Post("/api/v1/auth/login", authHandler.login)
	r.Post("/api/v1/auth/refresh", authHandler.refresh)

	// ── Authenticated ─────────────────────────────────────────────────────────
	r.Group(func(r chi.Router) {
		r.Use(RequireAuth(jwtSecret, guard))

		r.Get("/api/v1/auth/me", authHandler.me)
		r.Post("/api/v1/auth/logout", authHandler.logout)
		r.Post("/api/v1/auth/credentials", authHandler.changeCredentials)
		r.Get("/api/v1/auth/devices", authHandler.listDevices)
		r.Delete("/api/v1/auth/devices/{id}", authHandler.deleteDevice)
		r.Post("/api/v1/auth/devices/{id}/ban", authHandler.banDevice)
		r.Delete("/api/v1/auth/devices/{id}/ban", authHandler.unbanDevice)

		// Collections
		r.Get("/api/v1/collections", collections.list)
		r.Get("/api/v1/collections/{id}", collections.get)
		r.Get("/api/v1/collections/{id}/collections", collections.listChildren)

		// Photo endpoints
		r.Get("/api/v1/collections/{id}/photos", photos.listPhotos)
		r.Get("/api/v1/collections/{id}/photos/stats", photos.photoStats)

		// Video endpoints
		r.Get("/api/v1/collections/{id}/videos", video.listVideos)
		r.Get("/api/v1/collections/{id}/items/{item_id}", video.getVideoItem)

		// Image serving
		r.Get("/images/{id}/{variant}", photos.serveVariant)
		r.Head("/images/{id}/{variant}", photos.serveVariant)
		r.Get("/images/{id}/exif", photos.getExif)
		r.Get("/images/{id}/tiles/image.dzi", photos.serveDZIManifest)
		r.Get("/images/{id}/tiles/*", photos.serveDZITile)
		r.Get("/images/collections/{id}/cover", photos.serveCollectionCover)
		r.Get("/images/artists/{id}/cover", music.serveArtistImage)
		r.Get("/images/albums/{albumId}/thumb", music.serveAlbumThumbnail)
		r.Get("/images/albums/{albumId}/cover", music.serveAlbumCover)

		// Music
		r.Get("/api/v1/collections/{collectionId}/artists", music.listArtists)
		r.Get("/api/v1/collections/{collectionId}/artists/{artistId}/albums", music.listArtistAlbums)
		r.Get("/api/v1/collections/{collectionId}/albums/{albumId}/tracks", music.listAlbumTracks)

		// Radio shows (audio:show collections)
		r.Get("/api/v1/collections/{collectionId}/shows", radio.listShows)
		r.Get("/api/v1/collections/{collectionId}/shows/{artistId}/episodes", radio.listShowEpisodes)
		r.Put("/api/v1/audio-shows/{artistId}/bookmark", radio.upsertShowBookmark)

		// Audio streaming
		r.Get("/audio/{id}/stream", music.stream)
		r.Head("/audio/{id}/stream", music.stream)

		// Video streaming
		r.Get("/videos/{id}/stream", video.stream)
		r.Get("/videos/{id}/raw", video.raw)
		r.Head("/videos/{id}/raw", video.raw)
		r.Get("/videos/{id}/hls/manifest.m3u8", video.hlsManifest)
		r.Get("/videos/{id}/hls/{segment}", video.hlsSegment)
		r.Get("/videos/{id}/live/manifest.m3u8", video.liveManifest)
		r.Get("/videos/{id}/live/{segment}", video.liveSegment)

		// Video images
		r.Get("/images/videos/{id}/cover", video.cover)

		// Video bookmarks
		r.Put("/api/v1/collections/{id}/items/{item_id}/bookmark", video.upsertBookmark)
		r.Delete("/api/v1/collections/{id}/items/{item_id}/bookmark", video.deleteBookmark)

		// Library-wide search
		r.Get("/api/v1/search/photos", search.searchPhotos)
		r.Get("/api/v1/search/photos/collections", search.searchPhotoCollections)
		r.Get("/api/v1/search/videos", search.searchVideos)
		r.Get("/api/v1/search/audio/artists", search.searchAudioArtists)
		r.Get("/api/v1/search/audio/albums", search.searchAudioAlbums)
		r.Get("/api/v1/search/audio/tracks", search.searchAudioTracks)
	})

	// ── Admin ─────────────────────────────────────────────────────────────────
	r.Group(func(r chi.Router) {
		r.Use(RequireAdmin(jwtSecret, guard))

		r.Get("/api/v1/admin/users", admin.listUsers)
		r.Post("/api/v1/admin/users", admin.createUser)
		r.Delete("/api/v1/admin/users/{id}", admin.deleteUser)
		r.Post("/api/v1/admin/users/{id}/credentials", admin.changeUserCredentials)
		r.Get("/api/v1/admin/users/{id}/devices", admin.listUserDevices)
		r.Delete("/api/v1/admin/users/{id}/devices", admin.revokeAllUserDevices)
		r.Delete("/api/v1/admin/users/{id}/devices/{deviceId}", admin.revokeUserDevice)

		r.Get("/api/v1/admin/collections", admin.listCollections)
		r.Post("/api/v1/admin/collections", admin.createCollection)
		r.Delete("/api/v1/admin/collections/{id}", admin.deleteCollection)
		r.Post("/api/v1/admin/collections/{id}/cover", admin.uploadCollectionCover)
		r.Delete("/api/v1/admin/collections/{id}/derivatives", admin.deleteDerivatives)
		r.Get("/api/v1/admin/collections/{id}/users", admin.listCollectionUsers)
		r.Post("/api/v1/admin/collections/{id}/users", admin.grantUsersCollectionAccess)

		r.Get("/api/v1/admin/jobs", admin.listJobs)
		r.Post("/api/v1/admin/jobs", admin.createJob)
		r.Get("/api/v1/admin/jobs/{id}", admin.getJob)
		r.Get("/api/v1/admin/jobs/{id}/events", admin.jobEvents)

		r.Get("/api/v1/admin/schedules", admin.listSchedules)
		r.Put("/api/v1/admin/schedules/{name}", admin.upsertSchedule)
		r.Delete("/api/v1/admin/schedules/{name}", admin.deleteSchedule)

		r.Get("/api/v1/admin/users/{userId}/collection_access", admin.getCollectionAccess)
		r.Patch("/api/v1/admin/users/{userId}/collection_access", admin.grantCollectionAccess)
		r.Post("/api/v1/admin/users/{userId}/collection_access", admin.setCollectionAccess)
		r.Delete("/api/v1/admin/users/{userId}/collection_access/{collectionId}", admin.revokeCollectionAccess)

		// Media item visibility
		r.Post("/api/v1/admin/media/{id}/hide", admin.hideMediaItem)
		r.Delete("/api/v1/admin/media/{id}/hide", admin.unhideMediaItem)

		// Video cover management
		r.Post("/api/v1/admin/media/{id}/cover", video.uploadCover)

		// Artist image management
		r.Post("/api/v1/admin/artists/{id}/image", music.uploadArtistImage)
		r.Delete("/api/v1/admin/artists/{id}/image", music.deleteArtistImage)

		// Album cover management
		r.Post("/api/v1/admin/albums/{albumId}/cover", music.uploadAlbumCover)
		r.Delete("/api/v1/admin/albums/{albumId}/cover", music.deleteAlbumCover)

		// Media directory browser
		r.Get("/api/v1/admin/directories", admin.listDirectories)
		r.Get("/api/v1/admin/directories/*", admin.listDirectories)
	})

	return r
}
