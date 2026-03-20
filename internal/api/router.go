package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	authpkg "github.com/stevenvi/bokeh-mediaserver/internal/auth"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
	"github.com/stevenvi/bokeh-mediaserver/internal/repository"
)

// NewRouter builds and returns the fully configured Chi router.
func NewRouter(db *pgxpool.Pool, pool *jobs.Pool, guard *DeviceGuard, jwtSecret, mediaPath, dataPath string) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(requestLogger)

	rateLimiter := newLoginRateLimiter()

	userRepo := repository.NewUserRepository(db)
	deviceRepo := repository.NewDeviceRepository(db)
	collRepo := repository.NewCollectionRepository(db)
	mediaRepo := repository.NewMediaItemRepository(db)
	jobRepo := repository.NewJobRepository(db)

	authPlugins := authpkg.DefaultPlugins()
	authHandler := newAuthHandler(db, userRepo, deviceRepo, guard, rateLimiter, jwtSecret, authPlugins)
	collections := &collectionsHandler{collections: collRepo, media: mediaRepo}
	photos := &photosHandler{media: mediaRepo, dataPath: dataPath, mediaPath: mediaPath}
	admin := &adminHandler{
		db: db, users: userRepo, devices: deviceRepo, guard: guard,
		collections: collRepo, media: mediaRepo, jobs: jobRepo, pool: pool,
		authPlugins: authPlugins, authHandler: authHandler,
		mediaPath: mediaPath, dataPath: dataPath,
	}

	// ── Public ────────────────────────────────────────────────────────────────
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
		r.Post("/api/v1/auth/credentials", authHandler.changeCredentials)
		r.Get("/api/v1/auth/devices", authHandler.listDevices)
		r.Delete("/api/v1/auth/devices/{id}", authHandler.deleteDevice)
		r.Post("/api/v1/auth/devices/{id}/ban", authHandler.banDevice)
		r.Delete("/api/v1/auth/devices/{id}/ban", authHandler.unbanDevice)

		// Collections
		r.Get("/api/v1/collections", collections.list)
		r.Get("/api/v1/collections/{id}", collections.get)
		r.Get("/api/v1/collections/{id}/collections", collections.listChildren)
		r.Get("/api/v1/collections/{id}/items", collections.listItems)
		r.Get("/api/v1/collections/{id}/slideshow", collections.slideshow)

		// Media items and photos
		r.Get("/api/v1/media/{id}", photos.getItem)

		// Image serving
		r.Get("/images/{id}/{variant}", photos.serveVariant)
		r.Head("/images/{id}/{variant}", photos.serveVariant)
		r.Get("/images/{id}/exif", photos.getExif)
		r.Get("/images/{id}/tiles/image.dzi", photos.serveDZIManifest)
		r.Get("/images/{id}/tiles/*", photos.serveDZITile)
	})

	// ── Admin ─────────────────────────────────────────────────────────────────
	r.Group(func(r chi.Router) {
		r.Use(RequireAdmin(jwtSecret, guard))

		r.Post("/api/v1/admin/users", admin.createUser)
		r.Delete("/api/v1/admin/users/{id}", admin.deleteUser)
		r.Post("/api/v1/admin/users/{id}/credentials", admin.changeUserCredentials)
		r.Get("/api/v1/admin/users/{id}/devices", admin.listUserDevices)
		r.Delete("/api/v1/admin/users/{id}/devices", admin.revokeAllUserDevices)
		r.Delete("/api/v1/admin/users/{id}/devices/{deviceId}", admin.revokeUserDevice)

		r.Get("/api/v1/admin/collections", admin.listCollections)
		r.Post("/api/v1/admin/collections", admin.createCollection)
		r.Post("/api/v1/admin/collections/{id}/scan", admin.triggerScan)

		r.Get("/api/v1/admin/jobs/{id}", admin.getJob)
		r.Get("/api/v1/admin/jobs/{id}/events", admin.jobEvents)

		r.Get("/api/v1/admin/users/{userId}/collection_access", admin.getCollectionAccess)
		r.Patch("/api/v1/admin/users/{userId}/collection_access", admin.grantCollectionAccess)
		r.Post("/api/v1/admin/users/{userId}/collection_access", admin.setCollectionAccess)
		r.Delete("/api/v1/admin/users/{userId}/collection_access/{collectionId}", admin.revokeCollectionAccess)

		// Media item visibility
		r.Post("/api/v1/admin/media/{id}/hide", admin.hideMediaItem)
		r.Delete("/api/v1/admin/media/{id}/hide", admin.unhideMediaItem)

		// Maintenance
		r.Post("/api/v1/admin/maintenance/orphan-cleanup", admin.triggerOrphanCleanup)
		r.Post("/api/v1/admin/maintenance/integrity-check", admin.triggerIntegrityCheck)
		r.Post("/api/v1/admin/maintenance/device-cleanup", admin.triggerDeviceCleanup)
	})

	return r
}
