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
func NewRouter(db *pgxpool.Pool, pool *jobs.Pool, jwtSecret, mediaPath, dataPath string) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(requestLogger)

	userRepo := repository.NewUserRepository(db)
	sessionRepo := repository.NewSessionRepository(db)
	collRepo := repository.NewCollectionRepository(db)
	mediaRepo := repository.NewMediaItemRepository(db)
	jobRepo := repository.NewJobRepository(db)

	authPlugins := authpkg.DefaultPlugins()
	auth := newAuthHandler(db, userRepo, sessionRepo, jwtSecret, authPlugins)
	collections := &collectionsHandler{collections: collRepo, media: mediaRepo}
	photos := &photosHandler{media: mediaRepo, dataPath: dataPath, mediaPath: mediaPath}
	admin := &adminHandler{
		db: db, users: userRepo, sessions: sessionRepo,
		collections: collRepo, media: mediaRepo, jobs: jobRepo, pool: pool,
		authPlugins: authPlugins, authHandler: auth,
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

	r.Get("/api/v1/auth/providers", auth.listProviders)
	r.Post("/api/v1/auth/login", auth.login)
	r.Post("/api/v1/auth/refresh", auth.refresh)

	// ── Authenticated ─────────────────────────────────────────────────────────
	r.Group(func(r chi.Router) {
		r.Use(RequireAuth(jwtSecret))

		r.Get("/api/v1/auth/me", auth.me)
		r.Post("/api/v1/auth/credentials", auth.changeCredentials)
		r.Get("/api/v1/auth/sessions", auth.listSessions)
		r.Delete("/api/v1/auth/sessions/{id}", auth.revokeSession)

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
		r.Get("/images/{id}/exif", photos.getExif)
		r.Get("/images/{id}/tiles/image.dzi", photos.serveDZIManifest)
		r.Get("/images/{id}/tiles/*", photos.serveDZITile)
	})

	// ── Admin ─────────────────────────────────────────────────────────────────
	r.Group(func(r chi.Router) {
		r.Use(RequireAdmin(jwtSecret))

		r.Post("/api/v1/admin/users", admin.createUser)
		r.Delete("/api/v1/admin/users/{id}", admin.deleteUser)
		r.Post("/api/v1/admin/users/{id}/credentials", admin.changeUserCredentials)
		r.Get("/api/v1/admin/users/{id}/sessions", admin.listUserSessions)
		r.Delete("/api/v1/admin/users/{id}/sessions", admin.revokeAllUserSessions)
		r.Delete("/api/v1/admin/users/{id}/sessions/{sessionId}", admin.revokeUserSession)

		r.Get("/api/v1/admin/collections", admin.listCollections)
		r.Post("/api/v1/admin/collections", admin.createCollection)
		r.Post("/api/v1/admin/collections/{id}/scan", admin.triggerScan)

		r.Get("/api/v1/admin/jobs/{id}", admin.getJob)
		r.Get("/api/v1/admin/jobs/{id}/events", admin.jobEvents)

		r.Patch("/api/v1/admin/users/{userId}/collection_access", admin.grantCollectionAccess)
		r.Post("/api/v1/admin/users/{userId}/collection_access", admin.setCollectionAccess)
		r.Delete("/api/v1/admin/users/{userId}/collection_access/{collectionId}", admin.revokeCollectionAccess)

		// Media item visibility
		r.Post("/api/v1/admin/media/{id}/hide", admin.hideMediaItem)
		r.Delete("/api/v1/admin/media/{id}/hide", admin.unhideMediaItem)

		// Maintenance
		r.Post("/api/v1/admin/maintenance/orphan-cleanup", admin.triggerOrphanCleanup)
		r.Post("/api/v1/admin/maintenance/integrity-check", admin.triggerIntegrityCheck)
	})

	return r
}
