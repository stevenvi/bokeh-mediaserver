package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stevenvi/bokeh-mediaserver/internal/jobs"
)

// NewRouter builds and returns the fully configured Chi router.
func NewRouter(db *pgxpool.Pool, pool *jobs.Pool, jwtSecret, mediaPath, dataPath string) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(requestLogger)

	auth := newAuthHandler(db, jwtSecret)
	collections := &collectionsHandler{db: db}
	photos := &photosHandler{db: db, dataPath: dataPath}
	admin := &adminHandler{db: db, pool: pool, mediaPath: mediaPath, dataPath: dataPath}

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

	// ── Authenticated ─────────────────────────────────────────────────────────
	r.Group(func(r chi.Router) {
		r.Use(RequireAuth(jwtSecret))

		r.Get("/api/v1/auth/me", auth.me)

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

		r.Get("/api/v1/admin/collections", admin.listCollections)
		r.Post("/api/v1/admin/collections", admin.createCollection)
		r.Post("/api/v1/admin/collections/{id}/scan", admin.triggerScan)

		r.Get("/api/v1/admin/jobs/{id}", admin.getJob)
		r.Get("/api/v1/admin/jobs/{id}/events", admin.jobEvents)
	})

	return r
}
