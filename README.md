# bokeh-mediaserver

Personal media server focused on photo viewing. Built in Go.

## Quick start (development)

**Requirements:** Go 1.26+, Docker, libvips (`apt install libvips-dev` / `brew install vips`), make

Run `make setup` after cloning to set up your repo for contributing to this project.

```bash
# 1. Start PostgreSQL
docker compose up postgres -d

# 2. Copy and configure environment
cp .env.example .env
# Edit .env — set MEDIA_PATH to your photo library, DATA_PATH to writable storage

# 3. Run the server (migrations run automatically on startup)
go run ./cmd/server
```

The server starts on `http://localhost:3000`.

Default dev credentials: **admin / admin** (seeded by migration — remove before production).

## First steps after starting

```bash
# Login and get a token
TOKEN=$(curl -s -X POST http://localhost:3000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"provider":"local","credentials":{"username":"admin","password":"admin"}}' \
  | jq -r .access_token)

# Create a photo collection pointing at your library
curl -X POST http://localhost:3000/api/v1/admin/collections \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Photos","type":"photo","root_path":"/path/to/your/photos"}'

# Trigger an index scan (returns a job ID immediately)
curl -X POST http://localhost:3000/api/v1/admin/collections/1/scan \
  -H "Authorization: Bearer $TOKEN"

# Poll job status
curl http://localhost:3000/api/v1/admin/jobs/1 \
  -H "Authorization: Bearer $TOKEN"

# Or stream live progress via SSE
curl -N http://localhost:3000/api/v1/admin/jobs/1/events \
  -H "Authorization: Bearer $TOKEN"
```

## Production (Docker)

```bash
cp .env.example .env
# Edit .env — set JWT_SECRET, MEDIA_PATH, DATA_PATH, POSTGRES_PASSWORD

docker compose --profile production up -d
```

## Multi-platform build (ARM64 + AMD64)

```bash
# One-time setup
docker buildx create --use --name multibuilder
docker run --privileged --rm tonistiigi/binfmt --install all

# Build and push
docker buildx build \
  --platform linux/arm64,linux/amd64 \
  --tag yourusername/mediaserver:latest \
  --push .
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `JWT_SECRET` | required | Secret key for signing JWTs |
| `DATA_PATH` | `./data` | Writable path for thumbnails, tiles, HLS cache |
| `MEDIA_PATH` | `/media` | Read-only path to media library (used by Docker mount) |
| `PORT` | `3000` | HTTP port |
| `WORKER_COUNT` | `2` | Max parallel indexer workers |
| `LOG_LEVEL` | `warn` | `error` / `warn` / `info` / `debug` |
| `LOG_PATH` | stdout | Log file path (`/tmp/mediaserver.log` recommended on Pi) |

## API summary

See `docs/api.md` for full documentation.

```
POST  /api/v1/auth/login
GET   /api/v1/auth/me
GET   /api/v1/collections
GET   /api/v1/collections/:id
GET   /api/v1/collections/:id/collections
GET   /api/v1/collections/:id/items
GET   /api/v1/collections/:id/slideshow
GET   /api/v1/media/:id
GET   /api/v1/media/:id/exif
GET   /images/:id/:variant          (thumb|small|preview|large)
GET   /images/:id/tiles/image.dzi
GET   /images/:id/tiles/*

POST  /api/v1/admin/collections
GET   /api/v1/admin/collections
POST  /api/v1/admin/collections/:id/scan
GET   /api/v1/admin/jobs/:id
GET   /api/v1/admin/jobs/:id/events  (SSE)

GET   /api/v1/system/health
```
