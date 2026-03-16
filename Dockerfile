# syntax=docker/dockerfile:1

# ── Build stage ────────────────────────────────────────────────────────────────
FROM golang:1.26-bookworm AS builder

# Install libvips-dev for bimg (CGo binding to libvips)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libvips-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Cache deps before copying source
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=1 required for bimg (libvips bindings)
# GOARCH is set by Docker BuildKit automatically for multi-platform builds
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o /bin/mediaserver ./cmd/server

# ── Runtime stage ──────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# Runtime deps: libvips, exiftool
RUN apt-get update && apt-get install -y --no-install-recommends \
    libvips42 \
    exiftool \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /bin/mediaserver /bin/mediaserver

# Derived data and media are mounted as volumes at runtime
RUN mkdir -p /data /media

EXPOSE 3000

ENTRYPOINT ["/bin/mediaserver"]
