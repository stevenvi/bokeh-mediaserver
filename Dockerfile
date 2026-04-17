# syntax=docker/dockerfile:1

ARG VIPS_VERSION=8.18.0

# ── libvips build stage ─────────────────────────────────────────────────────
FROM golang:1.26-bookworm AS vips-builder

ARG VIPS_VERSION

RUN apt-get update && apt-get install -y --no-install-recommends \
    meson ninja-build \
    xz-utils \
    libglib2.0-dev \
    libexpat1-dev \
    libjpeg62-turbo-dev \
    libpng-dev \
    libwebp-dev \
    libheif-dev \
    libtiff-dev \
    libexif-dev \
    libarchive-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN wget -qO /tmp/vips.tar.xz \
    "https://github.com/libvips/libvips/releases/download/v${VIPS_VERSION}/vips-${VIPS_VERSION}.tar.xz" \
    && mkdir /tmp/vips-src \
    && tar -xf /tmp/vips.tar.xz -C /tmp/vips-src --strip-components=1 \
    && cd /tmp/vips-src \
    && meson setup build --buildtype=release --prefix=/opt/vips \
        -Ddeprecated=false \
        -Dexamples=false \
        -Dcplusplus=false \
    && cd build && meson compile && meson install \
    && rm -rf /tmp/vips.tar.xz /tmp/vips-src

# ── Go build stage ──────────────────────────────────────────────────────────
FROM golang:1.26-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-dev \
    libexpat1-dev \
    libjpeg62-turbo-dev \
    libpng-dev \
    libwebp-dev \
    libheif-dev \
    libtiff-dev \
    libexif-dev \
    libarchive-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY --from=vips-builder /opt/vips /opt/vips

ENV PKG_CONFIG_PATH=/opt/vips/lib/pkgconfig
ENV CGO_CFLAGS="-I/opt/vips/include"
ENV CGO_LDFLAGS="-L/opt/vips/lib -Wl,-rpath,/opt/vips/lib"

WORKDIR /src

# Cache deps before copying source
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=1 required for bimg (libvips bindings)
# GOARCH is set by Docker BuildKit automatically for multi-platform builds
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o /bin/mediaserver ./cmd/server

# ── Runtime stage ───────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# libvips42 brings the full transitive dependency tree for libvips and its
# format libraries (libwebpdemux, libwebpmux, liblzma, libzstd, etc.).
# We install it here for its deps; our newer /opt/vips build takes precedence
# via ldconfig because /opt/vips/lib is listed first.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libvips42 \
    libarchive13 \
    exiftool \
    ffmpeg \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=vips-builder /opt/vips /opt/vips
RUN echo "/opt/vips/lib" > /etc/ld.so.conf.d/vips.conf && ldconfig

COPY --from=builder /bin/mediaserver /bin/mediaserver

# Derived data and media are mounted as volumes at runtime
RUN mkdir -p /data /media

EXPOSE 3000

ENTRYPOINT ["/bin/mediaserver"]
