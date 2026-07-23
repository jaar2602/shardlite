# Build the meshdb **console** as one self-contained image.
#
# The backend embeds the built SPA at compile time (`include_dir!("$CARGO_MANIFEST_DIR/../web/dist")`),
# so the React app must be built FIRST and its `dist/` placed next to the server crate before cargo
# runs. Three stages: build the SPA, compile the server (embedding dist), ship a slim runtime.
#
# Build context is the `console/` directory (the server is a standalone crate — the repo's root
# Cargo workspace excludes `console/`, and the root .dockerignore excludes it from the meshdb build).

# 1. Frontend → web/dist
FROM node:22-slim AS web
WORKDIR /console/web
# Copy manifests first so `npm ci` is cached until dependencies actually change.
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# 2. Backend — embeds ../web/dist at compile time
FROM rust:1-slim AS build
WORKDIR /console
COPY server/ ./server/
COPY --from=web /console/web/dist ./web/dist
RUN cargo build --manifest-path server/Cargo.toml --release

# 3. Runtime — slim image, curl for the container healthcheck
FROM debian:stable-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /console/server/target/release/meshdb-console /usr/local/bin/meshdb-console
# 7100: the console web UI + JSON API. Data (users, connections, AI settings) lives under /data.
EXPOSE 7100
ENTRYPOINT ["meshdb-console"]
