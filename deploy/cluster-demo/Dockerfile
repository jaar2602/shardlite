# Build the shardlite server (with the HTTP gateway + JSON-TCP), then run it in a slim image.
# Edition 2024 needs a recent toolchain; rust:1-slim tracks the latest stable 1.x.
FROM rust:1-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release --features cli,http,json-tcp --bin shardlite

FROM debian:stable-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/shardlite /usr/local/bin/shardlite
# 4600: client + intra-cluster peer traffic (one port, per shardlite's design). 8080: HTTP gateway.
EXPOSE 4600 8080
ENTRYPOINT ["shardlite"]
