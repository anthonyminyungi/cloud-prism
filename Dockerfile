# Stage 1: Build the single-binary Rust web app & CLI
FROM rust:slim-bookworm AS builder

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --bin mybox-sync

# Stage 2: Minimal TrueNAS SCALE runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data /tmp/mybox_sync

COPY --from=builder /app/target/release/mybox-sync /usr/local/bin/mybox-sync

ENV PORT=3080 \
    DB_PATH=/data/sync_state.db \
    TMP_DIR=/tmp/mybox_sync \
    TZ=Asia/Seoul

EXPOSE 3080

ENTRYPOINT ["/usr/local/bin/mybox-sync"]
CMD ["serve"]
