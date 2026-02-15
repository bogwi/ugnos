# Multi-stage build for ugnosd. Build from ugnos project root (e.g. docker build -f Dockerfile .).
# Runtime: non-root user, minimal deps; PID 1 is ugnosd so SIGTERM triggers graceful shutdown.
# Requires Rust 1.85+ for edition 2024; use latest stable (e.g. 1.93) for builds.

ARG RUST_VERSION=1.93

# -----------------------------------------------------------------------------
# Build stage
# -----------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-bookworm AS build
ARG BIN_NAME=ugnosd
WORKDIR /app

# Copy source; .dockerignore excludes target/, dev/, etc., for smaller context and cache.
COPY . .

# Build release binary. --locked for reproducible builds.
RUN cargo build --release --bin "${BIN_NAME}" --locked \
    && cp "/app/target/release/${BIN_NAME}" /bin/ugnosd

# -----------------------------------------------------------------------------
# Runtime stage
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS final

# wget for healthcheck in compose/orchestration; no shell required for CMD.
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 10001 --system ugnos \
    && useradd --uid 10001 --gid ugnos --system --no-create-home --shell /usr/sbin/nologin ugnos

# Data directory: writable by ugnos; daemon creates it if missing but we pre-create for correct ownership.
ENV UGNOS_DATA_DIR=/var/lib/ugnos
RUN mkdir -p "${UGNOS_DATA_DIR}" && chown -R ugnos:ugnos "${UGNOS_DATA_DIR}"

USER ugnos

COPY --from=build /bin/ugnosd /usr/local/bin/ugnosd

# Health/readiness on 8080; use 0.0.0.0 so orchestrators can probe from outside the container.
ENV UGNOS_HTTP_BIND=0.0.0.0:8080
EXPOSE 8080

# Exec form so ugnosd is PID 1 and receives SIGTERM for graceful shutdown (WAL flush, compaction stop).
# Override with config file or env (e.g. docker run ... --config /etc/ugnosd.toml).
ENTRYPOINT ["/usr/local/bin/ugnosd"]
CMD ["--data-dir", "/var/lib/ugnos", "--http-bind", "0.0.0.0:8080"]
