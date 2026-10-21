# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.99.1
ARG API_NAME=api
ARG WORKER_CDC_NAME=worker-cdc

################################################################################
# Create a stage for building the application.

FROM rust:${RUST_VERSION}-alpine AS build

ARG API_NAME
ARG WORKER_CDC_NAME

WORKDIR /app

# Install host build dependencies.
RUN apk add --no-cache clang lld musl-dev git openssl-dev pkgconfig curl openssl-libs-static

# Build the application.
# Leverage a cache mount to /usr/local/cargo/registry/
# for downloaded dependencies, a cache mount to /usr/local/cargo/git/db
# for git repository dependencies, and a cache mount to /app/target/ for
# compiled dependencies which will speed up subsequent builds.
# Leverage a bind mount to the src directory to avoid having to copy the
# source code into the container. Once built, copy the executable to an
# output directory before the cache mounted /app/target is unmounted.
# Каталог migrations нужен на этапе сборки: макрос sqlx::migrate! встраивает
# миграции в бинарник при компиляции.
RUN --mount=type=bind,source=crates,target=crates \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock,rw \
    --mount=type=bind,source=migrations,target=migrations \
    --mount=type=cache,target=/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --release && \
    cp ./target/release/$API_NAME /bin/ && \
    cp ./target/release/$WORKER_CDC_NAME /bin/

################################################################################
# Create a new stage for running the application that contains the minimal
# runtime dependencies for the application. This often uses a different base
# image from the build stage where the necessary files are copied from the build
# stage.
FROM alpine:3.18 AS final

# curl — healthcheck сервиса api
RUN apk add --no-cache curl

WORKDIR /app

# Создать директорию для логов
RUN mkdir -p /app/logs

# Copy the executable from the "build" stage.
COPY --from=build /bin/api /app/
COPY --from=build /bin/worker-cdc /app/

# Expose the port that the application listens on.
EXPOSE 8000

# Make executables executable
RUN chmod +x /app/api /app/worker-cdc

# Контейнер запускает процесс как PID 1 без init: ядро не доставляет сигналы
# с дефолтной диспетчеризацией PID 1, поэтому оба сервиса сами обрабатывают
# SIGTERM/SIGINT (worker-cdc — через tokio::signal, api — graceful shutdown).

# Что запускает контейнер по умолчанию; сервис worker-cdc в compose.yaml
# переопределяет команду на /app/worker-cdc.
CMD ["/app/api"]
