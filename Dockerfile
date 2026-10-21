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

# Исходники и миграции копируются COPY (не bind mounts): содержимое COPY
# участвует в ключе кэша слоя, а bind mounts слой не инвалидируют — без этого
# слой переиспользовался бы со старым бинарником (например, с устаревшим
# набором миграций, встроенных макросом sqlx::migrate! при компиляции).
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY migrations ./migrations

# Build the application.
# Кэш-маунты (target, git db, registry) дают инкрементальность между сборками.
# Каталог migrations нужен на этапе сборки: макрос sqlx::migrate! встраивает
# миграции в бинарник при компиляции.
RUN --mount=type=cache,target=/app/target/ \
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

# Непривилегированный пользователь сервисов с фиксированными UID/GID:
# стабильные идентификаторы нужны, чтобы владелец каталога логов совпадал
# между образом и томом, а на хосте файлы тома имели предсказуемого
# владельца (10001:10001).
ARG APP_UID=10001
ARG APP_GID=10001
RUN addgroup -g "$APP_GID" app && adduser -D -H -u "$APP_UID" -G app app

WORKDIR /app

# Каталог логов принадлежит пользователю сервиса: named volume из compose.yaml
# при первом подключении инициализируется содержимым и владельцем из образа,
# поэтому запись в логи не зависит от владельца на хосте (bind mount перекрыл
# бы chown правами своего каталога).
RUN mkdir -p /app/logs && chown app:app /app/logs

# Copy the executable from the "build" stage.
COPY --from=build /bin/api /app/
COPY --from=build /bin/worker-cdc /app/

# Expose the port that the application listens on.
EXPOSE 8000

# Make executables executable
RUN chmod +x /app/api /app/worker-cdc

# Сервисам не нужен root: из привилегий — только порт 8000 (> 1024) и запись в
# /app/logs; бинарники остаются во владении root (только чтение и исполнение).
USER app:app

# Контейнер запускает процесс как PID 1 без init: ядро не доставляет сигналы
# с дефолтной диспетчеризацией PID 1, поэтому оба сервиса сами обрабатывают
# SIGTERM/SIGINT (worker-cdc — через tokio::signal, api — graceful shutdown).

# Что запускает контейнер по умолчанию; сервис worker-cdc в compose.yaml
# переопределяет команду на /app/worker-cdc.
CMD ["/app/api"]
