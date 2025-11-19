# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/go/dockerfile-reference/

# Want to help us make this template better? Share your feedback here: https://forms.gle/ybq9Krt8jtBL3iCk7

ARG RUST_VERSION=1.90.0
ARG APP_NAME=api
ARG WORKER_APP_NAME=worker-app
ARG WORKER_DEBEZIUM_NAME=worker-debezium

################################################################################
# Create a stage for building the application.

FROM rust:${RUST_VERSION}-alpine AS build

ARG APP_NAME
ARG WORKER_APP_NAME
ARG WORKER_DEBEZIUM_NAME

WORKDIR /app

# Install host build dependencies.
RUN apk add --no-cache clang lld musl-dev git openssl-dev pkgconfig cmake make g++ curl openssl-libs-static

# Build the application.
# Leverage a cache mount to /usr/local/cargo/registry/
# for downloaded dependencies, a cache mount to /usr/local/cargo/git/db
# for git repository dependencies, and a cache mount to /app/target/ for
# compiled dependencies which will speed up subsequent builds.
# Leverage a bind mount to the src directory to avoid having to copy the
# source code into the container. Once built, copy the executable to an
# output directory before the cache mounted /app/target is unmounted.
RUN --mount=type=bind,source=crates,target=crates \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock,rw \
    --mount=type=bind,source=casbin.conf,target=casbin.conf \
    --mount=type=bind,source=policy.csv,target=policy.csv \
    --mount=type=bind,source=start.sh,target=start.sh \
    --mount=type=bind,source=migrations,target=migrations \
    --mount=type=cache,target=/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --release && \
    cp ./target/release/$APP_NAME /bin/ && \
    cp ./target/release/$WORKER_APP_NAME /bin/ && \
    cp ./target/release/$WORKER_DEBEZIUM_NAME /bin/ && \
    cp ./casbin.conf /bin/casbin.conf && \
    cp ./policy.csv /bin/policy.csv && \
    cp ./start.sh /bin/start.sh

################################################################################
# Create a new stage for running the application that contains the minimal
# runtime dependencies for the application. This often uses a different base
# image from the build stage where the necessary files are copied from the build
# stage.
#
# The example below uses the alpine image as the foundation for running the app.
# By specifying the "3.18" tag, it will use version 3.18 of alpine. If
# reproducibility is important, consider using a digest
# (e.g., alpine@sha256:664888ac9cfd28068e062c991ebcff4b4c7307dc8dd4df9e728bedde5c449d91).
FROM alpine:3.18 AS final

# Установить tini в runtime образе
RUN apk add --no-cache tini

WORKDIR /app

# Создать директорию для логов
RUN mkdir -p /app/logs

# Copy the executable from the "build" stage.
COPY --from=build /bin/api /app/
COPY --from=build /bin/worker-app /app/
COPY --from=build /bin/worker-debezium /app/
COPY --from=build /bin/casbin.conf /app/
COPY --from=build /bin/policy.csv /app/
COPY --from=build /bin/start.sh /app/

# Expose the port that the application listens on.
EXPOSE 8000

# Make executables executable
RUN chmod +x /app/api /app/worker-app /app/worker-debezium /app/start.sh

ENTRYPOINT ["tini", "--"]

# What the container should run when it is started.
CMD ["/app/start.sh"]
