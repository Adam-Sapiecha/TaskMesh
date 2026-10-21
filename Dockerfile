FROM --platform=$BUILDPLATFORM node:24-alpine AS frontend
WORKDIR /web
COPY web/package*.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM rust:1.99.1-bookworm AS build
ARG CARGO_BUILD_JOBS=2
ARG TARGETARCH
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
COPY db/migrations/ ./db/migrations/
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=taskmesh-target-${TARGETARCH},target=/src/target,sharing=locked \
    cargo build --release --locked && cp target/release/taskmesh /taskmesh

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 taskmesh && useradd --uid 10001 --gid taskmesh --no-create-home taskmesh
COPY --from=build /taskmesh /usr/local/bin/taskmesh
COPY --from=frontend /web/dist /app/web
ENV WEB_DIR=/app/web
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["taskmesh"]
CMD ["api"]
