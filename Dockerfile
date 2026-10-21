FROM --platform=$BUILDPLATFORM node:24-alpine AS frontend
WORKDIR /web
COPY web/package*.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS=linux
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags='-s -w' -o /taskmesh ./cmd/taskmesh

FROM alpine:3.23
RUN apk add --no-cache ca-certificates && addgroup -S -g 10001 taskmesh && adduser -S -u 10001 -G taskmesh taskmesh
COPY --from=build /taskmesh /usr/local/bin/taskmesh
COPY --from=frontend /web/dist /app/web
ENV WEB_DIR=/app/web
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["taskmesh"]
CMD ["api"]
