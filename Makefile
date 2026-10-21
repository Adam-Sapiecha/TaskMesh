.PHONY: test integration generate build check
test:
	go test ./...
integration:
	go test -count=1 -timeout=180s ./tests/integration
generate:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0 generate -f db/sqlc.yaml
build:
	go build -trimpath -o bin/taskmesh ./cmd/taskmesh
check:
	go vet ./...
	go test -race ./...
