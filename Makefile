.PHONY: all build k6 loader viewer paradedb-image pg-textsearch-image postgres-images clean test fmt lint deps help

GOBIN := $(shell if command -v go >/dev/null 2>&1; then go env GOPATH; else printf '%s/go' "$$HOME"; fi)/bin
XK6 := $(GOBIN)/xk6

# Default target
all: build

# Build everything
build: k6 loader viewer

# Build k6 with the search extension
k6: $(XK6)
	@echo "Building k6 with xk6-search extension..."
	$(XK6) build --with github.com/paradedb/benchmarker=.
	@echo "Done: ./k6"

$(XK6):
	@echo "Installing xk6..."
	go install go.k6.io/xk6/cmd/xk6@latest

# Build the loader CLI
loader:
	@echo "Building loader..."
	@mkdir -p bin
	go build -o bin/loader ./cmd/loader
	@echo "Done: ./bin/loader"

# Build the dashboard-viewer CLI
viewer:
	@echo "Building dashboard-viewer..."
	@mkdir -p bin
	go build -o bin/dashboard-viewer ./cmd/dashboard-viewer
	@echo "Done: ./bin/dashboard-viewer"

# Build ParadeDB 0.25.2 on the pinned PostgreSQL minor release
paradedb-image:
	docker build -t benchmarker-paradedb:pg18.6 docker/paradedb

# Build the pg_textsearch PostgreSQL image from the pinned upstream release
pg-textsearch-image:
	docker build -t benchmarker-pg-textsearch:pg18.6 docker/pg_textsearch

postgres-images: paradedb-image pg-textsearch-image

# Run tests
test:
	go test -v ./...

# Format code
fmt:
	go fmt ./...

# Lint code
lint:
	golangci-lint run

# Clean build artifacts
clean:
	rm -f k6
	rm -rf bin/

# Install dependencies
deps:
	go mod download
	go install go.k6.io/xk6/cmd/xk6@latest

# Help
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  all      Build everything (default)"
	@echo "  build    Build k6 and loader"
	@echo "  k6       Build k6 with xk6-search extension"
	@echo "  loader   Build the loader CLI to bin/"
	@echo "  viewer   Build the dashboard-viewer CLI to bin/"
	@echo "  paradedb-image Build ParadeDB 0.25.2 on PostgreSQL 18.6"
	@echo "  pg-textsearch-image Build benchmarker-pg-textsearch:pg18.6"
	@echo "  postgres-images Build both PostgreSQL 18.6 extension images"
	@echo "  test     Run tests"
	@echo "  fmt      Format code"
	@echo "  lint     Run golangci-lint"
	@echo "  clean    Remove build artifacts"
	@echo "  deps     Install dependencies (go modules + xk6)"
	@echo "  help     Show this help"
