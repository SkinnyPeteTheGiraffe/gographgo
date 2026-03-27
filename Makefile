.PHONY: help build test test-race lint fmt format vet tidy clean ci

GO ?= go
PKGS ?= ./...

help:
	@printf "Available targets:\n"
	@printf "  build      Build all packages\n"
	@printf "  test       Run unit tests\n"
	@printf "  test-race  Run tests with race detector\n"
	@printf "  lint       Run golangci-lint\n"
	@printf "  fmt        Format Go files with gofmt\n"
	@printf "  format     Alias for fmt\n"
	@printf "  vet        Run go vet\n"
	@printf "  tidy       Run go mod tidy\n"
	@printf "  clean      Remove build artifacts\n"
	@printf "  ci         Run fmt, build, vet, lint, and race tests\n"

build:
	$(GO) build $(PKGS)

test:
	$(GO) test $(PKGS)

test-race:
	$(GO) test -race $(PKGS)

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint is not installed"; \
		exit 1; \
	}
	golangci-lint run $(PKGS)

fmt:
	go fmt ./...

format: fmt

vet:
	$(GO) vet $(PKGS)

tidy:
	$(GO) mod tidy

clean:
	$(GO) clean -cache -testcache

ci: fmt build vet lint test-race
