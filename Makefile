SHELL := /bin/bash

GOLANGCI_VERSION ?= v2.15.0

.PHONY: verify fmt-check tidy-check vet build lint lint-selftest lint-install test format tidy

## verify: every check; the project gate and the CI entry point
verify: fmt-check tidy-check vet build lint lint-selftest test

## fmt-check: fail and print unformatted Go files
fmt-check:
	@files="$$(gofmt -l .)"; if [[ -n "$$files" ]]; then printf '%s\n' "$$files" >&2; exit 1; fi

## tidy-check: verify go.mod and go.sum are tidy
tidy-check:
	go mod tidy -diff

## vet: vet the packages without cgo
vet:
	CGO_ENABLED=0 go vet ./...

## build: prove the packages build without cgo
build:
	CGO_ENABLED=0 go build ./...

## lint: run golangci-lint and the text-level review checks
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not found — install with make lint-install" >&2; exit 1; }
	@actual="$$(golangci-lint version --short)"; expected="$(patsubst v%,%,$(GOLANGCI_VERSION))"; if [[ "$$actual" != "$$expected" ]]; then echo "golangci-lint version mismatch: expected $(GOLANGCI_VERSION), found $$actual" >&2; if [[ -n "$$CI" ]]; then exit 1; fi; fi
	CGO_ENABLED=0 golangci-lint run ./...
	bash scripts/lint-checks.sh

## lint-selftest: run the lint-check script self-tests
lint-selftest:
	bash scripts/lint-checks-selftest.sh

## lint-install: install the pinned golangci-lint version
lint-install:
	curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b "$$(go env GOPATH)/bin" $(GOLANGCI_VERSION)

## test: the suite under the race detector (race needs cgo on Linux)
test:
	CGO_ENABLED=1 go test -race ./...

## format: format the Go source files
format:
	gofmt -w .

## tidy: update the Go module files
tidy:
	go mod tidy
