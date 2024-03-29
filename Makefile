SHELL = bash

all: lint

.PHONY: lint
lint: .prepare ## Lint the files
	@go mod tidy
	@golint ./...
	@golangci-lint run ./...

.PHONY: fix
fix: .prepare ## Lint and fix vialoations
	@go mod tidy
	@golangci-lint run --fix ./...

.PHONY: test
test: .prepare ## Run unittests
	. .env; go test --count 1 -v -timeout 300s -short ./...

.PHONY: one-test
one-test: .prepare ## Run one unittest
	. .env; go test --count 1 -v -timeout 30s -run ^$(FILTER) github.com/alwitt/goutils/...

.prepare: ## Prepare the project for local development
	@pip3 install --user pre-commit
	@pre-commit install
	@pre-commit install-hooks
	@GO111MODULE=on go install github.com/go-critic/go-critic/cmd/gocritic@v0.5.4
	@touch .prepare

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
