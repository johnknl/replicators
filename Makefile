BIN_DIR ?= $(CURDIR)/bin
APP_NAME ?= replicators
BUILD_TAGS ?= 
COVERAGE_FILE ?= coverage.out
BENCH_DIR ?= $(CURDIR)/.bench
BENCH_BASELINE_FILE ?= $(BENCH_DIR)/baseline.txt
BENCH_CURRENT_FILE ?= $(BENCH_DIR)/current.txt
DOCS_DIR ?= $(CURDIR)/.docs

.PHONY: help
help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "; printf "Available targets:\n"} /^[a-zA-Z0-9_.%\/-]+:.*## / {printf "  make %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: project-init
project-init: tidy tools-tidy tools ## Initialize a freshly generated project

.PHONY: project-update
project-update: tidy tools-tidy tools ## Refresh tooling and deps after template update

.PHONY: tidy
tidy: ## Tidy and verify app module dependencies
	go mod tidy
	go mod verify

.PHONY: tools-tidy
tools-tidy: ## Tidy and verify tools module dependencies
	go -C tools mod tidy
	go -C tools mod verify

.PHONY: fmt
fmt: ## Format Go code
	go fmt ./...
	$(BIN_DIR)/golangci-lint run --fix

.PHONY: tools
tools: ## Install tools declared in tools/tools.go
	mkdir -p "$(BIN_DIR)"
	@for tool in $$(awk -F'"' '/^\t_ "[^"]+"/ {print $$2}' tools/tools.go); do \
		GOBIN="$(BIN_DIR)" go -C tools install $$tool; \
	done

.PHONY: lint
lint: tools ## Run golangci-lint (verify + lint)
	$(BIN_DIR)/golangci-lint config verify -c .golangci.yml
	$(BIN_DIR)/golangci-lint run

.PHONY: vuln
vuln: tools ## Run govulncheck
	$(BIN_DIR)/govulncheck ./...

.PHONY: run-example
run-example: ## Run the example application
	go run -race ./examples/sse

.PHONY: bench
bench: ## Run benchmarks
	go test -v -run ^$$ -bench . -benchmem -count=3 

.PHONY: bench-baseline
bench-baseline: ## Run and save baseline benchmarks
	mkdir -p "$(BENCH_DIR)"
	go test -run ^$$ -bench . -benchmem -count=6 ./... | tee "$(BENCH_BASELINE_FILE)"

.PHONY: bench-current
bench-current: ## Run and save current benchmarks
	mkdir -p "$(BENCH_DIR)"
	go test -run ^$$ -bench . -benchmem -count=6 ./... | tee "$(BENCH_CURRENT_FILE)"

.PHONY: bench-compare
bench-compare: tools ## Compare baseline and current benchmarks
	$(BIN_DIR)/benchstat "$(BENCH_BASELINE_FILE)" "$(BENCH_CURRENT_FILE)"

.PHONY: bench-run-compare
bench-run-compare: bench-current bench-compare ## Run current benchmarks and compare

.PHONY: test
test: ## Run all tests
	go test -v -race -tags "$$(make list-build-tags | tr '\n' ' ')" ./...

.PHONY: test-coverage
test-coverage: ## Run tests with coverage profile
	go test -v -race -covermode=atomic -coverprofile="$(COVERAGE_FILE)" -tags "$$(make list-build-tags | tr '\n' ' ')" ./...
	go tool cover -func="$(COVERAGE_FILE)"

.PHONY: test-%
test-%: ## Run tests by build tag (e.g. make test-unit)
	go test -tags "$*" ./...

.PHONY: check
check: vuln lint test ## Run all checks

.PHONY: upgrade-toolchain
upgrade-toolchain: ## Upgrade Go toolchain to the latest patch
	go get toolchain@patch

.PHONY: ci
ci: tidy tools-tidy check build ## Run full CI pipeline locally

.PHONY: list-build-tags
list-build-tags: ## List build tags declared in //go:build directives
	@ #all files, whether they would be built or not with the current environment
	@go_list_template='{{$$d := .Dir}}{{range .GoFiles}}{{$$d}}/{{.}} {{end}}{{range .TestGoFiles}}{{$$d}}/{{.}} {{end}}{{range .XTestGoFiles}}{{$$d}}/{{.}} {{end}}{{range .IgnoredGoFiles}}{{$$d}}/{{.}} {{end}}'; \
	\
	for f in $$(go list -f "$$go_list_template" ./...); do \
		awk '/^\/\/go:build[[:space:]]+/ { \
			sub(/^\/\/go:build[[:space:]]+/, ""); \
			gsub(/&&|\|\||[()!]/, " "); \
			for (i = 1; i <= NF; i++) print $$i; \
		}' "$$f"; \
	done | awk '$$0 != "" && $$0 != "true" && $$0 != "false" && !seen[$$0]++'
