SHELL := /bin/bash
GOVERSION := $(shell go version | awk '{print $$3}')
GO := GOTOOLCHAIN=$(GOVERSION) go
BUF := $(GO) run github.com/bufbuild/buf/cmd/buf
GOLANGCI := $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint
DOCKER_FLAGS=--rm --user $(shell id -u):$(shell id -g)
DOCKER_OPA := docker run $(DOCKER_FLAGS) -v $(PWD)/internal/opa:/policy openpolicyagent/opa:latest
DOCKER_JQ := docker run $(DOCKER_FLAGS) -v $(PWD):/data -w /data jq-tools

.PHONY: help
help:  ## This help dialog.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m\033[0m\n"} /^[$$()% 0-9a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: check
check: lint  ## CI code checks

.PHONY: lint
lint: lint-openapi lint-go lint-buf lint-opa lint-keycloak-turnstile lint-charts lint-kube ## Lint source code
	@echo "All lint tasks completed at $$(date)"

.PHONY: lint-charts
lint-charts: ## Lint Helm charts
	@cd charts && $(MAKE) lint

.PHONY: lint-kube
lint-kube: ## Lint Kubernetes manifests
	@cd kustomize && $(MAKE) lint

.PHONY: lint-openapi
lint-openapi: ## Lint OpenAPI code
	@cd openapi && $(MAKE) lint

.PHONY: lint-go
lint-go: ## Lint Go code
	@$(GOLANGCI) run ./...

.PHONY: lint-buf
lint-buf:
	@$(BUF) lint


.PHONY: lint-opa
lint-opa:
	@$(DOCKER_OPA) check /policy

.PHONY: lint-keycloak-turnstile
lint-keycloak-turnstile: ## Lint Keycloak Turnstile plugin (Kotlin)
	@cd dev/docker/keycloak/keycloak-turnstile && $(MAKE) lint

.PHONY: lint-workflows
lint-workflows: ## Lint GitHub Actions workflows
	@command -v actionlint >/dev/null 2>&1 || $(GO) install github.com/rhysd/actionlint/cmd/actionlint@latest
	@actionlint

.PHONY: fmt
fmt: tools fmt-openapi fmt-go fmt-buf fmt-opa fmt-json fmt-keycloak-turnstile ## Format source code
	@echo "All format tasks completed at $$(date)"

.PHONY: fmt-openapi
fmt-openapi:
	@cd openapi && $(MAKE) fmt

.PHONY: fmt-go
fmt-go: ## Format Go code (use FILES="path1 path2" for specific files/dirs)
	@FMT_ARGS=$$(if [ -z "$(FILES)" ]; then echo "."; else echo "$(FILES)"; fi); \
	$(GO) run mvdan.cc/gofumpt -w -modpath xata $$FMT_ARGS

.PHONY: fmt-buf
fmt-buf:
	@$(BUF) format -w

.PHONY: fmt-opa
fmt-opa:
	@$(DOCKER_OPA) fmt -w /policy

.PHONY: fmt-json
fmt-json:
	@$(DOCKER_JQ) jq -L /jq -f /jq/clean-realm.jq charts/keycloak/files/realm.json > charts/keycloak/files/realm.json.tmp && mv charts/keycloak/files/realm.json.tmp charts/keycloak/files/realm.json

.PHONY: fmt-keycloak-turnstile
fmt-keycloak-turnstile: ## Format Keycloak Turnstile plugin (Kotlin)
	@cd dev/docker/keycloak/keycloak-turnstile && $(MAKE) fmt

.PHONY: generate
generate: generate-openapi generate-buf generate-go generate-agents ## Generate code
	@echo "All generate tasks completed at $$(date)"

.PHONY: generate-openapi
generate-openapi:
	@cd openapi && $(MAKE) generate

.PHONY: generate-buf
generate-buf:
	@$(BUF) generate

.PHONY: generate-go
generate-go: ## Generate Go code (use FILES="path1 path2" for specific files/dirs)
	@GEN_ARGS=$$(if [ -z "$(FILES)" ]; then echo "./..."; else echo "$(FILES)"; fi); \
	GODEBUG=gotypesalias=0 $(GO) generate $$GEN_ARGS

.PHONY: generate-agents
generate-agents: ## Generate agent files
	cp AGENTS.md CLAUDE.md

.PHONY: test
test: ## Run unit and integration tests
	$(GO) test -coverprofile=coverage -timeout 5m -race -failfast -v ./...
	$(DOCKER_OPA) test /policy
	@cd dev/docker/keycloak/keycloak-turnstile && $(MAKE) test

.PHONY: test-e2e
test-e2e:
	$(GO) test -tags=e2e -timeout 5m -v ./e2e/...

tools: $(shell find ./dev/docker/jq-tools -type f)  ## Install/Build tools
	cd ./dev/docker/jq-tools && $(MAKE)

.PHONY: build-image
build-image: ## Build and push image. Requires IMAGE and PATHS. Optional: DOCKERFILE, BUILD_PATH, SERVICE_NAME, GIT_TOKEN, TAG_AS_LATEST, EXTRA_BUILD_ARGS.
	@set -euo pipefail; \
	image_name="$(IMAGE)"; \
	paths="$$PATHS"; \
	dockerfile="$(or $(DOCKERFILE),Dockerfile)"; \
	build_path="$(or $(BUILD_PATH),.)"; \
	service_name="$(or $(SERVICE_NAME),)"; \
	git_token="$(or $(GIT_TOKEN),)"; \
	tag_as_latest="$(or $(TAG_AS_LATEST),false)"; \
	extra_build_args="$(or $(EXTRA_BUILD_ARGS),)"; \
	force_build="$(or $(FORCE_BUILD),false)"; \
	\
	input_hash=$$( \
		while IFS= read -r path; do \
			[[ -z "$$path" ]] && continue; \
			git rev-parse "HEAD:$$path"; \
		done <<< "$$paths" \
		| sha256sum | cut -c1-12 \
	); \
	\
	image_tag="$$input_hash"; \
	image_reference="$${image_name}:$${image_tag}"; \
	\
	SAAS_SERVICES="auth billing projects clusterpool-operator product-analytics"; \
	service_path=""; \
	if [[ " $$SAAS_SERVICES " == *" $$service_name "* ]]; then \
		service_path="saas-services/$$service_name"; \
	fi; \
	\
	extra_args=(); \
	if [[ -n "$$git_token" ]]; then \
		extra_args+=("--build-arg" "GIT_TOKEN=$$git_token"); \
	fi; \
	if [[ -n "$$service_name" ]]; then \
		extra_args+=("--build-arg" "SERVICE_NAME=$$service_name"); \
	fi; \
	if [[ -n "$$service_path" ]]; then \
		extra_args+=("--build-arg" "SERVICE_PATH=$$service_path"); \
	fi; \
	if [[ -n "$$extra_build_args" ]]; then \
		while IFS= read -r arg; do \
			[[ -n "$$arg" ]] && extra_args+=("--build-arg" "$$arg"); \
		done <<< "$$extra_build_args"; \
	fi; \
	\
	if [[ "$$force_build" != "true" ]] && docker manifest inspect "$$image_reference" >/dev/null 2>&1; then \
		echo "Cache hit for $$image_reference — skip build/push" >&2; \
	else \
		(set -x; docker buildx build \
			-f "$$dockerfile" \
			"$${extra_args[@]}" \
			--platform linux/amd64,linux/arm64 \
			--cache-from "type=registry,ref=$$image_name:buildcache" \
			--cache-to "type=registry,ref=$$image_name:buildcache,mode=max" \
			--progress=plain \
			--push \
			-t "$$image_reference" \
			"$$build_path" \
		); \
	fi; \
	\
	if [[ "$$tag_as_latest" == "true" ]]; then \
		(set -x; docker buildx imagetools create --tag "$$image_name:latest" "$$image_reference"); \
	fi; \
	\
	echo "$$image_name:$$image_tag"

.PHONY: get-pr-info
get-pr-info: ## Get PR info for a commit (requires COMMIT=<sha> REPO=<owner/repo>)
	@if [ -z "$(COMMIT)" ] || [ -z "$(REPO)" ]; then \
		echo '{"error": "COMMIT and REPO are required"}'; \
		exit 1; \
	fi; \
	gh api "repos/$(REPO)/commits/$(COMMIT)/pulls" 2>/dev/null | \
		jq -c 'if .[0] then {number: .[0].number, url: .[0].html_url} else {} end' || \
		echo '{}'
