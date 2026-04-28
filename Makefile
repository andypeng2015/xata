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

find-tags: # Finds first tag thats not latest for the given image
	@set -e; \
	FULL_IMAGE="${IMAGE}"; \
	IMAGE_WITHOUT_TAG="$${FULL_IMAGE%:*}"; \
	TARGET_DIGEST=$$(regctl manifest digest "$$FULL_IMAGE"); \
	TAGS_SORTED=$$(regctl tag ls "$$IMAGE_WITHOUT_TAG" | sort -t- -k1,1nr); \
	echo "$$TAGS_SORTED" | while read -r tag; do \
		if [ "$$tag" != "latest" ]; then \
			CURRENT_DIGEST=$$(regctl manifest digest "$${IMAGE_WITHOUT_TAG}:$${tag}"); \
			if [ "$$CURRENT_DIGEST" = "$$TARGET_DIGEST" ]; then \
				echo "$$tag"; \
				exit 0; \
			fi; \
		fi; \
	done

.PHONY: get-pr-info
get-pr-info: ## Get PR info for a commit (requires COMMIT=<sha> REPO=<owner/repo>)
	@if [ -z "$(COMMIT)" ] || [ -z "$(REPO)" ]; then \
		echo '{"error": "COMMIT and REPO are required"}'; \
		exit 1; \
	fi; \
	gh api "repos/$(REPO)/commits/$(COMMIT)/pulls" 2>/dev/null | \
		jq -c 'if .[0] then {number: .[0].number, url: .[0].html_url} else {} end' || \
		echo '{}'
