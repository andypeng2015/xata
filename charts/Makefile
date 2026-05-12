SHELL := /bin/bash

CHARTS ?= $(patsubst %/Chart.yaml,%,$(wildcard */Chart.yaml))
CHART_REGISTRY ?= oci://ghcr.io/xataio/maki/charts

GIT_COMMIT_SHORT := $(shell git rev-parse --short=7 HEAD)
APP_VERSION ?= $(GIT_COMMIT_SHORT)

GO ?= go
CUE ?= $(GO) run cuelang.org/go/cmd/cue
KUBE_LINTER ?= $(GO) run golang.stackrox.io/kube-linter/cmd/kube-linter

env: # Print Makefile variables for debugging
	@echo "CHARTS: $(CHARTS)"
	@echo "CHART_REGISTRY: $(CHART_REGISTRY)"

git-commit-sha: # Print the short git commit SHA
	@echo "$(GIT_COMMIT_SHORT)"

registry: # Print the chart registry/repo
	@echo "$(CHART_REGISTRY)"

build-deps: # Build helm charts dependencies
	@set -e; \
	for chart in $(CHARTS); do \
		if [ "$$(yq eval '.dependencies // "null"' "$$chart/Chart.yaml" 2>/dev/null)" != "null" ]; then \
			yq eval -r '.dependencies[] | "helm repo add " + .name + " " + .repository + " --force-update >/dev/null"' "$$chart/Chart.yaml" | sh; \
		fi; \
		echo "Building dependencies for $$chart..." >&2; \
		helm dependency build $$chart; \
	done

list-charts: # Print chart names, one per line
	@printf '%s\n' $(CHARTS)

# Inject tag for image into chart values.yaml files
# Usage: make inject-tags IMAGE=ghcr.io/xataio/xata/auth TAG=1234567-abc
# Note: Silently skips if chart directory doesn't exist or has no image section
inject-tags:
	@SERVICE=$$(basename "$(IMAGE)"); \
	if [ -d "$$SERVICE" ]; then \
		if yq eval 'has("image")' "$$SERVICE/values.yaml" 2>/dev/null | grep -q true; then \
			echo "Injecting image $(IMAGE):$(TAG) for $$SERVICE chart"; \
			yq eval '.image.repository = "$(IMAGE)" | .image.tag = "$(TAG)"' -i "$$SERVICE/values.yaml"; \
		fi; \
	fi

chart-info: # Print chart metadata as TSV. Override with CHARTS="auth projects".
	@set -euo pipefail; \
	for chart in $(CHARTS); do \
		if [ ! -d "$$chart" ]; then \
			echo "Chart $$chart does not exist"; \
			exit 1; \
		fi; \
		hash=$$( \
			find "$$chart" -type f | LC_ALL=C sort | while IFS= read -r file; do \
				rel="$${file#$$chart/}"; \
				printf 'path:%s\n' "$$rel"; \
				printf 'content:\n'; \
				if [[ "$$rel" == "Chart.yaml" ]]; then \
					yq eval 'del(.version) | del(.appVersion)' "$$file"; \
				else \
					cat "$$file"; \
				fi; \
				printf '\n'; \
			done | sha256sum | cut -c1-12 \
		); \
		printf '%s\t%s\t0.0.0-h%s\n' "$$chart" "$(CHART_REGISTRY)" "$$hash"; \
	done

# Version a chart with its content hash
# Override with CHARTS="auth projects".
set-chart-version:
	@set -euo pipefail; \
	info="$$( $(MAKE) --no-print-directory chart-info CHARTS="$(CHARTS)" )"; \
	while IFS=$$'\t' read -r chart _ version; do \
		app_version="$(APP_VERSION)"; \
		echo "Setting $$chart version to $$version, appVersion $$app_version"; \
		VERSION="$$version" APP_VERSION="$$app_version" \
			yq eval '.version = strenv(VERSION) | .appVersion = strenv(APP_VERSION)' -i "$$chart/Chart.yaml"; \
	done <<< "$$info"

# Package and push charts
# Override with CHARTS="auth projects".
push-charts: build-deps
	@set -euo pipefail; \
	info="$$( $(MAKE) --no-print-directory chart-info CHARTS="$(CHARTS)" )"; \
	while IFS=$$'\t' read -r chart _ version; do \
		if helm show chart "$(CHART_REGISTRY)/$$chart" --version "$$version" >/dev/null 2>&1; then \
			echo "Chart $$chart:$$version already exists - skip package/push"; \
			continue; \
		fi; \
		$(MAKE) set-chart-version CHARTS="$$chart"; \
		echo "Packaging $$chart"; \
		helm package "$$chart"; \
		echo "Pushing $$chart to $(CHART_REGISTRY)"; \
		helm push "$$chart-$$version.tgz" $(CHART_REGISTRY); \
		rm -f "$$chart-$$version.tgz"; \
	done <<< "$$info"

lint: lint-helm lint-kube lint-cue

lint-helm:
	@set -e; \
	echo "Running helm lint..." >&2; \
	for chart in $(CHARTS); do \
		helm lint $$chart; \
	done

lint-cue: build-deps
	@set -e; \
	echo "Running cue vet..." >&2; \
	for chart in $(CHARTS); do \
		echo "Validating $$chart..." >&2; \
		helm template $$chart $$chart \
			| yq eval '.metadata.namespace = (.metadata.namespace // "_")' - \
			| $(CUE) vet -l 'kind' -l 'metadata.namespace' -l 'metadata.name' ../cue/** yaml: - ; \
	done

lint-kube:
	@set -e; \
	echo "Running kube-linter..." >&2; \
	$(MAKE) --no-print-directory template 2>/dev/null \
		| $(KUBE_LINTER) lint -v --config "../.kube-linter.yml" -

template:
	@set -e; \
	for chart in $(CHARTS); do \
		echo "Render chart template for $$chart..." >&2; \
		helm template $$chart $$chart; \
	done
