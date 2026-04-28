SHELL := /bin/bash

CHARTS ?= $(patsubst %/Chart.yaml,%,$(wildcard */Chart.yaml))
CHART_REGISTRY ?= oci://ghcr.io/xataio/maki/charts

GIT_COMMIT_FULL := $(shell git rev-parse HEAD)
GIT_COMMIT_SHORT := $(shell printf '%.7s' $(GIT_COMMIT_FULL))
CHART_VERSION := 0.0.0-g$(GIT_COMMIT_SHORT)

GO ?= go
CUE ?= $(GO) run cuelang.org/go/cmd/cue
KUBE_LINTER ?= $(GO) run golang.stackrox.io/kube-linter/cmd/kube-linter

env: # Print Makefile variables for debugging
	@echo "CHARTS: $(CHARTS)"
	@echo "CHART_REGISTRY: $(CHART_REGISTRY)"
	@echo "GIT_COMMIT_FULL: $(GIT_COMMIT_FULL)"
	@echo "GIT_COMMIT_SHORT: $(GIT_COMMIT_SHORT)"

version: # Print the chart version for the current commit
	@echo "$(CHART_VERSION)"

git-commit-sha: # Print the short git commit SHA
	@echo "$(GIT_COMMIT_SHORT)"

registry: # Print the chart registry/repo
	@echo "$(CHART_REGISTRY)"

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

# Version a chart with git commit
# Usage: make version-chart SERVICE=auth
version-chart:
	@echo "Setting $(SERVICE) version to $(CHART_VERSION), appVersion $(GIT_COMMIT_SHORT)"
	@yq eval '.version = "$(CHART_VERSION)" | .appVersion = "$(GIT_COMMIT_SHORT)"' -i $(SERVICE)/Chart.yaml

# Package and push a single chart
# Usage: make push-chart SERVICE=auth
push-chart: version-chart
	@set -e; \
	echo "Packaging $(SERVICE)"; \
	helm package $(SERVICE); \
	echo "Pushing $(SERVICE) to $(CHART_REGISTRY)"; \
	helm push $(SERVICE)-$(CHART_VERSION).tgz $(CHART_REGISTRY); \
	rm -f $(SERVICE)-$(CHART_VERSION).tgz

# Push all charts (discovered via glob)
push-charts:
	@set -e; \
	for chart in $(CHARTS); do \
		$(MAKE) push-chart SERVICE=$$chart; \
	done



lint: lint-helm lint-kube lint-cue

lint-helm:
	@set -e; \
	echo "Running helm lint..." >&2; \
	for chart in $(CHARTS); do \
		helm lint $$chart; \
	done

lint-cue:
	@set -e; \
	echo "Running cue vet..." >&2; \
	for chart in $(CHARTS); do \
		echo "Validating $$chart..." >&2; \
		helm template $$chart $$chart | $(CUE) vet \
			-l 'kind' -l 'metadata.name' ../cue/** yaml: - ; \
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
