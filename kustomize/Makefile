SHELL := /bin/bash

GIT_REF ?= main
KUSTOMIZE ?= kustomize --load-restrictor LoadRestrictionsNone
OVERLAYS ?= overlays/local overlays/local-saas
IMAGE_REPO ?= ghcr.io/xataio/maki

GIT_REMOTE := $(shell if git remote | grep -q "^upstream$$"; then echo "upstream"; else echo "origin"; fi)
GIT_BRANCH := $(or $(GITHUB_HEAD_REF),$(subst refs/heads/,,$(GITHUB_REF)),$(shell git branch --show-current 2>/dev/null || git rev-parse --abbrev-ref HEAD))
GIT_COMMIT_FULL := $(shell git rev-parse HEAD)
GIT_COMMIT_SHORT := $(shell printf '%.7s' $(GIT_COMMIT_FULL))

GO ?= go
KUBE_LINTER ?= $(GO) run golang.stackrox.io/kube-linter/cmd/kube-linter

env: # Print Makefile variables for debugging
	@echo "GIT_REF: $(GIT_REF)"
	@echo "KUSTOMIZE: $(KUSTOMIZE)"
	@echo "OVERLAYS: $(OVERLAYS)"
	@echo "IMAGE_REPO: $(IMAGE_REPO)"
	@echo "GIT_REMOTE: $(GIT_REMOTE)"
	@echo "GIT_BRANCH: $(GIT_BRANCH)"
	@echo "GIT_COMMIT_FULL: $(GIT_COMMIT_FULL)"
	@echo "GIT_COMMIT_SHORT: $(GIT_COMMIT_SHORT)"

build:
	@set -e; \
	for overlay in $(OVERLAYS); do \
		>&2 echo "Building $$overlay"; \
		$(KUSTOMIZE) build --enable-helm "$$overlay"; \
	done

lint:
	@set -e; \
	for overlay in $(OVERLAYS); do \
		>&2 echo "Linting $$overlay"; \
		$(KUSTOMIZE) build --enable-helm "$$overlay" \
			| yq 'with(select(.kind == "Secret"); del(.sops))' \
			| $(KUBE_LINTER) lint - --config "../.kube-linter.yml"; \
	done

diff-oci:
	@set -e; \
	for overlay in $(OVERLAYS); do \
		tmp_dir=$$(mktemp -d); \
		left_dir="$$tmp_dir/left"; \
		right_dir="$$tmp_dir/right"; \
		mkdir -p "$$left_dir" "$$right_dir"; \
		\
		image="oci://$(IMAGE_REPO)/$${overlay#overlays/}"; \
		flux pull artifact "$$image:current" --output "$$left_dir"; \
		flux pull artifact "$$image:$(GIT_COMMIT_SHORT)" --output "$$right_dir"; \
		echo ""; \
		diff -u \
			--label "$$overlay (current)" \
			--label "$$overlay ($(GIT_COMMIT_SHORT))" \
			"$$left_dir/manifests.yaml" "$$right_dir/manifests.yaml" || true; \
	done

diff:
	@dir=$$(git rev-parse --show-prefix) && \
	tmp_dir=$$(mktemp -d) && \
	git fetch $(GIT_REMOTE) $(GIT_REF) && \
	git worktree add --detach $$tmp_dir $(GIT_REMOTE)/$(GIT_REF) > /dev/null && \
	for overlay in $(OVERLAYS); do \
		echo ""; \
		diff -u \
			--label "$(GIT_REMOTE)/$(GIT_REF) (git)" \
			--label "$$overlay" \
			<( $(KUSTOMIZE) build --enable-helm "$$tmp_dir/$$dir$$overlay" ) \
			<( $(KUSTOMIZE) build --enable-helm "$$overlay" ) || true; \
	done; \
	find . -path '*/components/*/charts' -type d -exec rm -rf {} + 2>/dev/null; \
	git worktree remove --force $$tmp_dir

diff-summary:
	@diff_output=$$(mktemp) && \
	$(MAKE) diff > $$diff_output && \
	\
	current_overlay=""; \
	insertions=0; \
	deletions=0; \
	declare -a changed_overlays; \
	max_name_len=0; \
	total_insertions=0; \
	total_deletions=0; \
	while IFS= read -r line; do \
		if [[ "$$line" =~ ^\+\+\+\ (.+)$$ ]]; then \
			if [[ -n "$$current_overlay" ]]; then \
				changed_overlays+=("$$current_overlay|$$insertions|$$deletions"); \
				name_len=$${#current_overlay}; \
				if [ $$name_len -gt $$max_name_len ]; then \
					max_name_len=$$name_len; \
				fi; \
				total_insertions=$$((total_insertions + insertions)); \
				total_deletions=$$((total_deletions + deletions)); \
			fi; \
			current_overlay="$${BASH_REMATCH[1]}"; \
			insertions=0; \
			deletions=0; \
		elif [[ "$$line" =~ ^\+[^+] ]]; then \
			((insertions++)); \
		elif [[ "$$line" =~ ^\-[^-] ]]; then \
			((deletions++)); \
		fi; \
	done < $$diff_output; \
	if [[ -n "$$current_overlay" ]]; then \
		changed_overlays+=("$$current_overlay|$$insertions|$$deletions"); \
		name_len=$${#current_overlay}; \
		if [ $$name_len -gt $$max_name_len ]; then \
			max_name_len=$$name_len; \
		fi; \
		total_insertions=$$((total_insertions + insertions)); \
		total_deletions=$$((total_deletions + deletions)); \
	fi; \
	\
	if [ $${#changed_overlays[@]} -eq 0 ]; then \
		echo "No overlays changed"; \
	else \
		max_line_len=80; \
		max_visual=$$((max_line_len - max_name_len - 3)); \
		for overlay_stat in "$${changed_overlays[@]}"; do \
			IFS='|' read -r name ins del <<< "$$overlay_stat"; \
			total_changes=$$((ins + del)); \
			printf "%-*s | " $$max_name_len "$$name"; \
			if [ $$total_changes -gt $$max_visual ]; then \
				visual_ins=$$((ins * max_visual / total_changes)); \
				visual_del=$$((del * max_visual / total_changes)); \
				for i in $$(seq 1 $$visual_ins); do printf "+"; done; \
				for i in $$(seq 1 $$visual_del); do printf "-"; done; \
			else \
				for i in $$(seq 1 $$ins); do printf "+"; done; \
				for i in $$(seq 1 $$del); do printf "-"; done; \
			fi; \
			printf "\n"; \
		done; \
		echo ""; \
		echo "$${#changed_overlays[@]} overlay(s) changed, $$total_insertions insertions(+), $$total_deletions deletions(-)"; \
	fi; \
	\
	rm -f $$diff_output

list-overlays:
	@for overlay in $(OVERLAYS); do \
		echo $$overlay; \
	done

push:
	@set -e; \
	for overlay in $(OVERLAYS); do \
		manifests=$$(mktemp -d)/manifests.yaml; \
		echo "Building $$overlay"; \
		$(KUSTOMIZE) build --enable-helm "$$overlay" > $$manifests; \
		echo "Pushing $$overlay"; \
		image="oci://$(IMAGE_REPO)/$${overlay#overlays/}"; \
		flux push artifact $$image:$(GIT_COMMIT_SHORT) \
			--revision="$(GIT_BRANCH)" \
			--source="$$(git config --get remote.$(GIT_REMOTE).url)" \
			--path="$$manifests" \
			--reproducible; \
	done

check-deploy-needed:
	@deploy_needed=false; \
	for overlay in $(OVERLAYS); do \
		\
		image="$(IMAGE_REPO)/$${overlay#overlays/}"; \
		new_digest=$$(regctl manifest digest "$$image:$(GIT_COMMIT_SHORT)" 2>/dev/null); \
		current_digest=$$(regctl manifest digest "$$image:current" 2>/dev/null); \
		\
		if [ -z "$$new_digest" ]; then \
			echo "$$overlay: artifact not found, deployment needed"; \
			deploy_needed=true; \
		elif [ -z "$$current_digest" ]; then \
			echo "$$overlay: no current artifact, deployment needed"; \
			deploy_needed=true; \
		elif [ "$$new_digest" != "$$current_digest" ]; then \
			echo "$$overlay: artifact changed, deployment needed"; \
			deploy_needed=true; \
		else \
			echo "$$overlay: no changes"; \
		fi; \
	done; \
	if [ "$$deploy_needed" = true ]; then \
		echo "DEPLOY_NEEDED=true"; \
	else \
		echo "DEPLOY_NEEDED=false"; \
		exit 1; \
	fi

inject-tags:  # Inject tag for image into all kustomization.yaml files
	@find . -name "kustomization.yaml" -type f | while read -r file; do \
		if yq eval 'has("images")' "$$file" | grep -q true; then \
			if yq eval '.images[]? | select(.name == "$(IMAGE)")' "$$file" | grep -q .; then \
				echo "Injecting tag $(TAG) for image $(IMAGE) into $$file"; \
				yq eval '.images = (.images | map(select(.name == "$(IMAGE)") |= . + {"newTag":"$(TAG)"}))' -i "$$file"; \
			fi; \
		fi; \
	done

deploy:
	@set -e; \
	for overlay in $(OVERLAYS); do \
		image="oci://$(IMAGE_REPO)/$${overlay#overlays/}"; \
		echo "Tagging $$image:$(GIT_COMMIT_SHORT) as current"; \
		flux tag artifact $$image:$(GIT_COMMIT_SHORT) --tag current; \
	done
