SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help
.NOTPARALLEL: run wait

ifneq ("$(wildcard .env)","")
include .env
endif

export

VENV ?= $(CURDIR)/venv
PYTHON := $(VENV)/bin/python
DBT := $(VENV)/bin/dbt
PYTHONPATH := $(CURDIR)/src
DBT_PROJECT_DIR := $(CURDIR)/sdpipe_transforms
DBT_PROFILES_DIR ?= $(HOME)/.dbt
RUN_DATE ?= $(if $(strip $(STAGING_RUN_DATE)),$(STAGING_RUN_DATE),$(shell date +%F))

REQUIRED_ENV_VARS := DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD AWS_S3_ENDPOINT AWS_S3_ACCESS_KEY AWS_S3_SECRET_KEY AWS_S3_BUCKET_NAME AWS_REGION STAGING_SOURCE_ROOT

.PHONY: help check-tools check-env check-dbt-profile up down ps logs wait-db wait-minio wait migrate stage-load dbt-deps dbt-debug dbt-run dbt-test run

help:
	@printf "%s\n" \
		"Available targets:" \
		"  help               Show this help message." \
		"  check-tools        Verify docker, $(PYTHON), and $(DBT) are available." \
		"  check-env          Verify required environment variables are set." \
		"  check-dbt-profile  Verify $(DBT_PROFILES_DIR)/profiles.yml contains the sdpipe_transforms profile." \
		"  up                 Start postgres and minio with docker compose." \
		"  down               Stop docker compose services." \
		"  ps                 Show docker compose service status." \
		"  logs               Show recent postgres and minio logs." \
		"  wait-db            Wait for postgres readiness." \
		"  wait-minio         Wait for MinIO readiness." \
		"  wait               Wait for postgres and MinIO readiness." \
		"  migrate            Run existing SQL migrations." \
		"  stage-load         Run the existing staging loader." \
		"  dbt-deps           Install dbt packages for sdpipe_transforms." \
		"  dbt-debug          Validate the dbt connection/profile." \
		"  dbt-run            Run dbt models from sdpipe_transforms." \
		"  dbt-test           Run dbt tests from sdpipe_transforms." \
		"  run                Execute the full local workflow." \
		"" \
		"Overridable variables:" \
		"  RUN_DATE           Defaults to STAGING_RUN_DATE or today's date." \
		"  DBT_PROFILES_DIR   Defaults to $(HOME)/.dbt."

check-tools:
	@set -euo pipefail; \
	command -v docker >/dev/null 2>&1 || { echo "Missing required tool: docker" >&2; exit 1; }; \
	docker compose version >/dev/null 2>&1 || { echo "Missing required docker compose support." >&2; exit 1; }; \
	[ -x "$(PYTHON)" ] || { echo "Missing required Python executable: $(PYTHON)" >&2; exit 1; }; \
	[ -x "$(DBT)" ] || { echo "Missing required dbt executable: $(DBT)" >&2; exit 1; }

check-env:
	@set -euo pipefail; \
	for var in $(REQUIRED_ENV_VARS); do \
		if [ -z "$${!var:-}" ]; then \
			echo "Missing required env var: $$var" >&2; \
			exit 1; \
		fi; \
	done

check-dbt-profile:
	@set -euo pipefail; \
	profile_path="$(DBT_PROFILES_DIR)/profiles.yml"; \
	[ -f "$$profile_path" ] || { echo "Missing dbt profile file: $$profile_path" >&2; exit 1; }; \
	grep -Eq '^[[:space:]]*sdpipe_transforms:' "$$profile_path" || { \
		echo "dbt profile 'sdpipe_transforms' not found in $$profile_path" >&2; \
		exit 1; \
	}

up:
	@docker compose up -d postgres minio

down:
	@docker compose down

ps:
	@docker compose ps

logs:
	@docker compose logs --tail=100 postgres minio

wait-db:
	@set -euo pipefail; \
	for attempt in $$(seq 1 30); do \
		if docker exec sdp_postgres pg_isready -U "$(DB_USER)" -d "$(DB_NAME)" >/dev/null 2>&1; then \
			echo "Postgres is ready."; \
			exit 0; \
		fi; \
		echo "Waiting for Postgres ($$attempt/30)..."; \
		sleep 2; \
	done; \
	echo "Postgres did not become ready in time." >&2; \
	exit 1

wait-minio:
	@set -euo pipefail; \
	url="$(AWS_S3_ENDPOINT)"; \
	scheme="$${url%%://*}"; \
	if [ "$$scheme" = "$$url" ]; then scheme="http"; fi; \
	rest="$${url#*://}"; \
	hostport="$${rest%%/*}"; \
	host="$${hostport%%:*}"; \
	port="$${hostport##*:}"; \
	if [ "$$host" = "$$port" ]; then \
		if [ "$$scheme" = "https" ]; then port=443; else port=80; fi; \
	fi; \
	[ -n "$$host" ] || { echo "Unable to determine MinIO host from AWS_S3_ENDPOINT=$(AWS_S3_ENDPOINT)" >&2; exit 1; }; \
	for attempt in $$(seq 1 30); do \
		if exec 3<>"/dev/tcp/$$host/$$port" 2>/dev/null; then \
			printf 'GET /minio/health/live HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n' "$$host" >&3; \
			IFS= read -r status_line <&3 || true; \
			exec 3<&-; \
			exec 3>&-; \
			case "$$status_line" in \
				*"200"*) echo "MinIO is ready."; exit 0 ;; \
			esac; \
		fi; \
		echo "Waiting for MinIO ($$attempt/30)..."; \
		sleep 2; \
	done; \
	echo "MinIO did not become ready in time." >&2; \
	exit 1

wait: wait-db wait-minio

migrate:
	@PYTHONPATH="$(PYTHONPATH)" "$(PYTHON)" scripts/migrate.py

stage-load:
	@PYTHONPATH="$(PYTHONPATH)" STAGING_RUN_DATE="$(RUN_DATE)" "$(PYTHON)" -m pipeline.staging.load_staging

dbt-deps:
	@"$(DBT)" deps --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"

dbt-debug:
	@"$(DBT)" debug --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"

dbt-run:
	@"$(DBT)" run --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"

dbt-test:
	@"$(DBT)" test --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"

run: check-tools check-env check-dbt-profile up wait migrate dbt-deps dbt-debug stage-load dbt-run dbt-test
