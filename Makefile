.PHONY: init lint test ci smoke
init:
	python -m pip install --upgrade pip
	pip install -r requirements-dev.txt
	pre-commit install
	git config core.hooksPath .githooks

lint:
	ruff check . && ruff format --check .

test:
	pytest -q

ci:
	make lint && make test

smoke:
	bash scripts/smoke.sh
# Minimal Makefile for common tasks
.PHONY: build test

build:
	docker build -t pv-etl -f docker/etl-runner/Dockerfile docker/etl-runner

test:
	pytest -q
