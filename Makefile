# Minimal Makefile for common tasks
.PHONY: build test

build:
	docker build -t pv-etl -f docker/etl-runner/Dockerfile docker/etl-runner

test:
	pytest -q
