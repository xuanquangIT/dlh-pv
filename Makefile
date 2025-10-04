.PHONY: help init lint test ci smoke up down ps logs health clean

help:
	@echo "PV Lakehouse - Makefile targets:"
	@echo "  init         - Install dev dependencies and setup git hooks"
	@echo "  lint         - Run ruff linter and formatter checks"
	@echo "  test         - Run pytest tests"
	@echo "  ci           - Run lint + test (CI pipeline)"
	@echo "  smoke        - Basic smoke test (start compose + wait)"
	@echo "  up           - Start core services (docker compose up)"
	@echo "  up-all       - Start all services including ML and orchestration"
	@echo "  down         - Stop all services"
	@echo "  ps           - Show running containers"
	@echo "  logs         - Tail logs from all services"
	@echo "  health       - Run health check script"
	@echo "  clean        - Remove volumes and cleanup"

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

# Docker Compose targets
up:
	cd docker && docker compose --profile core up -d

up-all:
	cd docker && docker compose --profile core --profile spark --profile ml --profile orchestrate up -d

down:
	cd docker && docker compose --profile "*" down

ps:
	cd docker && docker compose ps

logs:
	cd docker && docker compose logs -f

health:
	pwsh docker/scripts/stack-health.ps1

clean:
	cd docker && docker compose --profile "*" down -v
	@echo "Volumes removed. Use 'make up' to start fresh."
