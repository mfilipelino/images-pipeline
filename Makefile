.PHONY: install lint format check test clean run help

# Variables
PYTHON := uv run python
UV := uv

help:  ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:  ## Install dependencies
	$(UV) sync

lint:  ## Run linting with ruff
	$(UV) run ruff check .

format:  ## Format code with ruff
	$(UV) run ruff format .

check:  ## Run both linting and formatting checks
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fix:  ## Auto-fix linting issues and format code
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

test:  ## Run tests (add test runner when available)
	@echo "No test runner configured yet"

clean:  ## Clean up build artifacts and cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +

run-serial:  ## Run serial processing
	$(PYTHON) serial.py

run-multithread:  ## Run multithreaded processing
	$(PYTHON) multithreads.py

run-multiprocess:  ## Run multiprocess processing
	$(PYTHON) multiprocess.py

run-asyncio:  ## Run async processing
	$(PYTHON) asyncio.py

run-ray:  ## Run Ray distributed processing
	$(PYTHON) minimal-ray-glue-template.py

dev-setup:  ## Complete development setup
	$(UV) sync
	$(UV) run ruff check --fix .
	$(UV) run ruff format .