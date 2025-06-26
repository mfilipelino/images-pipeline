.PHONY: install update lint format typecheck test build docker-build docker-run ci hooks clean

install:
	uv sync --dev

update:
	uv lock --upgrade

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run pyright src/

test:
	uv run pytest -v

test-coverage:
	uv run pytest --cov=src --cov-report=term-missing --cov-report=html

build:
	uv build --out-dir dist/

docker-build:
	docker build -t images-pipeline .

docker-run:
	docker run --rm -it images-pipeline

ci: lint format typecheck test

hooks:
	uv run pre-commit install

clean:
	rm -rf dist/ .pytest_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true