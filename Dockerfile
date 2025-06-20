# Use official Python
FROM python:3.12-slim

# Install uv using the official installer
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Copy dependency files first for better caching
COPY pyproject.toml uv.lock ./

# Install production dependencies
RUN uv sync --frozen --no-dev

# Copy application code
COPY src/ ./src

# Run the application
CMD ["uv", "run", "python", "-m", "images_pipeline.main"]