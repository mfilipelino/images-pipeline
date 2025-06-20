# Images Pipeline

A modern Python project for image processing pipeline built with best practices and production-ready tooling.

## 🚀 Quick Start

```bash
# Install dependencies
make install

# Install pre-commit hooks
make hooks

# Run lint and format checks
make ci

# Run tests
make test
```

## 📦 Project Structure

```
images_pipeline/
├── src/images_pipeline/   # Production code
│   ├── __init__.py
│   ├── main.py           # CLI entry point
│   └── module1.py        # Sample module
├── tests/                 # Test suite
│   ├── __init__.py
│   └── test_module1.py
├── pyproject.toml        # Single config for all tools
├── Makefile              # Common commands
├── Dockerfile            # Container definition
├── .pre-commit-config.yaml  # Git hooks
└── .github/workflows/ci.yml # CI/CD pipeline
```

## 🛠️ Development

### Available Commands

```bash
make install      # Install dependencies
make lint         # Run linting
make format       # Format code
make typecheck    # Run type checking
make test         # Run tests
make build        # Build package
make ci           # Run all checks
```

### Docker Support

```bash
make docker-build  # Build Docker image
make docker-run    # Run container
```

## 📋 Requirements

- Python >=3.12
- uv (for package management)
- Docker (optional, for containerization)

## 🤝 Contributing

1. Install pre-commit hooks: `make hooks`
2. Make your changes
3. Run `make ci` to ensure all checks pass
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License.