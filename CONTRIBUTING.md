# Contributing to Drift

Thank you for your interest in contributing to Drift! We welcome contributions from the community.

## Code of Conduct

This project adheres to a Code of Conduct that all contributors are expected to follow. Please read [CODE_OF_CONDUCT](CODE_OF_CONDUCT.md) before contributing.

## How to Contribute

### Reporting Issues

If you find a bug or have a suggestion for improvement:

1. Check if the issue already exists in the GitHub issue tracker
2. If not, create a new issue with a clear title and description
3. Include steps to reproduce the problem (for bugs)
4. Provide your environment details (Python version, OS, etc.)

### Submitting Changes

1. Fork the repository
2. Create a new branch for your feature or bugfix (`git checkout -b feature/my-new-feature`)
3. Make your changes following our coding standards
4. Write or update tests as needed
5. Ensure all tests pass
6. Commit your changes with clear, descriptive commit messages
7. Push to your fork and submit a pull request

## Development Setup

### Getting Started

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/Drift.git
   cd Drift
   ```

2. **Install dependencies with `uv`:**
   ```bash
   uv sync
   ```

3. **Run tests:**
   ```bash
   uv run pytest
   ```

### Code Quality Standards

This project uses several tools to maintain code quality:

- **Black** for code formatting
- **Ruff** for linting
- **MyPy** for type checking
- **Pytest** for testing

#### Running Quality Checks

Before submitting a pull request, ensure your code passes all quality checks:

```bash
# Format code
uv run black src/

# Check linting
uv run ruff check src/

# Type checking
uv run mypy src/

# Run tests
uv run pytest
```

#### Code Style Guidelines

- Follow PEP 8 style guidelines (enforced by Black and Ruff)
- Write clear, descriptive variable and function names
- Add type hints to all functions
- Keep functions focused and concise
- Write docstrings for modules, classes, and functions

### Testing

- Write tests for new features and bug fixes
- Ensure existing tests continue to pass
- Aim for good test coverage
- Use descriptive test names that explain what is being tested

### Documentation

- Update the README.md if you change functionality
- Add docstrings to new functions and classes
- Update configuration examples if needed
- Document any new dependencies or requirements

## Project Structure

```
Drift/
├── src/
│   └── drift/
│       ├── __init__.py
│       ├── __main__.py
│       ├── config/
│       │   └── logging.ini
│       ├── registrating/
│       │   ├── data_asset_registrator.py
│       │   └── dataset_registrator.py
│       ├── retraining/
│       │   ├── job_group.py
│       │   ├── model_retrainer.py
│       │   └── training_status_refresher.py
│       └── tools/
│           └── azml.py
├── docs/
│   ├── drift.png
│   ├── example-databricks-workflow.yaml
│   ├── example-model-retrainer.conf
│   └── example-training-dataset-registrator.conf
├── pyproject.toml
└── README.md
```

## Pull Request Process

1. Update the README.md with details of changes if applicable
2. Update documentation and examples as needed
3. Ensure all tests pass and code quality checks succeed
4. Your pull request will be reviewed by maintainers
5. Address any feedback or requested changes
6. Once approved, your PR will be merged

## Development Dependencies

Development dependencies are managed in `pyproject.toml`. Install them with:

```bash
uv sync
```

## Questions?

If you have questions about contributing, feel free to:
- Open an issue for discussion
- Check existing documentation in the `docs/` directory
- Reach out to the maintainers

## License

By contributing to Drift, you agree that your contributions will be licensed under the Apache License 2.0.

---

Thank you for contributing to Drift! 🎉
