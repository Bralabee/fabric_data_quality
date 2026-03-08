# ==================================================================================
# Makefile for Fabric Data Quality Framework
# ==================================================================================
# 
# This Makefile provides common development tasks.
# 
# Usage:
#   make help          # Show this help message
#   make install       # Install package in development mode
#   make test          # Run tests
#   make lint          # Run linters
#   make format        # Format code
#
# ==================================================================================

.PHONY: help install install-dev test test-cov lint format clean docs build

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)Fabric Data Quality Framework - Development Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# ==================================================================================
# Installation
# ==================================================================================

install: ## Install package in development mode
	@echo "$(BLUE)Installing package in development mode...$(NC)"
	pip install -e .

install-dev: ## Install package with development dependencies
	@echo "$(BLUE)Installing package with development dependencies...$(NC)"
	pip install -e ".[dev]"

install-conda: ## Create conda environment from environment.yml
	@echo "$(BLUE)Creating conda environment...$(NC)"
	conda env create -f environment.yml
	@echo "$(GREEN)Environment created! Activate with: conda activate fabric-dq$(NC)"

update-conda: ## Update conda environment
	@echo "$(BLUE)Updating conda environment...$(NC)"
	conda env update -f environment.yml --prune

# ==================================================================================
# Testing
# ==================================================================================

test: ## Run tests
	@echo "$(BLUE)Running tests...$(NC)"
	pytest tests/ -v

test-cov: ## Run tests with coverage report
	@echo "$(BLUE)Running tests with coverage...$(NC)"
	pytest tests/ -v --cov=dq_framework --cov-report=html --cov-report=term-missing
	@echo "$(GREEN)Coverage report generated in htmlcov/index.html$(NC)"

test-fast: ## Run tests (skip slow tests)
	@echo "$(BLUE)Running fast tests...$(NC)"
	pytest tests/ -v -m "not slow"

test-integration: ## Run integration tests only
	@echo "$(BLUE)Running integration tests...$(NC)"
	pytest tests/ -v -m "integration"

# ==================================================================================
# Code Quality
# ==================================================================================

lint: ## Run all linters
	@echo "$(BLUE)Running linters...$(NC)"
	@echo "$(YELLOW)Running ruff check...$(NC)"
	ruff check .
	@echo "$(YELLOW)Running mypy...$(NC)"
	mypy dq_framework/ --ignore-missing-imports || true
	@echo "$(GREEN)Linting complete!$(NC)"

format: ## Format code with ruff
	@echo "$(BLUE)Formatting code...$(NC)"
	ruff format .
	ruff check --fix .
	@echo "$(GREEN)Formatting complete!$(NC)"

format-check: ## Check code formatting without making changes
	@echo "$(BLUE)Checking code formatting...$(NC)"
	ruff format --check .
	ruff check .

security: ## Run security checks
	@echo "$(BLUE)Running security checks...$(NC)"
	@echo "$(YELLOW)Running bandit...$(NC)"
	bandit -r dq_framework/ -ll
	@echo "$(YELLOW)Running safety...$(NC)"
	safety check --file requirements.txt || true
	@echo "$(GREEN)Security checks complete!$(NC)"

# ==================================================================================
# Pre-commit
# ==================================================================================

pre-commit-install: ## Install pre-commit hooks
	@echo "$(BLUE)Installing pre-commit hooks...$(NC)"
	pre-commit install
	@echo "$(GREEN)Pre-commit hooks installed!$(NC)"

pre-commit-run: ## Run pre-commit hooks on all files
	@echo "$(BLUE)Running pre-commit hooks...$(NC)"
	pre-commit run --all-files

# ==================================================================================
# Documentation
# ==================================================================================

docs: ## Build documentation
	@echo "$(BLUE)Building documentation...$(NC)"
	cd docs && make html
	@echo "$(GREEN)Documentation built in docs/_build/html/index.html$(NC)"

docs-serve: ## Serve documentation locally
	@echo "$(BLUE)Serving documentation on http://localhost:8000$(NC)"
	cd docs/_build/html && python -m http.server 8000

# ==================================================================================
# Build and Package
# ==================================================================================

build: ## Build distribution packages
	@echo "$(BLUE)Building distribution packages...$(NC)"
	python -m build
	@echo "$(GREEN)Packages built in dist/$(NC)"

# ==================================================================================
# Cleaning
# ==================================================================================

clean: ## Clean build artifacts and cache files
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .tox/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete
	find . -type f -name '*.pyo' -delete
	find . -type f -name '*~' -delete
	@echo "$(GREEN)Cleanup complete!$(NC)"

clean-all: clean ## Deep clean including conda environment
	@echo "$(BLUE)Removing conda environment...$(NC)"
	conda env remove -n fabric-dq -y || true
	@echo "$(GREEN)Deep cleanup complete!$(NC)"

# ==================================================================================
# Development Workflow
# ==================================================================================

dev-setup: install-dev pre-commit-install ## Complete development setup
	@echo "$(GREEN)Development environment setup complete!$(NC)"
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  1. Run tests: make test"
	@echo "  2. Check code: make lint"
	@echo "  3. Format code: make format"

check-all: format-check lint test security ## Run all quality checks
	@echo "$(GREEN)All checks passed!$(NC)"

# ==================================================================================
# Continuous Integration
# ==================================================================================

ci: ## Run CI pipeline (format, lint, test, security)
	@echo "$(BLUE)Running CI pipeline...$(NC)"
	make format-check
	make lint
	make test-cov
	make security
	@echo "$(GREEN)CI pipeline complete!$(NC)"

# ==================================================================================
# Utility
# ==================================================================================

requirements: ## Generate requirements.txt from pyproject.toml
	@echo "$(BLUE)Generating requirements.txt...$(NC)"
	pip-compile pyproject.toml -o requirements.txt

version: ## Show package version
	@python -c "from dq_framework import __version__; print(__version__)" 2>/dev/null || echo "Package not installed"

info: ## Show environment information
	@echo "$(BLUE)Environment Information:$(NC)"
	@echo "Python version: $$(python --version)"
	@echo "Pip version: $$(pip --version)"
	@echo "Conda version: $$(conda --version 2>/dev/null || echo 'Not installed')"
	@echo "Virtual environment: $${CONDA_DEFAULT_ENV:-$${VIRTUAL_ENV:-None}}"
