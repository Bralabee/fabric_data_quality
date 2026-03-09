# ==================================================================================
# Makefile for Fabric Data Quality Framework
# ==================================================================================
#
# Prerequisites:
#   conda activate fabric-dq
#   pip install -e ".[dev]"
#
# Quick start:
#   make help       Show all commands
#   make test       Run tests
#   make format     Auto-format code
#   make dev-setup  Full dev environment setup
#
# ==================================================================================

.DEFAULT_GOAL := help

# All targets are phony (no file outputs)
.PHONY: help \
        install install-dev install-conda update-conda \
        test test-cov test-quick \
        lint format format-check \
        pre-commit-install pre-commit-run \
        docs docs-serve \
        build \
        clean clean-all \
        dev-setup check-all ci \
        version info

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m

# ==================================================================================
# Help
# ==================================================================================

help: ## Show this help message
	@echo ""
	@echo "  $(BLUE)Fabric Data Quality Framework$(NC)"
	@echo ""
	@echo "  $(YELLOW)INSTALLATION$(NC)"
	@echo "  install            Install package in editable mode"
	@echo "  install-dev        Install with dev dependencies (pytest, ruff, mypy)"
	@echo "  install-conda      Create conda env from environment.yml"
	@echo "  update-conda       Update existing conda env"
	@echo ""
	@echo "  $(YELLOW)TESTING$(NC)"
	@echo "  test               Run full test suite"
	@echo "  test-quick         Run tests, stop on first failure"
	@echo "  test-cov           Run tests with coverage report"
	@echo ""
	@echo "  $(YELLOW)CODE QUALITY$(NC)"
	@echo "  format             Auto-format code (ruff format + fix)"
	@echo "  format-check       Check formatting without changing files"
	@echo "  lint               Run ruff check + mypy"
	@echo "  pre-commit-install Install pre-commit hooks"
	@echo "  pre-commit-run     Run pre-commit on all files"
	@echo ""
	@echo "  $(YELLOW)DOCUMENTATION$(NC)"
	@echo "  docs               List available docs with titles"
	@echo "  docs-serve         Serve docs/ on http://localhost:8000"
	@echo ""
	@echo "  $(YELLOW)BUILD & PACKAGE$(NC)"
	@echo "  build              Build sdist and wheel into dist/"
	@echo ""
	@echo "  $(YELLOW)WORKFLOWS$(NC)"
	@echo "  dev-setup          One-command dev setup (install-dev + pre-commit)"
	@echo "  check-all          Run format-check + lint + test"
	@echo "  ci                 CI pipeline: format-check, lint, test-cov"
	@echo ""
	@echo "  $(YELLOW)MAINTENANCE$(NC)"
	@echo "  clean              Remove caches, build artifacts, coverage"
	@echo "  clean-all          clean + remove conda env"
	@echo "  version            Show installed package version"
	@echo "  info               Show Python/conda environment info"
	@echo ""

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

test-quick: ## Run tests, stop on first failure
	@echo "$(BLUE)Running tests (fail-fast)...$(NC)"
	pytest tests/ -x -q

test-cov: ## Run tests with coverage report
	@echo "$(BLUE)Running tests with coverage...$(NC)"
	pytest tests/ -v --cov=dq_framework --cov-report=html --cov-report=term-missing
	@echo "$(GREEN)Coverage report: htmlcov/index.html$(NC)"

# ==================================================================================
# Code Quality
# ==================================================================================

lint: ## Run ruff check and mypy
	@echo "$(BLUE)Running linters...$(NC)"
	ruff check dq_framework/
	mypy dq_framework/ --ignore-missing-imports || true
	@echo "$(GREEN)Linting complete$(NC)"

format: ## Format code with ruff
	@echo "$(BLUE)Formatting code...$(NC)"
	ruff format dq_framework/ tests/
	ruff check --fix dq_framework/ tests/
	@echo "$(GREEN)Formatting complete$(NC)"

format-check: ## Check code formatting without making changes
	@echo "$(BLUE)Checking code formatting...$(NC)"
	ruff format --check dq_framework/ tests/
	ruff check dq_framework/ tests/

# ==================================================================================
# Pre-commit
# ==================================================================================

pre-commit-install: ## Install pre-commit hooks
	@echo "$(BLUE)Installing pre-commit hooks...$(NC)"
	pre-commit install
	@echo "$(GREEN)Pre-commit hooks installed$(NC)"

pre-commit-run: ## Run pre-commit hooks on all files
	@echo "$(BLUE)Running pre-commit hooks...$(NC)"
	pre-commit run --all-files

# ==================================================================================
# Documentation
# ==================================================================================

docs: ## List available documentation in docs/
	@echo "$(BLUE)Documentation (docs/)$(NC)"
	@echo ""
	@ls -1 docs/*.md 2>/dev/null | while read f; do printf "  $(GREEN)%-45s$(NC) %s\n" "$$(basename $$f)" "$$(head -1 $$f | sed 's/^#* *//')"; done

docs-serve: ## Serve docs/ as local website (auto-selects free port)
	@cd docs && python -u -c "import http.server,socketserver;s=socketserver.TCPServer(('',0),http.server.SimpleHTTPRequestHandler);print('Serving docs at http://localhost:' + str(s.server_address[1]),flush=True);s.serve_forever()"

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
	@rm -rf build/ dist/ *.egg-info
	@rm -rf .pytest_cache/ .mypy_cache/ .coverage htmlcov/ .tox/ .ruff_cache/
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name '*.pyc' -delete 2>/dev/null || true
	@find . -type f -name '*.pyo' -delete 2>/dev/null || true
	@echo "$(GREEN)Clean complete$(NC)"

clean-all: clean ## Deep clean including conda environment
	@echo "$(BLUE)Removing conda environment...$(NC)"
	conda env remove -n fabric-dq -y || true
	@echo "$(GREEN)Deep clean complete$(NC)"

# ==================================================================================
# Development Workflows
# ==================================================================================

dev-setup: install-dev pre-commit-install ## Complete development setup
	@echo "$(GREEN)Development environment ready$(NC)"
	@echo ""
	@echo "  Next steps:"
	@echo "    make test       Run tests"
	@echo "    make lint       Check code"
	@echo "    make format     Format code"

check-all: format-check lint test ## Run all quality checks
	@echo "$(GREEN)All checks passed$(NC)"

ci: format-check lint test-cov ## CI pipeline: format-check, lint, test-cov
	@echo "$(GREEN)CI pipeline complete$(NC)"

# ==================================================================================
# Utility
# ==================================================================================

version: ## Show package version
	@python -c "from dq_framework import __version__; print(__version__)" 2>/dev/null || echo "Package not installed"

info: ## Show environment information
	@echo "$(BLUE)Environment Information$(NC)"
	@echo "  Python:    $$(python --version 2>&1)"
	@echo "  Pip:       $$(pip --version 2>&1 | cut -d' ' -f1-2)"
	@echo "  Conda:     $$(conda --version 2>/dev/null || echo 'Not installed')"
	@echo "  Conda env: $${CONDA_DEFAULT_ENV:-$${VIRTUAL_ENV:-None}}"
