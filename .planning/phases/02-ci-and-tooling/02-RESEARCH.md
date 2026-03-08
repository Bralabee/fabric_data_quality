# Phase 2: CI and Tooling - Research

**Researched:** 2026-03-08
**Domain:** CI/CD pipeline, Python linting/formatting, dev dependency management
**Confidence:** HIGH

## Summary

Phase 2 migrates CI and developer tooling from a fragmented setup (flake8 + black + isort in CI, conflicting version pins in requirements-dev.txt vs pyproject.toml) to a unified ruff-based workflow. The pyproject.toml already has ruff fully configured (lint rules, format settings, isort settings); the work is replacing references in CI, pre-commit, and removing requirements-dev.txt.

The current state has three concrete problems: (1) CI workflow tests Python 3.8/3.9/3.10/3.11 but pyproject.toml declares >=3.10 with classifiers for 3.10-3.13, (2) CI installs and runs flake8/black/isort while pyproject.toml configures ruff, (3) requirements-dev.txt has conflicting older version pins (pytest>=7.4.0,<8.0.0 vs pyproject.toml's pytest>=9.0.0) and pulls in redundant tools (black, flake8, isort, pylint, autopep8, sphinx, jupyter, memory-profiler, etc.).

**Primary recommendation:** Replace all flake8/black/isort references with ruff in CI and pre-commit, align CI matrix to 3.10-3.13, delete requirements-dev.txt, and use `pip install -e ".[dev]"` everywhere.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PKG-04 | Align CI matrix with pyproject.toml (Python 3.10-3.13, ruff instead of flake8/black/isort) | CI workflow rewrite pattern documented; exact matrix and ruff commands provided |
| PKG-05 | Unify dev tooling on pyproject.toml [dev] extras (remove requirements-dev.txt conflicts) | Dependency audit complete; [dev] extras already defined in pyproject.toml; requirements-dev.txt deletion safe |
| PKG-06 | Update pre-commit config to use ruff instead of flake8/black/isort | ruff-pre-commit v0.15.5 config documented; replacement mapping provided |
</phase_requirements>

## Standard Stack

### Core
| Tool | Version | Purpose | Why Standard |
|------|---------|---------|--------------|
| ruff | >=0.15.0 | Linting + formatting (replaces flake8, black, isort) | Already configured in pyproject.toml; single tool, 10-100x faster |
| ruff-pre-commit | v0.15.5 | Pre-commit hooks for ruff | Official astral-sh hook; mirrors ruff releases |
| ruff-action | v3 | GitHub Actions ruff integration | Official astral-sh action; reads config from pyproject.toml |
| actions/setup-python | v5 | Python version matrix | Current stable version |
| actions/checkout | v4 | Repo checkout | Current stable version |
| actions/cache | v4 | Pip cache | Current stable version |

### Supporting
| Tool | Version | Purpose | When to Use |
|------|---------|---------|-------------|
| pre-commit-hooks | v4.5.0+ | YAML/JSON/TOML checks, trailing whitespace, etc. | Keep existing general hooks |
| bandit | 1.7.6+ | Security scanning | Keep existing security job |
| codecov-action | v4 | Coverage upload | Keep existing coverage upload |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| ruff-action@v3 | Manual `pip install ruff && ruff check` | Manual approach gives more control but ruff-action auto-detects pyproject.toml version |
| ruff-pre-commit | Local ruff via `language: system` | System hook is faster (no venv) but requires ruff installed locally |

**Installation:**
```bash
pip install -e ".[dev]"
pre-commit install
```

## Architecture Patterns

### CI Workflow Structure (target state)
```
.github/workflows/ci.yml
  lint job:        ruff check + ruff format --check (single Python version)
  test job:        pytest matrix [3.10, 3.11, 3.12, 3.13]
  build job:       python -m build + twine check
  security job:    bandit scan
```

### Pattern 1: Unified Dev Dependency Installation
**What:** All CI jobs install dev dependencies via `pip install -e ".[dev]"` instead of separate requirements files.
**When to use:** Every CI job that needs dev tools.
**Example:**
```yaml
# Source: pyproject.toml [project.optional-dependencies]
- name: Install dependencies
  run: |
    python -m pip install --upgrade pip
    pip install -e ".[dev]"
```

### Pattern 2: Ruff Lint Job (replacing flake8+black+isort)
**What:** Single lint job using ruff for both linting and format checking.
**When to use:** CI lint stage.
**Example:**
```yaml
# Source: https://docs.astral.sh/ruff/integrations/
lint:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-python@v5
      with:
        python-version: "3.12"
    - name: Install ruff
      run: pip install ruff
    - name: Ruff lint
      run: ruff check --output-format=github .
    - name: Ruff format check
      run: ruff format --check .
```

Alternative (using official action):
```yaml
lint:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: astral-sh/ruff-action@v3
```

### Pattern 3: Pre-commit with ruff
**What:** Replace black, isort, flake8 hooks with ruff-check and ruff-format.
**When to use:** .pre-commit-config.yaml.
**Example:**
```yaml
# Source: https://docs.astral.sh/ruff/integrations/
- repo: https://github.com/astral-sh/ruff-pre-commit
  rev: v0.15.5
  hooks:
    - id: ruff-check
      args: [--fix]
    - id: ruff-format
```

### Anti-Patterns to Avoid
- **Running both ruff and flake8/black/isort:** Conflicting rules and duplicate work. Remove ALL legacy linter references.
- **Pinning ruff-pre-commit rev to a different version than pyproject.toml ruff:** Can cause local vs CI drift. Keep versions aligned.
- **Keeping requirements-dev.txt "just in case":** Creates confusion about which file is authoritative. Delete it entirely.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Lint + format checking | Separate flake8 + black + isort steps | `ruff check` + `ruff format --check` | ruff replaces all three with consistent config in pyproject.toml |
| Pre-commit ruff integration | `language: system` hooks calling ruff | `astral-sh/ruff-pre-commit` | Official hook handles version management and caching |
| CI ruff step | Manual pip install + run | `astral-sh/ruff-action@v3` or simple pip install | Action auto-detects version from pyproject.toml |

**Key insight:** ruff already has all configuration in pyproject.toml. The work is removing old tools, not configuring new ones.

## Common Pitfalls

### Pitfall 1: Leaving flake8/isort config remnants
**What goes wrong:** Old .flake8, setup.cfg [isort] sections, or tox.ini entries cause confusion even if not used.
**Why it happens:** Legacy config files are easy to miss.
**How to avoid:** Search the entire repo for flake8, black, isort references (config files, CI, Makefile, etc.).
**Warning signs:** `grep -r "flake8\|isort\|black" --include="*.yml" --include="*.yaml" --include="*.cfg" --include="*.ini" --include="*.toml"`

### Pitfall 2: CI matrix mismatch with pyproject.toml
**What goes wrong:** CI tests Python 3.8/3.9 which pyproject.toml doesn't support (requires-python = ">=3.10").
**Why it happens:** CI was created before pyproject.toml was updated.
**How to avoid:** Matrix must be exactly ["3.10", "3.11", "3.12", "3.13"] to match classifiers.
**Warning signs:** CI passes on 3.8 but fails on 3.13, or vice versa.

### Pitfall 3: requirements-dev.txt version conflicts
**What goes wrong:** requirements-dev.txt pins pytest>=7.4.0,<8.0.0 but pyproject.toml [dev] has pytest>=9.0.0. Installing both causes unpredictable resolution.
**Why it happens:** Two files evolved independently.
**How to avoid:** Delete requirements-dev.txt entirely. Single source of truth is pyproject.toml [dev].

### Pitfall 4: CI still references requirements.txt
**What goes wrong:** CI runs `pip install -r requirements.txt` separately, but pyproject.toml already declares dependencies.
**Why it happens:** Legacy pattern from before pyproject.toml migration.
**How to avoid:** Use `pip install -e ".[dev]"` which installs both runtime and dev dependencies. The requirements.txt can remain for users who prefer it, but CI should use pyproject.toml.

### Pitfall 5: Pre-commit hook order matters
**What goes wrong:** ruff-format runs before ruff-check --fix, causing format violations from auto-fixes.
**Why it happens:** Hooks run in listed order.
**How to avoid:** Always list ruff-check (with --fix) BEFORE ruff-format.

### Pitfall 6: safety/python-safety-dependencies-check hook references requirements files
**What goes wrong:** The pre-commit config has a `python-safety-dependencies-check` hook with `files: requirements.*\.txt$`. After removing requirements-dev.txt, this hook may have nothing to check.
**Why it happens:** Hook was designed for requirements.txt-based workflows.
**How to avoid:** Either remove this hook or update it. Since requirements.txt still exists, it will still run, but verify.

## Code Examples

### Complete CI Workflow (target state)
```yaml
# Source: Synthesized from official docs and project requirements
name: Fabric Data Quality CI

on:
  push:
    branches: [master, main, develop]
  pull_request:
    branches: [master, main, develop]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install ruff
        run: pip install ruff
      - name: Ruff lint
        run: ruff check --output-format=github .
      - name: Ruff format check
        run: ruff format --check .

  test:
    runs-on: ubuntu-latest
    needs: lint
    strategy:
      fail-fast: false
      matrix:
        python-version: ["3.10", "3.11", "3.12", "3.13"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      - uses: actions/cache@v4
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-${{ matrix.python-version }}-${{ hashFiles('**/pyproject.toml') }}
          restore-keys: |
            ${{ runner.os }}-pip-${{ matrix.python-version }}-
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -e ".[dev]"
      - name: Run tests
        run: pytest tests/ -v --cov=dq_framework --cov-report=xml --cov-report=term-missing --cov-fail-under=60
      - name: Upload coverage
        if: matrix.python-version == '3.12'
        uses: codecov/codecov-action@v4
        with:
          file: ./coverage.xml
          flags: unittests
          fail_ci_if_error: false

  build:
    runs-on: ubuntu-latest
    needs: test
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Build and check package
        run: |
          pip install build twine
          python -m build
          twine check dist/*
      - uses: actions/upload-artifact@v4
        with:
          name: dist
          path: dist/

  security:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Security scan
        run: |
          pip install bandit
          bandit -r dq_framework/ -ll -ii
```

### Complete Pre-commit Config (target state)
```yaml
# Source: https://docs.astral.sh/ruff/integrations/ + existing hooks
repos:
  # Ruff: linting and formatting (replaces black, isort, flake8)
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.15.5
    hooks:
      - id: ruff-check
        args: [--fix]
      - id: ruff-format

  # Type checking
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.8.0
    hooks:
      - id: mypy
        additional_dependencies: [types-pyyaml, pandas-stubs]
        args: ['--ignore-missing-imports', '--strict-optional']

  # Security checks
  - repo: https://github.com/PyCQA/bandit
    rev: 1.7.6
    hooks:
      - id: bandit
        args: ['-r', 'dq_framework/', '-ll']

  # General file checks
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: check-yaml
        args: ['--safe']
      - id: check-json
      - id: check-toml
      - id: check-added-large-files
        args: ['--maxkb=1000']
      - id: check-merge-conflict
      - id: check-case-conflict
      - id: detect-private-key
      - id: end-of-file-fixer
      - id: trailing-whitespace
      - id: mixed-line-ending
```

### Hooks/configs to REMOVE
```
REMOVE from .pre-commit-config.yaml:
  - repo: https://github.com/psf/black          (replaced by ruff-format)
  - repo: https://github.com/PyCQA/isort         (replaced by ruff check with I rules)
  - repo: https://github.com/PyCQA/flake8        (replaced by ruff check)
  - repo: https://github.com/pycqa/pydocstyle    (can add D rules to ruff later if needed)
  - repo: https://github.com/Lucas-C/pre-commit-hooks-safety  (references requirements*.txt pattern)

DELETE file:
  - requirements-dev.txt

REMOVE from CI:
  - pip install flake8 black isort
  - flake8 commands
  - black --check commands
  - isort --check commands
  - pip install -r requirements-dev.txt
  - safety check (uses requirements.txt; optional to keep)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| flake8 + black + isort (3 tools) | ruff check + ruff format (1 tool) | ruff stable since 2024 | Single config in pyproject.toml, 10-100x faster |
| requirements-dev.txt | pyproject.toml [project.optional-dependencies] | PEP 621 (2021), widely adopted 2023+ | Single source of truth for versions |
| CI matrix 3.8-3.11 | CI matrix 3.10-3.13 | Python 3.8 EOL Oct 2024, 3.13 released Oct 2024 | Must match requires-python and classifiers |
| pip install -r requirements.txt + -r requirements-dev.txt | pip install -e ".[dev]" | pyproject.toml adoption | Editable install with dev extras covers everything |

## Open Questions

1. **Keep requirements.txt alongside pyproject.toml?**
   - What we know: pyproject.toml declares all runtime deps; requirements.txt has the same deps plus typing-extensions and python-dateutil
   - What's unclear: Whether any deployment pipeline depends on requirements.txt
   - Recommendation: Keep requirements.txt for now (Phase 2 scope is removing requirements-DEV.txt only). Can revisit in a later phase.

2. **safety check removal from CI?**
   - What we know: The `safety check` command in CI uses `pip install -r requirements.txt` then scans. The safety package has moved to a freemium model.
   - What's unclear: Whether the org has a safety API key or alternative vulnerability scanning
   - Recommendation: Remove `safety` from CI security job for now (it already has `|| true`). Keep bandit. Can add `pip-audit` as a replacement later if needed.

3. **pydocstyle hook removal**
   - What we know: pydocstyle is currently in pre-commit. Ruff has D rules for docstring linting but they are not currently enabled in pyproject.toml ruff config.
   - What's unclear: Whether the team actively enforces docstring conventions
   - Recommendation: Remove pydocstyle hook. If docstring linting is wanted, add "D" to ruff lint.select later.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest >=9.0.0 (from pyproject.toml [dev]) |
| Config file | pyproject.toml [tool.pytest.ini_options] |
| Quick run command | `pytest tests/ -x -q --no-cov` |
| Full suite command | `pytest tests/ -v --cov=dq_framework --cov-report=term-missing` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PKG-04 | CI matrix tests 3.10-3.13 with ruff | manual-only | Verify by reading .github/workflows/ci.yml | N/A (config file review) |
| PKG-05 | requirements-dev.txt removed; dev deps in pyproject.toml | smoke | `test ! -f requirements-dev.txt && pip install -e ".[dev]" && pytest --co -q` | No |
| PKG-06 | pre-commit uses ruff, no flake8/black/isort | smoke | `pre-commit run --all-files` | No |

### Sampling Rate
- **Per task commit:** `pre-commit run --all-files` (validates hooks work)
- **Per wave merge:** `pip install -e ".[dev]" && pytest tests/ -x -q --no-cov` (validates dev install works)
- **Phase gate:** Full test suite green + `pre-commit run --all-files` passes

### Wave 0 Gaps
None -- existing test infrastructure covers all phase requirements. This phase modifies CI/tooling config files, not application code. Validation is primarily config file review and smoke testing (pre-commit runs, pip install succeeds, CI YAML is valid).

## Sources

### Primary (HIGH confidence)
- [Ruff Integrations docs](https://docs.astral.sh/ruff/integrations/) - pre-commit config, GitHub Actions setup, version v0.15.5
- [astral-sh/ruff-pre-commit](https://github.com/astral-sh/ruff-pre-commit) - Hook IDs: ruff-check, ruff-format
- [astral-sh/ruff-action](https://github.com/astral-sh/ruff-action) - GitHub Action v3

### Secondary (MEDIUM confidence)
- [Ruff Lint and Format check GitHub Action](https://github.com/marketplace/actions/ruff-lint-and-format-check-in-pr-python-files) - Community action patterns
- [Automate Python Linting with Ruff and GitHub Actions](https://dev.to/ken_mwaura1/automate-python-linting-and-code-style-enforcement-with-ruff-and-github-actions-2kk1) - Workflow examples

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - ruff is already configured in pyproject.toml; just need to wire CI and pre-commit
- Architecture: HIGH - straightforward config file changes with clear before/after states
- Pitfalls: HIGH - well-documented migration path; main risks are leftover references

**Research date:** 2026-03-08
**Valid until:** 2026-04-08 (stable domain; ruff releases are backward compatible)
