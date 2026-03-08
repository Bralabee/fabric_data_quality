---
phase: 2
slug: ci-and-tooling
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-08
---

# Phase 2 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest >=9.0.0 (from pyproject.toml [dev]) |
| **Config file** | pyproject.toml [tool.pytest.ini_options] |
| **Quick run command** | `pytest tests/ -x -q --no-cov` |
| **Full suite command** | `pytest tests/ -v --cov=dq_framework --cov-report=term-missing` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pre-commit run --all-files`
- **After every plan wave:** Run `pip install -e ".[dev]" && pytest tests/ -x -q --no-cov`
- **Before `/gsd:verify-work`:** Full suite must be green + `pre-commit run --all-files`
- **Max feedback latency:** 60 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 1 | PKG-04 | config-review | `python -c "import yaml; y=yaml.safe_load(open('.github/workflows/ci.yml')); print(y)"` | N/A | ⬜ pending |
| 02-01-02 | 01 | 1 | PKG-04 | smoke | `ruff check --output-format=github . && ruff format --check .` | N/A | ⬜ pending |
| 02-02-01 | 02 | 1 | PKG-05 | smoke | `test ! -f requirements-dev.txt && pip install -e ".[dev]" && pytest --co -q` | No | ⬜ pending |
| 02-03-01 | 03 | 1 | PKG-06 | smoke | `pre-commit run --all-files` | No | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements. This phase modifies CI/tooling config files, not application code. Validation is primarily config file review and smoke testing (pre-commit runs, pip install succeeds, CI YAML is valid).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| CI matrix tests 3.10-3.13 | PKG-04 | Config file review — CI doesn't run locally | Read .github/workflows/ci.yml and verify matrix includes exactly ["3.10", "3.11", "3.12", "3.13"] |
| No flake8/black/isort references remain | PKG-04/06 | Negative assertion across repo | `grep -r "flake8\|isort\|black" --include="*.yml" --include="*.yaml" --include="*.cfg" .` should return empty |

*All other phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
