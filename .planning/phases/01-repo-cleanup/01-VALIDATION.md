---
phase: 1
slug: repo-cleanup
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-08
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest >= 9.0.0 |
| **Config file** | pyproject.toml [tool.pytest.ini_options] |
| **Quick run command** | `pytest tests/ -x --no-cov -q` |
| **Full suite command** | `pytest tests/` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pip install -e . && python -c "import dq_framework"`
- **After every plan wave:** Run `pytest tests/ -x`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 01-01-01 | 01 | 1 | PKG-01 | smoke | `test ! -f setup.py && echo PASS` | N/A (shell) | ⬜ pending |
| 01-01-02 | 01 | 1 | PKG-01 | smoke | `pip install -e . && python -c "import dq_framework"` | N/A (shell) | ⬜ pending |
| 01-01-03 | 01 | 1 | PKG-02 | smoke | `test ! -d build -a ! -d dist -a ! -d htmlcov -a ! -f .coverage -a ! -f pipeline.log && echo PASS` | N/A (shell) | ⬜ pending |
| 01-01-04 | 01 | 1 | PKG-03 | smoke | `git check-ignore build/ dist/ htmlcov/ .coverage pipeline.log '*.egg-info/'` | N/A (shell) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements. No new test files needed — validation is via shell checks and the existing test suite (regression confirmation).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Doc references updated | PKG-01 | Content correctness requires human review | Grep for `setup.py` in docs/ and README.md; verify no stale references remain |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
