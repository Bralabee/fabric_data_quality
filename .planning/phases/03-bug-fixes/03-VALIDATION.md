---
phase: 3
slug: bug-fixes
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-08
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest >=9.0.0 (from pyproject.toml [dev]) |
| **Config file** | pyproject.toml [tool.pytest.ini_options] |
| **Quick run command** | `python -m pytest tests/test_fabric_connector.py tests/test_ingestion.py -x -q` |
| **Full suite command** | `python -m pytest tests/ -x` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `python -m pytest tests/test_fabric_connector.py tests/test_ingestion.py -x -q`
- **After every plan wave:** Run `python -m pytest tests/ -x`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 03-01-01 | 01 | 1 | BUG-01 | unit (mocked Spark) | `python -m pytest tests/test_fabric_connector.py -x -k "chunk"` | ❌ W0 | ⬜ pending |
| 03-01-02 | 01 | 1 | BUG-02 | unit | `python -m pytest tests/test_fabric_connector.py -x -k "aggregate"` | ❌ W0 | ⬜ pending |
| 03-02-01 | 02 | 1 | BUG-03 | unit | `python -m pytest tests/test_ingestion.py -x` | ✅ (needs update) | ⬜ pending |
| 03-02-02 | 02 | 1 | BUG-04 | smoke | `test ! -f check_data.py` | N/A | ⬜ pending |
| 03-02-03 | 02 | 1 | BUG-05 | unit | `python -c "from dq_framework import __all__; assert not any(s.startswith('_') for s in __all__)"` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_fabric_connector.py` — add chunked validation tests (BUG-01), aggregation tests (BUG-02), __all__ audit test (BUG-05)
- [ ] `tests/test_ingestion.py` — update engine-related tests to reflect param removal (BUG-03)

*Existing test infrastructure covers framework needs. No new dependencies required.*

---

## Manual-Only Verifications

All phase behaviors have automated verification.

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
