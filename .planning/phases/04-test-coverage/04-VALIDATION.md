---
phase: 04
slug: test-coverage
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-08
---

# Phase 04 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x with pytest-cov |
| **Config file** | pyproject.toml [tool.pytest.ini_options] |
| **Quick run command** | `conda run -n fabric-dq python -m pytest tests/ -x -q` |
| **Full suite command** | `conda run -n fabric-dq python -m pytest tests/ --cov=dq_framework --cov-report=term-missing -q` |
| **Estimated runtime** | ~10 seconds |

---

## Sampling Rate

- **After every task commit:** Run `conda run -n fabric-dq python -m pytest tests/ -x -q`
- **After every plan wave:** Run `conda run -n fabric-dq python -m pytest tests/ --cov=dq_framework --cov-report=term-missing -q`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 04-01-01 | 01 | 1 | TEST-01 | fixtures | `pytest --fixtures -q \| grep mock_spark` | ✅ | ⬜ pending |
| 04-01-02 | 01 | 1 | TEST-01 | unit | `pytest tests/test_fabric_connector.py --cov=dq_framework/fabric_connector --cov-fail-under=60` | ✅ | ⬜ pending |
| 04-02-01 | 02 | 1 | TEST-02, TEST-03 | unit | `pytest tests/test_loader.py --cov=dq_framework/loader --cov-fail-under=60` | ✅ | ⬜ pending |
| 04-02-02 | 02 | 1 | TEST-02, TEST-03 | unit | `pytest tests/test_batch_profiler.py --cov=dq_framework/batch_profiler --cov-fail-under=60` | ✅ | ⬜ pending |
| 04-03-01 | 03 | 2 | TEST-04 | characterization | `pytest tests/test_threshold_characterization.py -v` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_threshold_characterization.py` — stubs for TEST-04 characterization tests

*Existing test infrastructure covers framework and basic fixtures.*

---

## Manual-Only Verifications

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 15s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-03-08
