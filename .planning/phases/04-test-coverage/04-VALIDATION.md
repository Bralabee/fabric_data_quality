---
phase: 04
slug: test-coverage
status: draft
nyquist_compliant: false
wave_0_complete: false
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
| 04-01-01 | 01 | 1 | TEST-01 | unit | `pytest tests/test_fabric_connector.py --cov=dq_framework/fabric_connector --cov-fail-under=60` | ✅ | ⬜ pending |
| 04-01-02 | 01 | 1 | TEST-01 | unit | `pytest tests/test_fabric_connector.py --cov=dq_framework/fabric_connector --cov-fail-under=60` | ✅ | ⬜ pending |
| 04-02-01 | 02 | 1 | TEST-02 | unit | `pytest tests/test_loader.py --cov=dq_framework/loader --cov-fail-under=60` | ✅ | ⬜ pending |
| 04-02-02 | 02 | 1 | TEST-02 | unit | `pytest tests/test_batch_profiler.py --cov=dq_framework/batch_profiler --cov-fail-under=60` | ✅ | ⬜ pending |
| 04-03-01 | 03 | 2 | TEST-03, TEST-04 | characterization | `pytest tests/test_threshold_behavior.py -v` | ❌ W0 | ⬜ pending |
| 04-03-02 | 03 | 2 | TEST-04 | fixtures | `pytest tests/conftest.py --co` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_threshold_behavior.py` — stubs for TEST-03, TEST-04 characterization tests
- [ ] Shared Spark/Fabric mock fixtures in `tests/conftest.py`

*Existing test infrastructure covers framework and basic fixtures.*

---

## Manual-Only Verifications

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
