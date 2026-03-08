---
phase: 5
slug: storage-abstraction
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-08
---

# Phase 5 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest >= 9.0.0 |
| **Config file** | `pyproject.toml` [tool.pytest.ini_options] |
| **Quick run command** | `pytest tests/test_storage.py -x` |
| **Full suite command** | `pytest --cov=dq_framework --cov-fail-under=60` |
| **Estimated runtime** | ~3 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_storage.py -x`
- **After every plan wave:** Run `pytest --cov=dq_framework --cov-fail-under=60`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 5 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 05-01-01 | 01 | 1 | STOR-01 | unit | `pytest tests/test_storage.py -x -k ResultStore` | ❌ W0 | ⬜ pending |
| 05-01-02 | 01 | 1 | STOR-01 | unit | `pytest tests/test_storage.py -x -k JSONFileStore` | ❌ W0 | ⬜ pending |
| 05-01-03 | 01 | 1 | STOR-01 | unit | `pytest tests/test_storage.py -x -k LakehouseStore` | ❌ W0 | ⬜ pending |
| 05-02-01 | 02 | 2 | STOR-02 | unit | `pytest tests/test_fabric_connector.py -x -k store` | ❌ W0 | ⬜ pending |
| 05-02-02 | 02 | 2 | STOR-03 | unit | `pytest tests/test_storage.py -x -k get_store` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_storage.py` — stubs for STOR-01, STOR-03 (JSONFileStore unit tests, LakehouseStore with mocked mssparkutils, get_store factory tests)
- [ ] Update `tests/test_fabric_connector.py` — covers STOR-02 (verify FabricDataQualityRunner delegates to ResultStore)
- [ ] Fixtures: `mock_mssparkutils` already exists in `conftest.py` — reuse for LakehouseStore tests

---

## Manual-Only Verifications

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
