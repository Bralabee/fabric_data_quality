# Phase 1: Repo Cleanup - Research

**Researched:** 2026-03-08
**Domain:** Python packaging, repository hygiene, build artifact management
**Confidence:** HIGH

## Summary

Phase 1 is a straightforward repository cleanup: remove the legacy `setup.py`, clean local build artifacts, and ensure `.gitignore` prevents re-committing them. The actual scope is narrower than it appears because investigation reveals that **no build artifacts are currently tracked by git** -- they exist only as untracked local files. The `.gitignore` already covers all required patterns (`build/`, `dist/`, `htmlcov/`, `.coverage`, `*.log`, `*.egg-info/`). The only git-tracked file to remove is `setup.py`.

The primary risk is that removing `setup.py` could break the downstream AIMS Data Platform project or Fabric notebook deployment workflows. The `docs/FABRIC_INTEGRATION.md` file explicitly documents `python setup.py bdist_wheel` as the build method. This documentation must be updated alongside the removal. PEP 660 editable installs via `pyproject.toml` require pip >= 21.3 and setuptools >= 64.0, which should be verified.

**Primary recommendation:** Delete `setup.py` in its own commit, verify `pip install -e .` works with only `pyproject.toml`, update documentation references, and clean up untracked build artifacts from the working tree.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PKG-01 | Remove legacy setup.py entirely (pyproject.toml v2.0.0 is canonical) | setup.py is git-tracked and has conflicting deps (v1.2.0, GX <1.0, Python >=3.8). pyproject.toml already has complete build config. Removal is safe with pip >= 21.3. |
| PKG-02 | Remove committed build artifacts (build/, dist/, htmlcov/, .coverage, pipeline.log, egg-info) | Investigation shows NONE of these are git-tracked. They are untracked local files only. Task reduces to local cleanup (`rm -rf`) rather than `git rm`. |
| PKG-03 | Update .gitignore to prevent re-committing build artifacts | Current .gitignore already covers all listed patterns. May need minor additions for completeness but baseline coverage is solid. |
</phase_requirements>

## Standard Stack

### Core

No new libraries are needed for this phase. This is purely a file removal and configuration task.

| Tool | Version | Purpose | Why Standard |
|------|---------|---------|--------------|
| setuptools | >=65.0 | Build backend (already in pyproject.toml) | PEP 660 editable installs require >=64.0; already configured |
| pip | >=21.3 | Package installer | Required for PEP 660 editable installs from pyproject.toml |
| python -m build | latest | Wheel building (replaces `python setup.py bdist_wheel`) | PEP 517 standard; used instead of setup.py commands |

### Alternatives Considered

None -- this phase does not introduce any new dependencies.

## Architecture Patterns

### File Removal Pattern

The removal should follow this order to minimize risk:

1. **Verify** `pip install -e .` works without `setup.py` (in current env)
2. **Remove** `setup.py` in a dedicated commit (easily revertible)
3. **Update** documentation references to `setup.py` in a separate commit
4. **Clean** local build artifacts (not a git operation)

### Anti-Patterns to Avoid

- **Bundling setup.py removal with other changes:** If AIMS breaks, you need to revert just the setup.py removal. Keep it in its own commit.
- **Forgetting to update documentation:** `docs/FABRIC_INTEGRATION.md` line 24 says `python setup.py bdist_wheel`. This must change to `python -m build`.
- **Removing files that aren't tracked:** `git rm build/` will fail because `build/` is not tracked. Use `rm -rf` for local cleanup instead.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Building wheels | `python setup.py bdist_wheel` | `python -m build` | PEP 517 standard; works with pyproject.toml only |
| Editable installs | `python setup.py develop` | `pip install -e .` | PEP 660 standard; supported by pip >= 21.3 |

## Common Pitfalls

### Pitfall 1: Assuming Build Artifacts Are Git-Tracked

**What goes wrong:** Planning `git rm` operations for files that are only on the local filesystem.
**Why it happens:** The requirements say "remove committed build artifacts" but investigation shows none are committed.
**How to avoid:** The task for PKG-02 should use filesystem deletion (`rm -rf`), not `git rm`. The success criterion is that these directories do not exist, not that they are untracked.
**Warning signs:** `git rm` commands failing with "pathspec did not match any files."

### Pitfall 2: Breaking AIMS Downstream Integration

**What goes wrong:** AIMS Data Platform imports dq_framework as an editable sibling package. If AIMS CI or scripts use `python setup.py develop` or have pip < 21.3, removing setup.py breaks them.
**Why it happens:** The removal is tested locally with modern pip but not in all downstream environments.
**How to avoid:** Test `pip install -e .` in a clean venv with only `pyproject.toml` before committing. Check that `pyproject.toml` build-system section is correct (it is: `requires = ["setuptools>=65.0", "wheel"]`).
**Warning signs:** AIMS integration tests failing after the change.

### Pitfall 3: Not Updating Documentation

**What goes wrong:** Docs still reference `python setup.py bdist_wheel` after setup.py is removed, confusing future developers.
**Why it happens:** Documentation updates are forgotten when the focus is on the code change.
**How to avoid:** Search for all `setup.py` references across the repo and update them. Key files:
- `docs/FABRIC_INTEGRATION.md` (line 24: build command)
- `docs/FILE_STRUCTURE.md` (line 10: file listing)
- `docs/PROJECT_SUMMARY.md` (line 44: file listing)
- `README.md` (line 29: file structure)
- `.planning/codebase/STRUCTURE.md` (line 64: file listing)

### Pitfall 4: Gitignore Already Covers Everything

**What goes wrong:** Creating a large .gitignore diff when the existing file already handles all required patterns.
**Why it happens:** Not checking what's already in .gitignore before adding entries.
**How to avoid:** The current `.gitignore` already covers: `build/`, `dist/`, `htmlcov/`, `.coverage`, `*.log` (catches `pipeline.log`), `*.egg-info/`. Verify this is sufficient rather than adding redundant entries. The only potential addition is an explicit `pipeline.log` entry if the team wants specificity, but `*.log` already covers it.

## Code Examples

### Verifying Editable Install Without setup.py

```bash
# In a clean venv or the project's conda env:
# 1. Rename setup.py temporarily to simulate removal
mv setup.py setup.py.bak

# 2. Test editable install
pip install -e .

# 3. Verify import works
python -c "import dq_framework; print(dq_framework.__version__)"

# 4. Restore if needed
mv setup.py.bak setup.py
```

### Building Wheels Without setup.py (Updated Method)

```bash
# Old way (setup.py):
python setup.py bdist_wheel

# New way (PEP 517, pyproject.toml):
pip install build
python -m build --wheel
# Output: dist/fabric_data_quality-2.0.0-py3-none-any.whl
```

### Cleaning Local Build Artifacts

```bash
rm -rf build/ dist/ htmlcov/ fabric_data_quality.egg-info/
rm -f .coverage pipeline.log
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `setup.py` for packaging | `pyproject.toml` with PEP 517/660 | pip 21.3 (Oct 2021) | setup.py is fully redundant when pyproject.toml has [build-system] |
| `python setup.py bdist_wheel` | `python -m build` | PEP 517 (2017, widely adopted ~2021) | Build isolation, reproducibility |
| `python setup.py develop` | `pip install -e .` | PEP 660 (2021) | Works with pyproject.toml only |

## Existing State Inventory

**Git-tracked files to remove:**
- `setup.py` -- the ONLY tracked file that needs removal

**Untracked local files to clean (not in git):**
- `build/` directory
- `dist/` directory
- `htmlcov/` directory
- `fabric_data_quality.egg-info/` directory
- `.coverage` file
- `pipeline.log` file

**Gitignore coverage (already present):**
| Pattern | .gitignore Line | Covers |
|---------|-----------------|--------|
| `build/` | Line 7 | build/ directory |
| `dist/` | Line 9 | dist/ directory |
| `*.egg-info/` | Line 19 | fabric_data_quality.egg-info/ |
| `.coverage` | Line 38 | .coverage file |
| `htmlcov/` | Line 39 | htmlcov/ directory |
| `*.log` | Line 56 | pipeline.log |

**Documentation files referencing setup.py (need updates):**
1. `docs/FABRIC_INTEGRATION.md` -- build command (line 24)
2. `docs/FILE_STRUCTURE.md` -- file listing (line 10)
3. `docs/PROJECT_SUMMARY.md` -- file listing (line 44)
4. `README.md` -- file structure (line 29)
5. `.planning/codebase/STRUCTURE.md` -- file listing (line 64)

## Open Questions

1. **AIMS pip version in CI and Fabric notebooks**
   - What we know: PEP 660 editable installs require pip >= 21.3. Modern environments (2024+) all ship with pip >= 21.3.
   - What's unclear: Exact pip version in AIMS CI and Fabric notebook environments.
   - Recommendation: Test `pip install -e .` without setup.py as the first action. If it fails, the commit is easily revertible. Fabric notebooks in 2025/2026 use pip >= 23.x, so this is LOW risk.

2. **Whether .planning/ doc updates are in scope**
   - What we know: `.planning/codebase/STRUCTURE.md` and `.planning/codebase/CONCERNS.md` reference setup.py.
   - What's unclear: Whether planning docs should be updated as part of this phase or left for later.
   - Recommendation: Update user-facing docs (README, docs/) in this phase. Planning docs can be updated as a byproduct but are lower priority.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest >= 9.0.0 (configured in pyproject.toml) |
| Config file | `pyproject.toml` [tool.pytest.ini_options] |
| Quick run command | `pytest tests/ -x --no-cov -q` |
| Full suite command | `pytest tests/` |

### Phase Requirements to Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PKG-01 | setup.py does not exist | smoke | `test ! -f setup.py && echo PASS` | N/A (shell check) |
| PKG-01 | `pip install -e .` succeeds with pyproject.toml only | smoke | `pip install -e . 2>&1 && python -c "import dq_framework"` | N/A (shell check) |
| PKG-02 | No build artifacts in repo | smoke | `test ! -d build -a ! -d dist -a ! -d htmlcov -a ! -f .coverage -a ! -f pipeline.log -a ! -d fabric_data_quality.egg-info && echo PASS` | N/A (shell check) |
| PKG-03 | .gitignore blocks artifact patterns | smoke | `git check-ignore build/ dist/ htmlcov/ .coverage pipeline.log '*.egg-info/' && echo PASS` | N/A (shell check) |

### Sampling Rate

- **Per task commit:** `pip install -e . && python -c "import dq_framework"` (verify install still works)
- **Per wave merge:** Full pytest suite: `pytest tests/ -x`
- **Phase gate:** All smoke checks pass + full test suite green

### Wave 0 Gaps

None -- this phase does not require new test files. Validation is done via shell checks and the existing test suite (to confirm nothing regressed).

## Sources

### Primary (HIGH confidence)

- **Local repository investigation** -- `git ls-files`, `git check-ignore`, direct file inspection confirmed artifact tracking status and .gitignore coverage
- **pyproject.toml** -- Verified build-system configuration (setuptools>=65.0, wheel)
- **setup.py** -- Confirmed version conflict (v1.2.0 vs pyproject.toml v2.0.0) and dependency divergence
- **Existing project research** -- `.planning/research/PITFALLS.md` Pitfall 5 documents the AIMS compatibility concern

### Secondary (MEDIUM confidence)

- **PEP 660 requirements** -- pip >= 21.3 for editable installs via pyproject.toml (well-established since 2021)
- **PEP 517** -- `python -m build` as replacement for `setup.py bdist_wheel` (standard since ~2021)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - no new dependencies, just file operations
- Architecture: HIGH - straightforward removal pattern, well-documented in project's own research
- Pitfalls: HIGH - main risk (AIMS breakage) is already documented and mitigated by dedicated commit strategy

**Research date:** 2026-03-08
**Valid until:** 2026-04-08 (stable domain, no fast-moving dependencies)
