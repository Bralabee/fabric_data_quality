# Phase 5: Storage Abstraction - Research

**Researched:** 2026-03-08 (re-researched)
**Domain:** Python abstract storage layer with local JSON and MS Fabric Lakehouse backends
**Confidence:** HIGH

## Summary

Phase 5 creates a `ResultStore` abstraction that replaces the inline `_save_results_to_lakehouse` method in `fabric_connector.py` (lines 576-603) with a pluggable storage interface. Two concrete backends are needed: `JSONFileStore` for local development (using `pathlib` and `json` stdlib) and `LakehouseStore` for Fabric production (using `notebookutils.fs` / `mssparkutils.fs`). The runtime auto-selects the backend via the existing `_is_fabric_runtime()` function in `utils.py`.

This is a straightforward Python Strategy pattern via ABC with zero new external dependencies. The existing codebase already has all the primitives: `_is_fabric_runtime()` for environment detection, `mssparkutils.fs.put/head/ls/rm/exists` for Fabric I/O, and `json.dumps` for serialization. The task is to extract, formalize, and test these patterns behind a clean interface that future phases (8: Schema Evolution, 9: Validation History) can extend with new backends.

**Important discovery:** Microsoft has officially renamed `mssparkutils` to `notebookutils`. The old namespace is backward-compatible but will be retired. The existing codebase imports via `from notebookutils import mssparkutils` which continues to work. The `LakehouseStore` should import through the existing `utils.get_mssparkutils()` indirection to remain agnostic to this transition.

**Primary recommendation:** Use `abc.ABC` with four abstract methods (`write`, `read`, `list`, `delete`), implement two concrete classes in a new `dq_framework/storage.py` module, and add a `get_store()` factory function that auto-selects based on `_is_fabric_runtime()`.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| STOR-01 | Create ResultStore abstraction with JSONFileStore (local) and LakehouseStore (Fabric) backends | ABC pattern with two concrete implementations; factory function for auto-selection; all stdlib, no new deps |
| STOR-02 | Refactor existing `_save_results_to_lakehouse` into ResultStore backend | Extract inline code from `fabric_connector.py:576-603` into `LakehouseStore.write()`; replace both call sites (lines 207 and 287) with `self._store.write()` |
| STOR-03 | Support both local (SQLite/JSON) and Fabric (Parquet/Lakehouse) storage modes | Phase 5 implements JSON (local) and JSON-via-notebookutils (Fabric). The `ResultStore` ABC is designed so Phase 9 can add `SQLiteStore` / `ParquetStore` backends without interface changes. STOR-03 is satisfied by the architecture supporting both modes. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| abc (stdlib) | N/A | Abstract base class for ResultStore | Built-in, zero deps, standard Python pattern |
| json (stdlib) | N/A | Serialize/deserialize validation results | Already used in `_save_results_to_lakehouse` |
| pathlib (stdlib) | N/A | Local file path operations | Already used throughout codebase (`utils.py`, `loader.py`) |
| logging (stdlib) | N/A | Structured logging | Already used throughout codebase |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| notebookutils (mssparkutils) | Fabric-provided | Lakehouse file I/O (`fs.put`, `fs.head`, `fs.ls`, `fs.rm`, `fs.exists`) | Only in Fabric runtime; accessed via `utils.get_mssparkutils()` |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| abc.ABC | Protocol (typing) | Protocol is structural typing; ABC provides explicit enforcement and `register()` for third-party backends. ABC is better here because we want TypeError on instantiation if methods are missing |
| json files | SQLite | SQLite adds query capability but is Phase 9 scope (HIST-01). Phase 5 keeps it simple with JSON |
| Custom factory | Dependency injection framework | Over-engineering for two backends; simple factory function is sufficient |
| json.dumps default=str | Custom JSONEncoder | `default=str` handles datetime, Path, and other edge cases; the existing code already uses this pattern |

**Installation:**
```bash
# No new dependencies required - all stdlib
```

## Architecture Patterns

### Recommended Project Structure
```
dq_framework/
    storage.py           # NEW: ResultStore ABC + JSONFileStore + LakehouseStore + get_store()
    fabric_connector.py  # MODIFIED: use ResultStore instead of inline _save_results_to_lakehouse
    utils.py             # UNCHANGED: _is_fabric_runtime() already exists here
    constants.py         # MODIFIED: add DEFAULT_RESULTS_DIR constant
    __init__.py          # MODIFIED: export ResultStore, JSONFileStore, LakehouseStore, get_store
```

### Pattern 1: Strategy via Abstract Base Class
**What:** Define a `ResultStore` ABC with `write`, `read`, `list`, `delete` methods. Each backend implements all four.
**When to use:** When you have multiple interchangeable implementations of the same interface and want enforcement at instantiation time.
**Example:**
```python
# dq_framework/storage.py
from abc import ABC, abstractmethod
from typing import Any, Optional
import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class ResultStore(ABC):
    """Abstract interface for validation result persistence.

    Subclasses must implement all four CRUD operations.
    Phase 9 will add SQLiteStore and ParquetStore backends.
    """

    @abstractmethod
    def write(self, key: str, data: dict[str, Any]) -> None:
        """Write a validation result.

        Args:
            key: Unique identifier (e.g., "validation_mydata_20260308_120000")
            data: Validation result dictionary

        Raises:
            OSError: If write fails (callers can catch and log if desired)
        """

    @abstractmethod
    def read(self, key: str) -> dict[str, Any]:
        """Read a validation result by key.

        Args:
            key: Result identifier

        Returns:
            Validation result dictionary

        Raises:
            FileNotFoundError: If key does not exist
        """

    @abstractmethod
    def list(self, prefix: Optional[str] = None) -> list[str]:
        """List available result keys.

        Args:
            prefix: Optional prefix filter

        Returns:
            Sorted list of result keys
        """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a stored result.

        Args:
            key: Result identifier

        Returns:
            True if deleted, False if not found
        """
```

### Pattern 2: Factory Function with Auto-Detection
**What:** A `get_store()` function that returns the appropriate backend based on runtime environment.
**When to use:** When the caller should not need to know which backend is active.
**Example:**
```python
def get_store(
    results_dir: Optional[str] = None,
    backend: Optional[str] = None,
) -> ResultStore:
    """Create a ResultStore instance.

    Auto-selects backend based on runtime environment unless
    explicitly specified.

    Args:
        results_dir: Directory/path for results storage.
            Local: filesystem path (default: ./dq_results)
            Fabric: Lakehouse path (default: Files/dq_results)
        backend: Force a specific backend ("local" or "fabric").
            If None, auto-detects using _is_fabric_runtime().

    Returns:
        Configured ResultStore instance

    Raises:
        RuntimeError: If backend="fabric" but not in Fabric runtime
        ValueError: If backend is not "local", "fabric", or None
    """
    from .utils import _is_fabric_runtime

    if backend == "local" or (backend is None and not _is_fabric_runtime()):
        return JSONFileStore(results_dir=results_dir or "dq_results")
    elif backend == "fabric" or (backend is None and _is_fabric_runtime()):
        return LakehouseStore(results_dir=results_dir or "Files/dq_results")
    else:
        raise ValueError(f"Unknown backend: {backend!r}. Use 'local' or 'fabric'.")
```

### Pattern 3: Serialization Helper
**What:** Centralized function to prepare validation result dicts for JSON serialization.
**When to use:** Both backends need to strip non-serializable keys before writing.
**Example:**
```python
def _prepare_for_serialization(data: dict[str, Any]) -> str:
    """Remove non-serializable GX objects and convert to JSON string."""
    serializable = {k: v for k, v in data.items() if k != "validation_result"}
    return json.dumps(serializable, indent=2, default=str)
```

### Pattern 4: Key Naming Convention
**What:** Consistent key format for stored results: `validation_{batch_name}_{timestamp}`
**When to use:** All write operations should produce predictable, sortable keys.
**Example:**
```python
def make_result_key(batch_name: str) -> str:
    """Generate a storage key from batch name and current time."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in batch_name)
    return f"validation_{safe_name}_{timestamp}"
```

### Anti-Patterns to Avoid
- **Leaking backend details:** After refactor, `FabricDataQualityRunner` must not reference `mssparkutils` directly for result storage. All storage goes through `ResultStore`.
- **Catching all exceptions silently:** The existing `_save_results_to_lakehouse` catches all exceptions and logs errors but swallows them. The new `write()` method should raise by default. The caller (`FabricDataQualityRunner`) can wrap in try/except with logging to match current behavior, but downstream consumers (Phase 9, Phase 10) get the choice.
- **Storing non-serializable data:** The existing code filters out `validation_result` (GX object). The `_prepare_for_serialization` helper should be used by both backends.
- **Hardcoded paths:** Use configurable `results_dir` parameter with sensible defaults (`"dq_results"` locally, `"Files/dq_results"` in Fabric).
- **Importing notebookutils at module level:** This will crash on local machines. Use lazy imports inside `LakehouseStore` methods or go through `utils.get_mssparkutils()`.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Abstract class enforcement | Manual isinstance checks | `abc.ABC` + `@abstractmethod` | ABC raises TypeError on instantiation if methods missing |
| JSON serialization of datetimes | Custom JSONEncoder subclass | `json.dumps(data, default=str, indent=2)` | Already used in existing code; handles datetime, Path, etc. |
| Path manipulation | String concatenation (`"/".join(...)`) | `pathlib.Path` for local, string concat for Fabric paths | Cross-platform locally; Fabric paths are ABFSS strings |
| Fabric runtime detection | New detection logic | `utils._is_fabric_runtime()` | Already exists, tested, single source of truth |
| Fabric fs operations | Direct notebookutils import | `utils.get_mssparkutils()` | Centralizes the import, handles unavailability gracefully |
| Directory creation | Manual os.makedirs | `Path.mkdir(parents=True, exist_ok=True)` | Idempotent, cross-platform |

**Key insight:** This phase is almost entirely extraction and formalization of existing code. The `_save_results_to_lakehouse` method already does 90% of what `LakehouseStore.write()` needs. The innovation is the interface, not the implementation.

## Common Pitfalls

### Pitfall 1: Module-Level Fabric Imports
**What goes wrong:** Importing `notebookutils` or `mssparkutils` at module level in `storage.py` will fail on local development machines.
**Why it happens:** Fabric SDK is only available in Fabric runtime notebooks.
**How to avoid:** Use lazy imports inside `LakehouseStore.__init__()`, or import via `get_mssparkutils()` from `utils.py`. Follow the existing pattern in `fabric_connector.py` lines 12-29.
**Warning signs:** `ImportError` when importing `dq_framework` locally.

### Pitfall 2: Serialization Failures on Write
**What goes wrong:** `json.dumps()` fails on GX validation result objects, numpy types, or pandas Timestamps.
**Why it happens:** The validation result dict contains non-JSON-serializable types (`validation_result` key holds a GX CheckpointResult).
**How to avoid:** Always use `default=str` in `json.dumps()`. Filter out the `validation_result` key before serialization -- the existing code already does this at line 593.
**Warning signs:** `TypeError: Object of type X is not JSON serializable`.

### Pitfall 3: Forgetting to Create Directories
**What goes wrong:** `JSONFileStore.write()` fails because the results directory does not exist.
**Why it happens:** First-time use on a fresh clone or new project.
**How to avoid:** Call `Path(results_dir).mkdir(parents=True, exist_ok=True)` in `JSONFileStore.__init__()`. For Fabric, `mssparkutils.fs.put()` creates parent directories automatically.
**Warning signs:** `FileNotFoundError` on first write.

### Pitfall 4: Two Call Sites for _save_results_to_lakehouse
**What goes wrong:** Refactoring only one of the two call sites that invoke `_save_results_to_lakehouse`.
**Why it happens:** The method is called in both `validate_spark_dataframe` (line 207) and `_validate_spark_chunked` (line 287).
**How to avoid:** Search for all references before refactoring. Both calls need to be replaced with `self._store.write(key, results)`.
**Warning signs:** Inline lakehouse saves still happening for chunked validation while regular validation uses the new store.

### Pitfall 5: mssparkutils Namespace Retirement
**What goes wrong:** Code that directly imports `mssparkutils` will break when Microsoft retires the old namespace.
**Why it happens:** Microsoft renamed `mssparkutils` to `notebookutils` (official docs updated Feb 2026). The old namespace is backward compatible but will be retired in the future.
**How to avoid:** All Fabric SDK access should go through `utils.get_mssparkutils()`. The `utils.py` module already imports from `notebookutils` (line 23: `from notebookutils import mssparkutils`). This indirection insulates `storage.py` from the namespace transition.
**Warning signs:** Deprecation warnings in Fabric notebooks.

### Pitfall 6: LakehouseStore.read() Size Limit
**What goes wrong:** `mssparkutils.fs.head()` has a default max of 100KB. Large validation results (e.g., with many failed_expectations) may be truncated.
**Why it happens:** `head()` is designed for previewing files, not reading entire files.
**How to avoid:** Pass a large `maxBytes` parameter (e.g., 10MB: `head(path, 10_000_000)`). Alternatively, consider using Spark `spark.read.text()` for very large files, but this is unlikely to be needed for JSON validation results.
**Warning signs:** JSON parse errors on read due to truncated content.

## Code Examples

### JSONFileStore Implementation
```python
class JSONFileStore(ResultStore):
    """Store validation results as JSON files in a local directory."""

    def __init__(self, results_dir: str = "dq_results"):
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"JSONFileStore initialized: {self.results_dir.resolve()}")

    def write(self, key: str, data: dict[str, Any]) -> None:
        file_path = self.results_dir / f"{key}.json"
        file_path.write_text(_prepare_for_serialization(data))
        logger.info(f"Result written: {file_path}")

    def read(self, key: str) -> dict[str, Any]:
        file_path = self.results_dir / f"{key}.json"
        if not file_path.exists():
            raise FileNotFoundError(f"No result found for key: {key}")
        return json.loads(file_path.read_text())

    def list(self, prefix: Optional[str] = None) -> list[str]:
        keys = [f.stem for f in self.results_dir.glob("*.json")]
        if prefix:
            keys = [k for k in keys if k.startswith(prefix)]
        return sorted(keys)

    def delete(self, key: str) -> bool:
        file_path = self.results_dir / f"{key}.json"
        if file_path.exists():
            file_path.unlink()
            logger.info(f"Result deleted: {file_path}")
            return True
        return False
```

### LakehouseStore Implementation
```python
class LakehouseStore(ResultStore):
    """Store validation results in MS Fabric Lakehouse via notebookutils/mssparkutils."""

    def __init__(self, results_dir: str = "Files/dq_results"):
        from .utils import get_mssparkutils, FABRIC_AVAILABLE

        self._mssparkutils = get_mssparkutils()
        if not FABRIC_AVAILABLE or self._mssparkutils is None:
            raise RuntimeError(
                "LakehouseStore requires Fabric runtime with mssparkutils/notebookutils"
            )
        self.results_dir = results_dir
        logger.info(f"LakehouseStore initialized: {self.results_dir}")

    def write(self, key: str, data: dict[str, Any]) -> None:
        file_path = f"{self.results_dir}/{key}.json"
        self._mssparkutils.fs.put(file_path, _prepare_for_serialization(data), True)
        logger.info(f"Result saved to Lakehouse: {file_path}")

    def read(self, key: str) -> dict[str, Any]:
        file_path = f"{self.results_dir}/{key}.json"
        content = self._mssparkutils.fs.head(file_path, 10_000_000)
        return json.loads(content)

    def list(self, prefix: Optional[str] = None) -> list[str]:
        try:
            files = self._mssparkutils.fs.ls(self.results_dir)
        except Exception:
            return []
        keys = [
            Path(f.path).stem
            for f in files
            if f.path.endswith(".json") and not f.isDir
        ]
        if prefix:
            keys = [k for k in keys if k.startswith(prefix)]
        return sorted(keys)

    def delete(self, key: str) -> bool:
        file_path = f"{self.results_dir}/{key}.json"
        try:
            self._mssparkutils.fs.rm(file_path, False)
            logger.info(f"Result deleted from Lakehouse: {file_path}")
            return True
        except Exception:
            return False
```

### Refactoring FabricDataQualityRunner
```python
# In fabric_connector.py:

# 1. Add import at top of file
from .storage import get_store, make_result_key

# 2. In __init__, create the store (replaces results_location usage):
class FabricDataQualityRunner:
    def __init__(self, config_path, workspace_id=None, results_location=None):
        # ... existing init code ...
        self._store = get_store(results_dir=results_location)

    # 3. Replace both _save_results_to_lakehouse calls:
    def validate_spark_dataframe(self, spark_df, ...):
        # ... existing validation code ...
        # OLD: if FABRIC_UTILS_AVAILABLE: self._save_results_to_lakehouse(results)
        # NEW:
        try:
            key = make_result_key(results["batch_name"])
            self._store.write(key, results)
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
        return results

    # 4. Same replacement in _validate_spark_chunked

    # 5. Remove _save_results_to_lakehouse method entirely
```

### constants.py Addition
```python
# Add to dq_framework/constants.py:

# Default directory for local validation results storage
DEFAULT_RESULTS_DIR = "dq_results"

# Default Lakehouse path for Fabric validation results storage
DEFAULT_FABRIC_RESULTS_DIR = "Files/dq_results"
```

### __init__.py Updates
```python
# Add to dq_framework/__init__.py imports:
from .storage import ResultStore, JSONFileStore, LakehouseStore, get_store

# Add to __all__:
__all__ = [
    # ... existing exports ...
    "ResultStore",
    "JSONFileStore",
    "LakehouseStore",
    "get_store",
]
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Inline `_save_results_to_lakehouse` in class | Strategy pattern via ABC | This phase | Enables local dev, testing, future backends |
| Fabric-only result storage | Dual local/Fabric support | This phase | Local development can persist and inspect results |
| No result reading/listing/deleting | Full CRUD interface | This phase | Foundation for Phase 9 (Validation History) |
| `mssparkutils` namespace | `notebookutils` namespace (backward compat) | Feb 2025 (Microsoft) | Code should import via `utils.get_mssparkutils()` indirection |

**Deprecated/outdated:**
- `_save_results_to_lakehouse`: Will be replaced by `LakehouseStore.write()` and removed from `FabricDataQualityRunner`.
- `mssparkutils` namespace: Microsoft officially renamed to `notebookutils`. Old name works but will be retired. The codebase already uses `from notebookutils import mssparkutils` in `utils.py`.

## Open Questions

1. **Should `write()` be fire-and-forget or raise on failure?**
   - What we know: Existing `_save_results_to_lakehouse` catches all exceptions and logs errors (lines 600-602) without re-raising. The validation result is still returned to the caller regardless.
   - What's unclear: Whether callers want storage failures to halt the pipeline.
   - Recommendation: `write()` should raise by default. The caller (`FabricDataQualityRunner`) wraps in try/except with logging, matching current behavior. This gives downstream consumers (Phase 9, Phase 10) the choice. This is the standard Python approach -- don't hide errors in the interface, let callers decide.

2. **Should `results_location` parameter name change?**
   - What we know: `FabricDataQualityRunner.__init__` currently accepts `results_location` (line 66). The new `get_store` uses `results_dir`.
   - Recommendation: Keep `results_location` as the public API parameter name in `FabricDataQualityRunner` for backward compatibility. Internally pass it as `results_dir` to `get_store()`.

3. **Should the store always write, or only in Fabric?**
   - What we know: Currently, results are only saved when `FABRIC_UTILS_AVAILABLE` is True (lines 206-207, 286-287). Local runs don't persist results at all.
   - Recommendation: Always write via the store. This is one of the main benefits of the abstraction -- local development now gets result persistence via `JSONFileStore`. The `get_store()` factory ensures the right backend is used.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest >= 9.0.0 |
| Config file | `pyproject.toml` [tool.pytest.ini_options] |
| Quick run command | `pytest tests/test_storage.py -x` |
| Full suite command | `pytest --cov=dq_framework --cov-fail-under=60` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| STOR-01 | ResultStore ABC enforces interface (TypeError on instantiation of incomplete subclass); JSONFileStore CRUD works (write/read/list/delete); LakehouseStore CRUD works with mocked mssparkutils | unit | `pytest tests/test_storage.py -x` | No -- Wave 0 |
| STOR-02 | FabricDataQualityRunner uses ResultStore instead of inline `_save_results_to_lakehouse`; both call sites (validate_spark_dataframe and _validate_spark_chunked) delegate to store | unit | `pytest tests/test_fabric_connector.py -x -k store` | No -- Wave 0 |
| STOR-03 | `get_store()` returns JSONFileStore locally (when `_is_fabric_runtime()` returns False); returns LakehouseStore in Fabric (when True); respects explicit `backend` override | unit | `pytest tests/test_storage.py -x -k get_store` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_storage.py -x`
- **Per wave merge:** `pytest --cov=dq_framework --cov-fail-under=60`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_storage.py` -- covers STOR-01, STOR-03 (ResultStore ABC enforcement, JSONFileStore unit tests using tmp_path, LakehouseStore with mocked mssparkutils, get_store factory tests with patched _is_fabric_runtime)
- [ ] Update `tests/test_fabric_connector.py` -- covers STOR-02 (verify FabricDataQualityRunner delegates to ResultStore after refactor; verify `_save_results_to_lakehouse` is removed)
- [ ] Fixtures: `mock_mssparkutils` already exists in `tests/conftest.py` (line 71-78) with `fs.head`, `fs.put`, `fs.ls`, `fs.mkdirs` mocked -- reuse for LakehouseStore tests. Add `fs.rm` and `fs.exists` return values.

*(Existing test infrastructure partially covers. `mock_mssparkutils` fixture exists but needs `fs.rm` added. No `test_storage.py` exists yet.)*

## Codebase Reference Points

Key locations the planner and implementer need to know:

| What | Where | Line(s) |
|------|-------|---------|
| `_save_results_to_lakehouse` (to be extracted) | `fabric_connector.py` | 576-603 |
| Call site 1: validate_spark_dataframe | `fabric_connector.py` | 206-207 |
| Call site 2: _validate_spark_chunked | `fabric_connector.py` | 286-287 |
| `_is_fabric_runtime()` | `utils.py` | 9-16 |
| `get_mssparkutils()` | `utils.py` | 32-39 |
| `FABRIC_AVAILABLE` / `FABRIC_UTILS_AVAILABLE` | `utils.py` | 20-43 |
| `results_location` parameter | `fabric_connector.py` | 66-67 |
| `mock_mssparkutils` fixture | `tests/conftest.py` | 71-78 |
| `sample_validation_result` fixture | `tests/conftest.py` | 101-115 |
| `TestSaveResultsToLakehouse` (existing tests to update) | `tests/test_fabric_connector.py` | 652-677 |
| Current `__all__` exports | `__init__.py` | 42-55 |
| Constants module | `constants.py` | entire file |

## Sources

### Primary (HIGH confidence)
- Codebase analysis: `fabric_connector.py` -- `_save_results_to_lakehouse` implementation, both call sites, `__init__` signature
- Codebase analysis: `utils.py` -- `_is_fabric_runtime()`, `get_mssparkutils()`, `FABRIC_AVAILABLE`, import pattern from `notebookutils`
- Codebase analysis: `tests/conftest.py` -- existing `mock_mssparkutils`, `fabric_runner`, `sample_validation_result` fixtures
- Codebase analysis: `tests/test_fabric_connector.py` -- existing test groups, especially `TestSaveResultsToLakehouse`
- Python stdlib docs: `abc.ABC`, `json`, `pathlib` -- stable, well-documented
- [Microsoft Fabric NotebookUtils docs (updated Feb 2026)](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-utilities) -- confirmed `fs.put`, `fs.head`, `fs.ls`, `fs.rm`, `fs.exists`, `fs.mkdirs` APIs; confirmed mssparkutils-to-notebookutils rename with backward compatibility

### Secondary (MEDIUM confidence)
- [mssparkutils.fs API reference (GitHub)](https://github.com/MicrosoftDocs/fabric-docs/blob/main/docs/data-engineering/microsoft-spark-utilities.md) -- additional API surface documentation

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all stdlib, no new dependencies, confirmed Fabric API surface via official docs
- Architecture: HIGH -- standard Python ABC pattern, straightforward extraction of existing code with clear line references
- Pitfalls: HIGH -- based on direct analysis of existing codebase patterns, verified Fabric API behavior, and mssparkutils namespace transition (confirmed via official docs)

**Research date:** 2026-03-08
**Valid until:** 2026-04-08 (stable domain; monitor mssparkutils retirement timeline)
