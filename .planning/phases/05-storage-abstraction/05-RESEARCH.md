# Phase 5: Storage Abstraction - Research

**Researched:** 2026-03-08
**Domain:** Python abstract storage layer with local JSON and MS Fabric Lakehouse backends
**Confidence:** HIGH

## Summary

Phase 5 creates a `ResultStore` abstraction that replaces the inline `_save_results_to_lakehouse` method in `fabric_connector.py` with a pluggable storage interface. The two concrete backends are `JSONFileStore` (local development using `pathlib` and `json` stdlib) and `LakehouseStore` (Fabric production using `mssparkutils.fs`). The runtime automatically selects the backend using the existing `_is_fabric_runtime()` detection in `utils.py`.

This is a straightforward Python design pattern (Strategy via ABC) with no new external dependencies. The existing codebase already has all the primitives needed: `_is_fabric_runtime()` for environment detection, `mssparkutils.fs.put/head/ls` for Fabric I/O, and `json.dumps` for serialization. The task is to extract, formalize, and test these patterns behind a clean interface.

**Primary recommendation:** Use `abc.ABC` with four abstract methods (`write`, `read`, `list`, `delete`), implement two concrete classes in a new `dq_framework/storage.py` module, and add a `get_store()` factory function that auto-selects based on `_is_fabric_runtime()`.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| STOR-01 | Create ResultStore abstraction with JSONFileStore (local) and LakehouseStore (Fabric) backends | ABC pattern with two concrete implementations; factory function for auto-selection |
| STOR-02 | Refactor existing `_save_results_to_lakehouse` into ResultStore backend | Extract inline code from `fabric_connector.py:576-603` into `LakehouseStore.write()` |
| STOR-03 | Support both local (SQLite/JSON) and Fabric (Parquet/Lakehouse) storage modes | JSONFileStore for local (JSON files in configurable dir); LakehouseStore for Fabric (mssparkutils.fs); SQLite/Parquet are Phase 9 concerns |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| abc (stdlib) | N/A | Abstract base class for ResultStore | Built-in, zero deps, standard Python pattern |
| json (stdlib) | N/A | Serialize/deserialize validation results | Already used in `_save_results_to_lakehouse` |
| pathlib (stdlib) | N/A | Local file path operations | Already used throughout codebase |
| logging (stdlib) | N/A | Structured logging | Already used throughout codebase |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| mssparkutils | Fabric-provided | Lakehouse file I/O (put, head, ls, rm) | Only in Fabric runtime; accessed via `get_mssparkutils()` |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| abc.ABC | Protocol (typing) | Protocol is structural typing; ABC is more explicit and provides `register()` for third-party backends. ABC is better here because we want enforcement, not duck typing |
| json files | SQLite | SQLite adds query capability but is Phase 9 scope (HIST-01). Phase 5 keeps it simple with JSON |
| Custom factory | dependency injection framework | Over-engineering for two backends; simple factory function is sufficient |

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
    __init__.py          # MODIFIED: export ResultStore, JSONFileStore, LakehouseStore
```

### Pattern 1: Strategy via Abstract Base Class
**What:** Define a `ResultStore` ABC with `write`, `read`, `list`, `delete` methods. Each backend implements all four.
**When to use:** When you have multiple interchangeable implementations of the same interface and want compile-time enforcement.
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
    """Abstract interface for validation result persistence."""

    @abstractmethod
    def write(self, key: str, data: dict[str, Any]) -> None:
        """Write a validation result.

        Args:
            key: Unique identifier (e.g., "validation_mydata_20260308_120000")
            data: Validation result dictionary
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
            List of result keys
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
    """
    from .utils import _is_fabric_runtime

    if backend == "local" or (backend is None and not _is_fabric_runtime()):
        return JSONFileStore(results_dir=results_dir or "dq_results")
    else:
        return LakehouseStore(results_dir=results_dir or "Files/dq_results")
```

### Pattern 3: Key Naming Convention
**What:** Consistent key format for stored results: `{batch_name}_{timestamp}`
**When to use:** All write operations should produce predictable, sortable keys.
**Example:**
```python
def _make_key(batch_name: str) -> str:
    """Generate a storage key from batch name and current time."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Sanitize batch_name: replace non-alphanumeric with underscore
    safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in batch_name)
    return f"validation_{safe_name}_{timestamp}"
```

### Anti-Patterns to Avoid
- **Leaking backend details:** The `FabricDataQualityRunner` should not reference `mssparkutils` for result storage after refactor. All storage goes through `ResultStore`.
- **Catching all exceptions silently:** The existing `_save_results_to_lakehouse` logs errors but swallows them. The new interface should propagate errors (callers can catch if needed).
- **Storing non-serializable data:** The existing code already filters out `validation_result` (GX object). The `write` method should handle serialization defensively with `default=str`.
- **Hardcoded paths:** Use configurable `results_dir` parameter, not hardcoded `"Files/dq_results"` (though that is the default for Fabric).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Abstract class enforcement | Manual isinstance checks | `abc.ABC` + `@abstractmethod` | ABC raises TypeError on instantiation if methods missing |
| JSON serialization of datetimes | Custom serializer | `json.dumps(data, default=str, indent=2)` | Already used in existing code; handles datetime, Path, etc. |
| Path manipulation | String concatenation | `pathlib.Path` | Cross-platform, already used in codebase |
| Fabric runtime detection | New detection logic | `utils._is_fabric_runtime()` | Already exists and tested |

**Key insight:** This phase is almost entirely extraction and formalization of existing code. The `_save_results_to_lakehouse` method already does 90% of what `LakehouseStore.write()` needs. The innovation is the interface, not the implementation.

## Common Pitfalls

### Pitfall 1: Module-Level Fabric Imports
**What goes wrong:** Importing `mssparkutils` at module level in `storage.py` will fail on local development machines.
**Why it happens:** Fabric SDK is only available in Fabric runtime.
**How to avoid:** Use lazy imports inside `LakehouseStore` methods, or import via `get_mssparkutils()` from `utils.py`. Follow the existing pattern in `fabric_connector.py`.
**Warning signs:** `ImportError` when importing `dq_framework` locally.

### Pitfall 2: Serialization Failures on Write
**What goes wrong:** `json.dumps()` fails on GX validation result objects, numpy types, or pandas Timestamps.
**Why it happens:** The validation result dict contains non-JSON-serializable types.
**How to avoid:** Always use `default=str` in `json.dumps()`. Filter out the `validation_result` key (GX object) before serialization -- the existing code already does this.
**Warning signs:** `TypeError: Object of type X is not JSON serializable`.

### Pitfall 3: Race Conditions on Key Collision
**What goes wrong:** Two validations in the same second produce the same key and overwrite each other.
**Why it happens:** Timestamp-only keys have 1-second resolution.
**How to avoid:** Include a short UUID suffix in keys (e.g., `uuid.uuid4().hex[:8]`), or accept that same-second overwrites are acceptable for this use case (they are -- validation runs are not sub-second).
**Warning signs:** Missing historical results when running batch validations.

### Pitfall 4: Forgetting to Create Directories
**What goes wrong:** `JSONFileStore.write()` fails because the results directory does not exist.
**Why it happens:** First-time use on a fresh clone.
**How to avoid:** Call `Path(results_dir).mkdir(parents=True, exist_ok=True)` in `JSONFileStore.__init__()`.
**Warning signs:** `FileNotFoundError` on first write.

### Pitfall 5: LakehouseStore.delete() May Not Exist
**What goes wrong:** `mssparkutils.fs` may not have a `rm` or `delete` method in all Fabric SDK versions.
**Why it happens:** Fabric SDK surface area varies.
**How to avoid:** Use `mssparkutils.fs.rm(path, recurse=False)` which is the documented method. Wrap in try/except and log warning if unavailable.
**Warning signs:** `AttributeError` on delete operations in Fabric.

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
        serializable = {k: v for k, v in data.items() if k != "validation_result"}
        file_path = self.results_dir / f"{key}.json"
        file_path.write_text(json.dumps(serializable, indent=2, default=str))
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
    """Store validation results in MS Fabric Lakehouse."""

    def __init__(self, results_dir: str = "Files/dq_results"):
        from .utils import get_mssparkutils, FABRIC_AVAILABLE

        self._mssparkutils = get_mssparkutils()
        if not FABRIC_AVAILABLE or self._mssparkutils is None:
            raise RuntimeError(
                "LakehouseStore requires Fabric runtime with mssparkutils"
            )
        self.results_dir = results_dir
        logger.info(f"LakehouseStore initialized: {self.results_dir}")

    def write(self, key: str, data: dict[str, Any]) -> None:
        serializable = {k: v for k, v in data.items() if k != "validation_result"}
        file_path = f"{self.results_dir}/{key}.json"
        results_json = json.dumps(serializable, indent=2, default=str)
        self._mssparkutils.fs.put(file_path, results_json, overwrite=True)
        logger.info(f"Result saved to Lakehouse: {file_path}")

    def read(self, key: str) -> dict[str, Any]:
        file_path = f"{self.results_dir}/{key}.json"
        content = self._mssparkutils.fs.head(file_path, 10_000_000)
        return json.loads(content)

    def list(self, prefix: Optional[str] = None) -> list[str]:
        files = self._mssparkutils.fs.ls(self.results_dir)
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
            self._mssparkutils.fs.rm(file_path, recurse=False)
            logger.info(f"Result deleted from Lakehouse: {file_path}")
            return True
        except Exception:
            return False
```

### Refactoring FabricDataQualityRunner
```python
# In fabric_connector.py -- replace _save_results_to_lakehouse usage:

# Old (in validate_spark_dataframe):
#   if FABRIC_UTILS_AVAILABLE:
#       self._save_results_to_lakehouse(results)

# New:
from .storage import get_store

class FabricDataQualityRunner:
    def __init__(self, config_path, workspace_id=None, results_location=None):
        # ... existing init ...
        self._store = get_store(results_dir=results_location)

    def validate_spark_dataframe(self, spark_df, ...):
        # ... existing validation ...
        # Replace inline save:
        key = f"validation_{results['batch_name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self._store.write(key, results)
        return results
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Inline `_save_results_to_lakehouse` in class | Strategy pattern via ABC | This phase | Enables local dev, testing, future backends |
| Fabric-only result storage | Dual local/Fabric support | This phase | Local development can persist and inspect results |
| No result reading/listing | Full CRUD interface | This phase | Foundation for Phase 9 (Validation History) |

**Deprecated/outdated:**
- `_save_results_to_lakehouse`: Will be replaced by `LakehouseStore.write()`. The method should be removed from `FabricDataQualityRunner` after migration.

## Open Questions

1. **Should `write()` be fire-and-forget or raise on failure?**
   - What we know: Existing `_save_results_to_lakehouse` catches all exceptions and logs errors without re-raising
   - What's unclear: Whether callers want storage failures to halt the pipeline
   - Recommendation: `write()` should raise by default. The caller (FabricDataQualityRunner) can wrap in try/except with logging, matching current behavior. This gives downstream consumers (Phase 9, Phase 10) the choice.

2. **Should STOR-03 include SQLite/Parquet in this phase?**
   - What we know: STOR-03 says "Support both local (SQLite/JSON) and Fabric (Parquet/Lakehouse) storage modes"
   - What's unclear: Whether SQLite and Parquet are Phase 5 or Phase 9 (HIST-01 explicitly mentions SQLite/Parquet)
   - Recommendation: Phase 5 implements JSON (local) and JSON-via-mssparkutils (Fabric) only. The `ResultStore` ABC is designed so that Phase 9 can add `SQLiteStore` and `ParquetStore` backends without changing the interface. This avoids scope creep -- STOR-03 is satisfied by the interface supporting both modes, not by implementing all formats now.

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
| STOR-01 | ResultStore ABC enforces interface; JSONFileStore CRUD works; LakehouseStore CRUD works | unit | `pytest tests/test_storage.py -x` | No -- Wave 0 |
| STOR-02 | FabricDataQualityRunner uses ResultStore instead of inline method | unit | `pytest tests/test_fabric_connector.py -x -k store` | No -- Wave 0 |
| STOR-03 | get_store() returns JSONFileStore locally, LakehouseStore in Fabric | unit | `pytest tests/test_storage.py -x -k get_store` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_storage.py -x`
- **Per wave merge:** `pytest --cov=dq_framework --cov-fail-under=60`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_storage.py` -- covers STOR-01, STOR-03 (JSONFileStore unit tests, LakehouseStore with mocked mssparkutils, get_store factory tests)
- [ ] Update `tests/test_fabric_connector.py` -- covers STOR-02 (verify FabricDataQualityRunner delegates to ResultStore)
- [ ] Fixtures: `mock_mssparkutils` already exists in `conftest.py` -- reuse for LakehouseStore tests

## Sources

### Primary (HIGH confidence)
- Codebase analysis: `fabric_connector.py` lines 576-603 (`_save_results_to_lakehouse` implementation)
- Codebase analysis: `utils.py` (`_is_fabric_runtime()`, `get_mssparkutils()`, `FABRIC_AVAILABLE`)
- Codebase analysis: `conftest.py` (existing `mock_mssparkutils` fixture)
- Python stdlib docs: `abc.ABC`, `json`, `pathlib` -- stable, well-documented

### Secondary (MEDIUM confidence)
- Microsoft Fabric `mssparkutils.fs` API: `put`, `head`, `ls`, `rm` methods -- based on existing usage in codebase (not independently verified against current Fabric SDK docs)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all stdlib, no new dependencies
- Architecture: HIGH - standard Python ABC pattern, straightforward extraction of existing code
- Pitfalls: HIGH - based on direct analysis of existing codebase patterns and known Python gotchas

**Research date:** 2026-03-08
**Valid until:** 2026-04-08 (stable domain, no external dependency changes expected)
