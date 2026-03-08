# Phase 3: Bug Fixes - Research

**Researched:** 2026-03-08
**Domain:** PySpark chunked validation, Python API hygiene, dead code removal
**Confidence:** HIGH

## Summary

Phase 3 addresses five known bugs across three domains: Spark chunked validation (BUG-01, BUG-02), dead code cleanup (BUG-03, BUG-04), and public API surface correction (BUG-05). All bugs are clearly scoped with well-defined fixes. The most technically complex work is BUG-01 (replacing `monotonically_increasing_id` with correct row numbering) and BUG-02 (changing aggregation from summed totals to per-expectation averages).

The codebase is well-structured with existing test infrastructure (pytest with Spark mocking patterns via `unittest.mock`). The validator returns a standard result dict with keys `success`, `success_rate`, `evaluated_checks`, `successful_checks`, `failed_checks`, `failed_expectations`, `threshold`, `severity_stats`, etc. The chunked aggregation must produce a compatible dict shape while adding the new `chunks` key for per-chunk breakdowns.

**Primary recommendation:** Fix BUG-01 and BUG-02 together (they share the same file and method chain), then handle the three simpler cleanup bugs (BUG-03, BUG-04, BUG-05) as a separate batch.

<user_constraints>

## User Constraints (from CONTEXT.md)

### Locked Decisions
- **check_data.py (BUG-04):** Delete entirely -- do not relocate to scripts/. It's a one-off exploration script with hardcoded paths, same "clean delete" pattern as setup.py in Phase 1. Only remove check_data.py -- other root-level files are intentional.
- **DataIngester.engine (BUG-03):** Remove the engine parameter entirely -- no deprecation warning. Full cleanup: remove param, update docstring, remove any "reserved for future use" mentions. No trace of the dead code should remain.
- **_is_fabric_runtime in __all__ (BUG-05):** Remove _is_fabric_runtime from __init__.py __all__ -- it stays in utils.py for internal use. Audit __all__ fully for any other private symbols accidentally exported and remove those too.
- **Chunked Spark validation (BUG-01 + BUG-02):** Fix both bugs properly -- do not deprecate or remove chunking. BUG-01: Replace monotonically_increasing_id with a correct row numbering approach (zipWithIndex or row_number()). BUG-02: Aggregated results should report per-expectation averages across chunks (mean success rate), not summed totals. "success" = average rate meets the existing threshold. Include per-chunk detail in the result dict (a 'chunks' key with per-chunk breakdowns) alongside the aggregated summary.

### Claude's Discretion
- Exact replacement for monotonically_increasing_id (zipWithIndex vs row_number() vs other)
- Internal structure of the per-chunk detail output
- Test approach for Spark-dependent code (mock strategy, fixtures)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.

</user_constraints>

<phase_requirements>

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| BUG-01 | Fix chunked Spark validation bug (monotonically_increasing_id misuse causes missed/inconsistent rows) | Row numbering alternatives analysis; `_validate_spark_chunked` code review; Spark window function patterns |
| BUG-02 | Fix aggregated chunk results miscounting (inflated statistics from counting expectations across all chunks) | `_aggregate_chunk_results` code review; per-expectation averaging strategy; result dict shape analysis |
| BUG-03 | Remove unused DataIngester.engine parameter (dead code misleading users) | `ingestion.py` and `test_ingestion.py` full review; downstream impact analysis |
| BUG-04 | Remove stale check_data.py script with hardcoded paths | File confirmed as one-off exploration script; clean delete |
| BUG-05 | Fix _is_fabric_runtime private function exposed in __init__.py __all__ | `__init__.py` audit; public API surface analysis |

</phase_requirements>

## Architecture Patterns

### Bug Fix Approach: BUG-01 (monotonically_increasing_id replacement)

**The problem:** `monotonically_increasing_id()` generates IDs that are unique and monotonically increasing but NOT consecutive. With partitioned data the IDs have gaps (upper bits encode partition ID). The current code filters with `>= offset` and `< offset + chunk_size` assuming consecutive IDs, which misses rows or produces empty chunks.

**Recommendation: Use `row_number()` with a Window function.**

```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, lit

# Create a deterministic row number (1-based)
window = Window.orderBy(lit(1))  # stable ordering for chunking
spark_df_with_id = spark_df.withColumn(
    "__chunk_row_num__", row_number().over(window)
)

# Chunk filtering becomes correct:
chunk_df = spark_df_with_id.filter(
    (spark_df_with_id["__chunk_row_num__"] > offset)
    & (spark_df_with_id["__chunk_row_num__"] <= offset + chunk_size)
).drop("__chunk_row_num__")
```

**Why row_number() over zipWithIndex:**
- `row_number()` stays in the DataFrame API (no RDD conversion overhead)
- `zipWithIndex()` requires converting to RDD and back, which is expensive and loses schema
- `row_number()` produces consecutive 1-based integers -- exactly what chunk filtering needs
- The `orderBy(lit(1))` preserves existing row order without requiring a sort column

**Caveat:** `row_number()` over `Window.orderBy(lit(1))` is non-deterministic across runs if the DataFrame has no natural ordering. This is acceptable because chunking is a memory optimization, not a correctness guarantee on row assignment. The requirement is that every row appears in exactly one chunk per run -- which `row_number()` guarantees.

**Alternative considered:** `zipWithIndex` via RDD. Would work but requires `spark_df.rdd.zipWithIndex().toDF()` which is slower and loses column types. Not recommended.

### Bug Fix Approach: BUG-02 (aggregation strategy)

**The problem:** Current `_aggregate_chunk_results` sums `evaluated_checks`, `successful_checks`, and `failed_checks` across chunks. Since each chunk runs the same N expectations, the totals are inflated by num_chunks. For example, 3 chunks with 10 expectations each reports 30 evaluated checks instead of 10.

**Correct approach: Per-expectation averaging.**

The validator returns results per run with a fixed set of expectations. Each chunk produces its own `success_rate`. The aggregated result should:
1. Compute mean `success_rate` across chunks
2. Report `evaluated_checks` as the number of unique expectations (from any single chunk, since all chunks run the same suite)
3. Determine `success` by comparing mean success_rate against the threshold
4. Include a `chunks` key with per-chunk breakdowns

```python
def _aggregate_chunk_results(self, chunk_results, batch_name):
    valid_results = [r for r in chunk_results if "error" not in r]
    error_results = [r for r in chunk_results if "error" in r]

    if not valid_results:
        return {
            "success": False,
            "error": "All chunks failed",
            "chunk_errors": error_results,
        }

    # Per-expectation average success rate
    avg_success_rate = sum(r["success_rate"] for r in valid_results) / len(valid_results)

    # Expectations count from a single chunk (all chunks run same suite)
    evaluated = valid_results[0].get("evaluated_checks", 0)
    threshold = valid_results[0].get("threshold", DEFAULT_VALIDATION_THRESHOLD)

    return {
        "success": avg_success_rate >= threshold,
        "batch_name": batch_name or f"spark_df_{...}",
        "suite_name": valid_results[0].get("suite_name", "unknown"),
        "timestamp": datetime.now().isoformat(),
        "evaluated_checks": evaluated,
        "success_rate": avg_success_rate,
        "threshold": threshold,
        "failed_expectations": [...],  # deduplicated across chunks
        "chunked_processing": True,
        "num_chunks": len(chunk_results),
        "chunks": [
            {
                "chunk_index": i,
                "success": r["success"],
                "success_rate": r["success_rate"],
                "evaluated_checks": r.get("evaluated_checks", 0),
                "failed_checks": r.get("failed_checks", 0),
                "failed_expectations": r.get("failed_expectations", []),
            }
            for i, r in enumerate(valid_results)
        ],
        "chunk_errors": error_results if error_results else None,
    }
```

**Key design decisions:**
- `successful_checks` and `failed_checks` are removed from the top-level aggregated result because they are meaningless as averages. They remain in per-chunk details.
- `failed_expectations` at top level should collect all unique failures across chunks (union, not intersection) limited to a display cap.
- `success` is determined by comparing `avg_success_rate` against the threshold from the validator config.

### Bug Fix Approach: BUG-03 (DataIngester.engine removal)

**Current state:** `DataIngester.__init__` accepts `engine` param, stores it as `self.engine`, but never uses it anywhere. The `ingest_file` method uses `pd.read_parquet`/`to_parquet` without passing an engine argument, and local mode uses `shutil.copy2`.

**Fix:**
1. Remove `engine` parameter from `__init__`
2. Remove `self.engine = engine` assignment
3. Update class docstring to remove engine references
4. Update `__init__` docstring to remove engine parameter
5. Update tests in `test_ingestion.py`: remove all engine-related tests (`TestDataIngesterInitialization` tests for engine, `TestDataIngesterWithDifferentEngines` class)

**Impact:** Minor breaking change for any caller passing `engine=`. CONTEXT.md confirms this is accepted since the param was never functional.

### Bug Fix Approach: BUG-04 (check_data.py deletion)

**Current state:** Root-level `check_data.py` with hardcoded path `/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/AIMS_LOCAL/data/...`. Uses `DataLoader` but is a one-off exploration script.

**Fix:** Delete the file. No relocation. Same pattern as `setup.py` removal in Phase 1.

### Bug Fix Approach: BUG-05 (__all__ audit)

**Current state in `__init__.py`:**
```python
__all__ = [
    "DataQualityValidator",
    "FabricDataQualityRunner",
    "ConfigLoader",
    "DataProfiler",
    "BatchProfiler",
    "DataLoader",
    "DataIngester",
    "FileSystemHandler",
    # Fabric detection utilities
    "FABRIC_AVAILABLE",
    "FABRIC_UTILS_AVAILABLE",
    "_is_fabric_runtime",
    "get_mssparkutils",
]
```

**Issues found:**
1. `_is_fabric_runtime` -- private function (underscore prefix), should not be in `__all__`
2. `get_mssparkutils` -- this is a public utility function, keep it
3. `FABRIC_AVAILABLE` and `FABRIC_UTILS_AVAILABLE` -- public constants, keep
4. `FileSystemHandler` -- need to verify this exists in utils.py (it is imported from `.utils` at line 36)

**Fix:** Remove `_is_fabric_runtime` from `__all__`. Keep the import statement (it is used internally by `fabric_connector.py`). Remove the import line from `__init__.py` only if nothing else in the package relies on it being importable via `from dq_framework import _is_fabric_runtime` -- but since it is private, removing from `__all__` is sufficient. The import can stay for internal use.

**Audit result:** No other private symbols found in `__all__`. Only `_is_fabric_runtime` needs removal.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Consecutive row IDs in Spark | Custom RDD-based indexing | `pyspark.sql.functions.row_number()` with Window | Built-in, stays in DataFrame API, handles partitioning correctly |
| Averaging across chunks | Manual weighted average logic | Simple arithmetic mean of `success_rate` values | All chunks run same expectations, so each chunk's rate is equally weighted |

## Common Pitfalls

### Pitfall 1: row_number() requires ORDER BY
**What goes wrong:** `Window.orderBy()` is required -- Spark will error without it.
**Why it happens:** `row_number()` is a ranking function that requires a deterministic ordering specification.
**How to avoid:** Use `Window.orderBy(lit(1))` or `Window.orderBy(monotonically_increasing_id())` as a stable no-op ordering. The former is simpler.
**Warning signs:** AnalysisException mentioning "window function requires ORDER BY".

### Pitfall 2: row_number() is 1-based, not 0-based
**What goes wrong:** Off-by-one errors in chunk boundary filtering.
**Why it happens:** Unlike Python indexing, `row_number()` starts at 1.
**How to avoid:** Use `> offset` and `<= offset + chunk_size` instead of `>= offset` and `< offset + chunk_size`, OR adjust offset arithmetic to be 1-based.

### Pitfall 3: Test breakage from engine removal
**What goes wrong:** Existing tests in `test_ingestion.py` explicitly test engine parameter behavior (8+ tests).
**Why it happens:** Tests were written for the current API including the dead `engine` param.
**How to avoid:** Remove or rewrite the `TestDataIngesterInitialization` and `TestDataIngesterWithDifferentEngines` test classes entirely. The `DataIngester()` constructor calls throughout other test classes will continue to work since `engine` had a default value -- removing the param means the no-arg constructor still works.

### Pitfall 4: Aggregated result dict shape change
**What goes wrong:** Downstream code (e.g., `handle_failure`, `_save_results_to_lakehouse`) expects certain keys in the result dict.
**Why it happens:** Changing aggregation changes the output shape.
**How to avoid:** Ensure the aggregated result retains all keys that `handle_failure` reads: `success`, `suite_name`, `batch_name`, `failed_checks`, `evaluated_checks`, `success_rate`, `failed_expectations`. The new `chunks` key is additive and won't break existing consumers.

### Pitfall 5: Import still needed after __all__ removal
**What goes wrong:** Removing `_is_fabric_runtime` from the import block in `__init__.py` breaks `fabric_connector.py` which imports it from `.utils` directly.
**Why it happens:** Confusion between `__all__` (star-import surface) and actual imports.
**How to avoid:** Only remove from `__all__` list. Keep the import statement. `fabric_connector.py` imports from `.utils` directly, not via `__init__.py`, so it is unaffected either way.

## Code Examples

### Correct chunk filtering with row_number()
```python
# Source: PySpark documentation - Window functions
from pyspark.sql import Window
from pyspark.sql.functions import row_number, lit

window = Window.orderBy(lit(1))
spark_df_with_id = spark_df.withColumn("__row_num__", row_number().over(window))

# For chunk_idx 0: rows 1..chunk_size
# For chunk_idx 1: rows chunk_size+1..2*chunk_size
# etc.
lower = chunk_idx * chunk_size + 1
upper = (chunk_idx + 1) * chunk_size
chunk_df = spark_df_with_id.filter(
    (spark_df_with_id["__row_num__"] >= lower)
    & (spark_df_with_id["__row_num__"] <= upper)
).drop("__row_num__")
```

### DataIngester after engine removal
```python
class DataIngester:
    """Handles data ingestion operations."""

    def __init__(self):
        """Initialize DataIngester."""
        pass  # No parameters needed

    def ingest_file(self, source_path: Path, target_path: Path, is_fabric: bool = False) -> bool:
        # ... unchanged ...
```

### Mocking Spark for chunked validation tests
```python
# Source: existing test patterns in tests/test_fabric_connector.py
from unittest.mock import MagicMock, patch, PropertyMock
import pandas as pd

def make_mock_spark_df(data, columns):
    """Create a mock Spark DataFrame that supports toPandas() and count()."""
    mock_df = MagicMock()
    mock_df.count.return_value = len(data)
    mock_df.columns = columns

    pdf = pd.DataFrame(data, columns=columns)
    mock_df.toPandas.return_value = pdf

    # Support withColumn, filter, drop chain
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.drop.return_value = mock_df

    return mock_df
```

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest >= 7.0 |
| Config file | pyproject.toml [tool.pytest.ini_options] |
| Quick run command | `python -m pytest tests/test_fabric_connector.py tests/test_ingestion.py -x -q` |
| Full suite command | `python -m pytest tests/ -x` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| BUG-01 | Chunked validation produces correct row-level results | unit (mocked Spark) | `python -m pytest tests/test_fabric_connector.py -x -k "chunk"` | Needs new tests |
| BUG-02 | Aggregated results report correct counts (averages) | unit | `python -m pytest tests/test_fabric_connector.py -x -k "aggregate"` | Needs new tests |
| BUG-03 | DataIngester has no engine parameter | unit | `python -m pytest tests/test_ingestion.py -x` | Existing tests need updating |
| BUG-04 | check_data.py is removed | smoke | `test ! -f check_data.py` | No test file needed |
| BUG-05 | __all__ contains only public symbols | unit | `python -m pytest tests/test_fabric_connector.py -x -k "all"` | Needs new test |

### Sampling Rate
- **Per task commit:** `python -m pytest tests/test_fabric_connector.py tests/test_ingestion.py -x -q`
- **Per wave merge:** `python -m pytest tests/ -x`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_fabric_connector.py` -- needs chunked validation tests (BUG-01), aggregation tests (BUG-02), __all__ audit test (BUG-05)
- [ ] `tests/test_ingestion.py` -- needs engine-related tests removed/updated (BUG-03)
- [ ] No new framework install needed -- pytest already configured

## Open Questions

1. **Window ordering for row_number()**
   - What we know: `Window.orderBy(lit(1))` works but Spark may warn about non-deterministic ordering in some versions
   - What's unclear: Whether the Fabric Spark version in production issues warnings for this pattern
   - Recommendation: Use `lit(1)` as order-by; add a comment explaining this is intentional for chunking. If warnings appear, switch to `monotonically_increasing_id()` as the order-by expression (ironic but correct when used only for ordering, not for filtering by value).

2. **Deduplication of failed_expectations across chunks**
   - What we know: The same expectation can fail in multiple chunks, producing duplicate entries
   - What's unclear: Whether users want to see per-chunk failure details or deduplicated failures at top level
   - Recommendation: Top-level `failed_expectations` should be the union (deduplicated by expectation type + column), capped at the existing `MAX_FAILURE_DISPLAY` constant. Per-chunk details in the `chunks` key retain all failures.

## Sources

### Primary (HIGH confidence)
- Direct code review of `dq_framework/fabric_connector.py` lines 242-325 -- chunked validation and aggregation logic
- Direct code review of `dq_framework/ingestion.py` -- DataIngester class with unused engine param
- Direct code review of `dq_framework/__init__.py` -- __all__ list with _is_fabric_runtime
- Direct code review of `check_data.py` -- confirmed one-off script with hardcoded paths
- Direct code review of `dq_framework/validator.py` lines 285-306 -- result dict shape
- Direct code review of `tests/test_ingestion.py` -- existing engine-related tests to update
- Direct code review of `tests/test_fabric_connector.py` -- existing test patterns (sparse, mostly Fabric-gated)

### Secondary (MEDIUM confidence)
- PySpark `monotonically_increasing_id` documentation -- confirms non-consecutive IDs with partition-based upper bits
- PySpark `row_number()` Window function -- confirmed 1-based consecutive numbering

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new libraries, all fixes within existing codebase
- Architecture: HIGH -- all target files reviewed, result dict shape verified
- Pitfalls: HIGH -- identified from direct code analysis and established Spark patterns

**Research date:** 2026-03-08
**Valid until:** 2026-04-08 (stable codebase, no external dependency changes)
