# Phase 3: Bug Fixes - Context

**Gathered:** 2026-03-08
**Status:** Ready for planning

<domain>
## Phase Boundary

Resolve all 5 known bugs in validation (BUG-01, BUG-02), ingestion (BUG-03), stale scripts (BUG-04), and public API (BUG-05). No new features — fix what's broken and clean up dead code.

</domain>

<decisions>
## Implementation Decisions

### check_data.py (BUG-04)
- Delete entirely — do not relocate to scripts/
- It's a one-off exploration script with hardcoded paths, same "clean delete" pattern as setup.py in Phase 1
- Only remove check_data.py — other root-level files are intentional

### DataIngester.engine (BUG-03)
- Remove the engine parameter entirely — no deprecation warning
- Full cleanup: remove param, update docstring, remove any "reserved for future use" mentions
- No trace of the dead code should remain

### _is_fabric_runtime in __all__ (BUG-05)
- Remove _is_fabric_runtime from __init__.py __all__ — it stays in utils.py for internal use
- Audit __all__ fully for any other private symbols accidentally exported and remove those too

### Chunked Spark validation (BUG-01 + BUG-02)
- Fix both bugs properly — do not deprecate or remove chunking
- BUG-01: Replace monotonically_increasing_id with a correct row numbering approach (zipWithIndex or row_number())
- BUG-02: Aggregated results should report per-expectation averages across chunks (mean success rate), not summed totals
- "success" = average rate meets the existing threshold
- Include per-chunk detail in the result dict (a 'chunks' key with per-chunk breakdowns) alongside the aggregated summary

### Claude's Discretion
- Exact replacement for monotonically_increasing_id (zipWithIndex vs row_number() vs other)
- Internal structure of the per-chunk detail output
- Test approach for Spark-dependent code (mock strategy, fixtures)

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `dq_framework/fabric_connector.py` lines 243-253: Current chunked validation with monotonically_increasing_id — the code to fix for BUG-01
- `dq_framework/fabric_connector.py` lines 287-325: `_aggregate_chunk_results` — the aggregation logic to fix for BUG-02
- `dq_framework/ingestion.py`: DataIngester class with unused engine parameter — target for BUG-03
- `dq_framework/__init__.py` line 38: __all__ with _is_fabric_runtime — target for BUG-05
- `check_data.py`: Root-level script to delete for BUG-04

### Established Patterns
- Phase 1 set precedent: delete dead code cleanly (setup.py removal)
- Tests in `tests/test_fabric_connector.py` mock Spark/Fabric dependencies since they can't run locally
- Validation results are returned as dicts with standard keys (success, success_rate, total_evaluated, etc.)

### Integration Points
- DataIngester is imported by external code (AIMS) — engine param removal is a minor breaking change but param was never functional
- __init__.py __all__ defines the public API surface — changes here affect star imports
- Chunked validation is used by FabricDataQualityRunner — fix must preserve the same call signature

</code_context>

<specifics>
## Specific Ideas

- Aggregated chunk results should include a 'chunks' key with per-chunk breakdowns for debugging where quality degrades
- Average success rate across chunks (not worst-chunk) is the aggregation strategy
- Full audit of __all__ for private symbol leaks, not just _is_fabric_runtime

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 03-bug-fixes*
*Context gathered: 2026-03-08*
