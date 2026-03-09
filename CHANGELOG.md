# Changelog

All notable changes to the Fabric Data Quality Framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-03-07

### Changed — Breaking: Great Expectations 1.x Migration

#### Core Migration
- Rewrote `validator.py` for GX 1.x API (DataSources, ExpectationSuite, ValidationDefinition)
- Rewrote `fabric_connector.py` — removed SparkDFDataset, 546 lines refactored
- Minimum Python version raised from 3.8 to 3.10
- GX dependency pinned to `>=1.0.0,<2.0.0`

#### New Features
- **Storage Abstraction** — pluggable result stores (`JSONFileStore`, `LakehouseStore`) via `get_store()`
- **Alert Infrastructure** — shared alerting layer with formatting, config, dispatcher, and circuit breaker
- **Data Ingestion** — `DataIngester` class for pipeline integration

#### Tooling
- Replaced black/flake8/isort/pylint with unified ruff linting
- Added Azure DevOps CI pipeline (`COE Fabric DQ CI`, ID: 2)
- Added `.order` file for ADO wiki page ordering

#### Documentation
- Freshness audit: fixed stale versions across 9 docs
- Published docs as ADO wiki: "DQ Framework Docs"
- Consolidated redundant guides

### Results
- **Test Coverage**: 61% → 65% (213+ tests passing)
- **All tests pass** on Python 3.10+

---

## [1.2.0] - 2026-01-19

### Added
- Enhanced test coverage (~70%, 213+ tests)
- Stability improvements across all modules
- ABFSS support for loading configs from `abfss://` paths in Microsoft Fabric
- Configurable pass/fail thresholds (e.g., 95%)
- Enhanced reporting with threshold details in results

### Changed
- Interactive documentation webapp (`webapp/index.html`)

---

## [1.1.3] - 2025-12-06

### Added
- Configurable global thresholds
- ABFSS path support for config loading

---

## [1.1.0] - 2025-10-28

### Added
- MS Fabric ETL integration guides
- `FabricDataQualityRunner` for Delta table validation
- Fabric-native examples and quick start guide

---

## [1.0.0] - 2025-10-28

### Added
- Initial standalone data quality framework
- Universal data profiler (CSV, Parquet, Excel, JSON)
- YAML-based configuration system
- Batch profiling with parallel workers
- Great Expectations integration (0.x)
- Bronze/Silver/Gold layer validation templates
- CLI profiling tool (`scripts/profile_data.py`)
