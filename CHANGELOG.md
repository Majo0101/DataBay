# Changelog

All notable changes to DataBay are documented in this file.

## [0.3.1] - 2026-09-08

### Added

- Selectable Delta/Iceberg output in `feed_spark()` through `table_format`.
- Configurable JDBC write partition cap, defaulting to 4 per table.
- Bounded Pandas conversion in SQL magic with named variables and truncation warnings.
- Optional Spark tuning overrides and startup configuration validation for Delta and Iceberg.

### Changed

- Skip distinct counts in `select_informative_columns()` when the threshold is 1 and no report is requested.
- Declare Spark Connect dependencies and align PySpark with the Docker runtime at 4.0.1.
- Clarify function usage and Pylance documentation.

### Fixed

- Return typed empty reports from `null_rate()` and `find_key_set()` when appropriate.
- Apply JDBC read schema overrides through `customSchema`.
- Remove forced broadcast from `find_key_set()` and escape literal column names in core analysis.
- Document Compose recreation after environment changes and remove unused local-mode executor sizing.

## [0.3.0] - 2026-07-12

### Added

- Added `select_informative_columns()` for removing empty, sparse, or constant Spark DataFrame columns while preserving selected keys and optionally returning a decision report.
- Added repository-local CSV fixtures and portable Delta Lake and Iceberg mounts.
- Added local fast, integration, and slow test modes.
- Added architecture, motivation, and design-limit documentation.

### Changed

- Standardized package and runtime version metadata on 0.3.0.
- Made Docker bind mounts opt-in instead of mounting a Windows drive by default.

## [0.2.0]

### Changed

- Removed the unused Plotly reporting subsystem and its dependencies.
- Focused the public API on Spark quality, comparison, ETL, and runtime tooling.

## [0.1.0]

### Added

- Initial DataBay release with Spark dataset comparison, data-quality checks, JDBC ETL, Docker runtime management, and lakehouse development environments.
