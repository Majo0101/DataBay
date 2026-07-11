# Changelog

All notable changes to DataBay are documented in this file.

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
