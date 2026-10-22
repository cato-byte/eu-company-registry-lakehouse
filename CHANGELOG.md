# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). A release is published by pushing a
`v<version>` tag; see [docs/deployment.md](docs/deployment.md#releases).

## [Unreleased]

## [1.0.0] - 2026-10-22

First tagged release: the pipeline as it runs in production.

### Pipeline

- Six national company registers (France, United Kingdom, Ireland, Norway, Estonia, Navarra)
  ingested into a bronze/silver/gold lakehouse on Databricks serverless (AWS) with Delta Lake.
- Silver: explicit per-source parsing, SCD Type 2 history via `MERGE`, pseudonymised personal
  identifiers, and quarantine of malformed source lines with a 1% abort threshold ([#6]).
- Data quality: single-pass rule engine with quarantine, metrics and per-rule thresholds.
- Geocoding: tiered and free of charge (register coordinates, postcode, district, town), plus a
  daily Lambda worker on free government geocoders ([#3]).
- Gold: Kimball star schema and insight tables (operating vs registered companies, demography,
  cohort survival, fishing hubs), with a schema diff and alert before each overwrite ([#6]).
- Publish: static JSON/GeoJSON for a map site with no runtime access to AWS or Databricks.
- Delta maintenance delegated to predictive optimization; Change Data Feed on `silver.company` ([#6]).

### Operations

- One Lakeflow Job running a Python wheel on serverless, defined as a Databricks Asset Bundle.
- Change gate: unchanged inputs end the run after one small task, and only registers with a new
  snapshot are reprocessed; monthly schedule ([#4]).
- Recovery without automatic retries: a failure alerts, and the next run resumes from the stage
  that failed ([#7]).
- Timeouts, duration health rules, e-mail and webhook alerts, schema-drift detection and a volume
  anomaly guard.
- DBU/USD cost estimator with prior coefficients, and a per-run cost report from `system.billing`.

### Delivery

- CI: ruff, a PySpark anti-pattern linter, a sensitive-data scan, gitleaks, strict bundle
  validation, 220+ unit tests and an end-to-end run on synthetic fixtures.
- CD: AWS ingestion via SAM with GitHub OIDC (no stored AWS keys), and the Databricks job deployed
  by a service principal ([#1], [#2]).
- Releases: tagging `v<version>` builds the wheel, checks it against the source version, and
  publishes a GitHub release with the wheel, its SHA-256 checksum and these notes.

### Fixed

- Concurrent table creation by parallel tasks, the Navarra 2020 establishment key, and table
  history reads on serverless, all found in the first production run ([#4]).
- A missing landing folder that serverless reports lazily no longer fails bronze ([#5]).
- The GitHub OIDC trust policy matches the immutable subject claim ([#2]).

[Unreleased]: https://github.com/cato-byte/eu-company-registry-lakehouse/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/cato-byte/eu-company-registry-lakehouse/releases/tag/v1.0.0
[#1]: https://github.com/cato-byte/eu-company-registry-lakehouse/pull/1
[#2]: https://github.com/cato-byte/eu-company-registry-lakehouse/pull/2
[#3]: https://github.com/cato-byte/eu-company-registry-lakehouse/pull/3
[#4]: https://github.com/cato-byte/eu-company-registry-lakehouse/pull/4
[#5]: https://github.com/cato-byte/eu-company-registry-lakehouse/pull/5
[#6]: https://github.com/cato-byte/eu-company-registry-lakehouse/pull/6
[#7]: https://github.com/cato-byte/eu-company-registry-lakehouse/pull/7
