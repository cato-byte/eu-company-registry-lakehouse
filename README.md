# EU Company Registry Lakehouse

Six national company registers (France, United Kingdom, Ireland, Norway, Estonia and Navarra in Spain)
cleaned, harmonised and geocoded on **Databricks serverless (AWS)** with **Delta Lake**, then published
as a static map of where companies are, by town and industry, with a focus on **fishing and aquaculture**.

The whole pipeline runs and is tested **locally**, on synthetic fixtures that reproduce every quirk of the
real files. Databricks compute is only used for production runs.

```
 registers ──► AWS Lambda ──► S3 landing ──► bronze ──► silver ──► geo ──► gold ──► publish ──► static site
 (CSV/JSON)    resolve URL,   date-          raw rows,  typed,     free,    star     small       map + charts,
               skip if same,  partitioned    all STRING cleaned,   tiered   schema,  JSON/       no backend
               JSON→NDJSON    + manifest     + lineage  SCD2,      geocoding aggregates GeoJSON
                                                        DQ, PII
```

| | |
|---|---|
| **Architecture** | Medallion (bronze/silver/gold) + SCD Type 2 silver + Kimball gold. Why not Data Vault: [ADR 0001](docs/adr/0001-medallion-over-data-vault.md) |
| **Data cleaning** | Explicit per-source parsing of dates, numbers, codes and text, ANSI-safe. Every rule traces back to a quirk observed in the real files: [data-quality.md](docs/data-quality.md) |
| **Geocoding** | Free and tiered: register coordinates → postcode → district → town (offline GeoNames joins), plus a gradual Lambda worker on free government geocoders: [ADR 0002](docs/adr/0002-free-tiered-geocoding.md) |
| **Delta features** | MERGE (SCD2), Change Data Feed, time travel / RESTORE, CHECK constraints, liquid clustering, deletion vectors, predictive optimization, replaceWhere idempotency |
| **Orchestration** | One Lakeflow Job running a Python wheel on serverless, not a Lakeflow pipeline: [ADR 0004](docs/adr/0004-jobs-over-lakeflow-pipelines.md) |
| **Operations** | Asset Bundle job with for-each tasks, schedule, timeouts, duration health rules, e-mail and webhook alerts, schema-drift detection, quarantine, volume anomaly guard: [operations.md](docs/operations.md) |
| **Cost** | Ad hoc DBU/USD estimator calibrated from `system.billing` tables: [cost.md](docs/cost.md) |
| **Tests** | 220+ tests: parsers (incl. property-based), per-register mappers, SCD2, DQ engine, geocoding, Lambdas, a PySpark anti-pattern linter, query-plan checks, full end-to-end run: [testing.md](docs/testing.md) |
| **Privacy** | No real data in the repo, natural persons never named, personal IDs pseudonymised, secret scanning: [privacy.md](docs/privacy.md) |
| **Insights** | Operating vs merely registered companies, mass-registration addresses, births/deaths, cohort survival, fishing hubs (location quotient), jobs, distress rates, each with explicit country coverage: [insights.md](docs/insights.md) |
| **Visualisation** | Static site from gold aggregates, so there is no runtime access to AWS or Databricks: [ADR 0003](docs/adr/0003-static-visualisation.md) |

## Quick start (local, free)

Requires Python 3.11+ and Java 17.

```bash
make setup      # venv with pyspark 4.0 + delta 4.0 (matches serverless) and test tooling
make test-all   # lint + unit tests + end-to-end pipeline on the fixtures
make run-local  # run every stage, write site data to site/data/demo/
make site       # open http://localhost:8000
make cost       # predicted cost of a full production run
```

## Deploying

Every merge to `main` is a release candidate: CD runs the full test suite, waits for approval in the
GitHub `production` environment, then deploys the ingestion Lambdas (AWS SAM, via GitHub OIDC)
and the Databricks job (Asset Bundle, as a service principal). One-time setup and rollback are in
[docs/deployment.md](docs/deployment.md).

Bucket names, workspace host and e-mail addresses are never committed: they are GitHub environment
secrets, masked in the public Actions logs.

## Repository layout

```
src/registry_lakehouse/
  contracts/        source contracts (expected columns, delimiter, PII columns, drift policy)
  bronze/           landing discovery, schema-drift detection, raw loader
  silver/           parsers, vocabularies, per-register mappers, canonical model, SCD2 merge
  quality/          data-quality rules and single-pass engine (quarantine, metrics, thresholds)
  geo/              Lambert-93 projection, GeoNames gazetteer, tiered geocoder
  gold/             company dimension, town/industry aggregates, fishing view
  publish/          static JSON/GeoJSON export
  ingestion/        Lambdas: register downloader, Norway NDJSON converter, geocoding worker
  ops/              alerts, cost estimator
  cli.py            one entrypoint for every job task
resources/          Databricks job definition (Asset Bundle)
infra/              AWS SAM template + per-register download requests
site/               static website (map, fishing layer, pipeline tour)
tests/              unit, integration (e2e) and privacy tests + synthetic fixtures
tools/              sample/fixture builders, PySpark linter, sensitive-data scanner
docs/               architecture decisions, data quality, operations, cost, privacy, testing
```

## Data sources

| Country | Register | Grain | Licence |
|---|---|---|---|
| France | INSEE SIRENE (StockEtablissement) | establishment | Licence Ouverte 2.0 |
| United Kingdom | Companies House Basic Company Data | legal entity | OGL v3 |
| Ireland | CRO companies (opendata.cro.ie) | legal entity | CC BY 4.0 |
| Norway | Brønnøysund Enhetsregisteret | legal entity | NLOD 2.0 |
| Estonia | RIK e-Business Register, basic data | legal entity | CC BY 4.0 |
| Spain | Navarra establishments (yearly panel) | establishment | CC BY 4.0 |

Postcode centroids: [GeoNames](https://www.geonames.org/) (CC BY 4.0).
