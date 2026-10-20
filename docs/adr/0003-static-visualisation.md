# ADR 0003: Static visualisation on the website (not QuickSight or live dashboards)

**Status:** accepted

## Options considered

| | Amazon QuickSight embedded | Databricks AI/BI dashboard embedded | **Static files + JS map** |
|---|---|---|---|
| Public, anonymous viewers | Anonymous embedding needs capacity pricing; per-session costs | External embedding needs auth / service principal setup | Any visitor, no auth |
| Running cost | Monthly fee + sessions | Serverless SQL warehouse while viewed | Zero (static hosting) |
| Attack surface | Embed URLs, IAM roles, an AWS account reachable from the browser | Workspace endpoint + tokens | None: no backend, no credentials |
| Data exposure | Whatever the dataset holds | Whatever the query can reach | Only the aggregates you exported |
| Fits a portfolio site | Heavy iframe | Heavy iframe | Native page, styled like the site |
| Freshness | Live | Live | As of the last pipeline run |

## Decision

The pipeline's last task (`publish`) writes **small, aggregated JSON/GeoJSON** (towns × industries,
fishing per town, pipeline stage counts, data-quality results) to a publish location. The website
serves those files next to `site/index.html` (Leaflet map + plain HTML/CSS charts).

So yes: static is the right call. The trade-off is freshness, and the registers publish monthly (or
daily but slowly changing), so the monthly refresh loses nothing.

## Guard rails

* Only aggregates are exported. No row-level company data and no names leave the lakehouse.
* The "one record, three stages" tour (`stage_samples.json`) is generated **only** from the synthetic
  test fixtures (`--samples` flag, used by `make run-local`), never in production.
* Output size is tested (< 2 MB on fixtures; production is capped by `max_cities`).
* QuickSight remains a fine choice for an internal, authenticated audience later. It would read the
  same gold tables.
