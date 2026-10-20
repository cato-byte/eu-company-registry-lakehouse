# ADR 0002: Free, tiered geocoding (offline joins first, gradual API worker second)

**Status:** accepted (replaces the AWS Location Service + Nominatim pandas UDF)

## Context

The first version geocoded every new address inside Spark with a pandas UDF that called AWS
Location Service (paid, per request) and fell back to Nominatim, with `time.sleep(1)` inside
executors. With roughly 40 million French establishments plus the other registers that is:

* expensive (pay per address, plus serverless compute idling in `sleep`),
* against Nominatim's usage policy (bulk geocoding from parallel workers),
* fragile (one throttled call fails a Spark task, and retries re-pay for the work).

The map needs **town-level** precision, with optional address-level detail later.

## Decision

Geocode in tiers. Use the most precise tier available, and validate every candidate against the
country's bounding box:

| Tier | Precision | Source | Cost |
|---|---|---|---|
| 1 | ADDRESS | results cached from the gradual worker (below) | free APIs, once per address |
| 2 | ADDRESS | coordinates published by the register (France: Lambert-93, about two thirds of establishments in our samples) | free |
| 3 | POSTCODE | GeoNames postcode centroid (full postcode in the UK) | free, offline join |
| 4 | DISTRICT | UK outward code, Irish Eircode routing key | free, offline join |
| 5 | CITY | GeoNames place / municipality centroid | free, offline join |

Tiers 2 to 5 are **joins against an offline gazetteer** loaded once into
`silver.ref_postal_code`. They place every company with a usable postcode or town in seconds, with
no network calls. The Lambert-93 → WGS84 conversion is written as native Spark expressions and
verified against PROJ to 1 cm.

Tier 1 is filled **gradually, outside Spark**. Each run exports a bounded batch (e.g. 700) of the
most valuable coarse locations (fishing companies first, then by company count) to
`_GEOCODE/requests/`. A Lambda that runs daily picks up request files without results and geocodes them with **free government services** where
they exist (Kartverket for Norway, Maa-amet In-ADS for Estonia, IGN CartoCiudad for Spain) and
Nominatim at 1 req/s elsewhere. It validates each result (bounding box, postcode cross-check) and
writes `_GEOCODE/results/`, which the next run merges into the insert-only `silver.geocode_cache`.
Precision improves run after run, nothing is geocoded twice, and Spark never waits on HTTP.

## Consequences

* A full run costs nothing extra for geocoding and has no external dependency.
* Town-level maps work from day one, and address precision accumulates in the background.
* Free providers can be wrong. CartoCiudad's free-text search was observed returning a street in
  a different town, so results are only accepted when they agree with the register's postcode.
* GeoNames is CC BY 4.0: the attribution is shown on the site and in `meta.json`.
* For millions of address-level geocodes, self-host Nominatim/Photon or use a bulk service (for
  France, the BAN CSV API). The worker's provider table makes that a one-line change.
