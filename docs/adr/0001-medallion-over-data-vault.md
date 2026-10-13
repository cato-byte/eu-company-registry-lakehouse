# ADR 0001: Medallion with SCD2 silver and Kimball gold, not Data Vault 2.0

**Status:** accepted (replaces the hub/link/satellite prototype in the first commit)

## Context

The first version modelled the six registers as a Data Vault 2.0 (hubs for company and location, a
link, satellites for profile, metrics and geocodes). The goals of the project are:

1. a map of companies by town and industry (with a fishing focus),
2. history of how companies change between register snapshots,
3. low and predictable cost on Databricks serverless,
4. code that can be tested end to end on a laptop.

## What Data Vault is good at, and why it doesn't pay off here

Data Vault earns its complexity when **many source systems describe the same business keys**
(CRM, ERP and billing all knowing customer 42), when sources change their structure often, and when
an auditable, insert-only raw history is a regulatory requirement.

None of that applies here:

* **No shared business keys.** A SIRET, a UK company number and a Norwegian organisation number
  never refer to the same entity. Every country has its own key space, so a hub never integrates
  two sources. It is a renamed copy of each register's ID column. Cross-border entity resolution
  (same company in two registers) is not a goal, and would need fuzzy matching anyway, not a hub.
* **One core entity.** Company/establishment plus its address. A hub/link/satellite split turns
  every query into a 5-6 way join (that is exactly what the old `vault_to_gold` did), and on
  serverless every shuffle is billed.
* **Auditability is already in the platform.** Bronze keeps every raw snapshot unchanged (append-only,
  with lineage columns), and Delta keeps every version of every table (time travel, `RESTORE`,
  Change Data Feed). The audit trail Data Vault builds by hand comes for free.
* **Snapshots, not change events.** Registers publish full dumps. History is obtained by comparing
  snapshots, which is precisely what an SCD Type 2 merge does.

## Decision

* **Bronze:** one table per register, raw columns as STRING, append-only per snapshot, lineage
  columns, `_rescued_data` and `_corrupt_record`.
* **Silver:** one harmonised `silver.company` table, **SCD Type 2** (`valid_from`, `valid_to`,
  `is_current`) driven by a `hash_diff` of normalised tracked attributes. One `MERGE` per snapshot.
  Plus `silver.location` (geocoded distinct addresses), `silver.quarantine`, reference tables.
* **Gold:** a Kimball star: `dim_company` (current state + location + NACE labels), `dim_industry`,
  and pre-aggregated facts for the map (`agg_city_industry`, `agg_country_industry`,
  `fishing_by_city`).

Data Vault ideas that were worth keeping are kept: hash keys (`company_key`, `location_key`),
`hash_diff` change detection, `record_source`/lineage, insert-only raw history.

## Consequences

* One `MERGE` per country and snapshot instead of six table writes, and gold reads one table, not six.
* SCD2 needs normalised inputs: format noise must not look like a change. Two real cases were
  found and fixed in tests: Navarra exports `4782.0` one year and `4782` the next, and some
  years are double-encoded (mojibake). See [data-quality.md](../data-quality.md).
* If cross-register entity resolution ever becomes a goal, a `link_same_entity` table can be added
  next to silver without remodelling.
