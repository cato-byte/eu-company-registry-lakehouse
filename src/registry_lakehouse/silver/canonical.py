"""The canonical company model shared by all six registers.

Each source mapper (silver/sources/*.py) produces the MAPPED columns from its bronze table.
``finalise`` then derives everything that is computed the same way for every country: keys,
NACE hierarchy, pseudonymisation and the change-detection hash.
"""

from __future__ import annotations

import os

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from registry_lakehouse.silver.classify import nace_section, valid_nace

# (name, Spark SQL type, description): single source of truth for the silver DDL and the docs.
MAPPED_COLUMNS: list[tuple[str, str, str]] = [
    ("business_id", "STRING", "Register identifier at the source grain (SIRET, company number, ...). "
                              "Pseudonymised for natural persons."),
    ("legal_entity_id", "STRING", "Identifier of the legal unit (SIREN for France), else = business_id"),
    ("id_checksum_valid", "BOOLEAN", "Identifier passes the register's check-digit rule (NULL if none exists)"),
    ("company_name", "STRING", "Display name. NULL for natural persons (data minimisation)"),
    ("is_natural_person", "BOOLEAN", "Sole trader / individual; name and street are never stored"),
    ("legal_form_local", "STRING", "Legal form as published"),
    ("legal_form_category", "STRING", "Harmonised legal form"),
    ("status_local", "STRING", "Status as published"),
    ("status", "STRING", "ACTIVE | LIQUIDATION | INSOLVENCY | CLOSED | UNKNOWN"),
    ("registration_date", "DATE", "Incorporation / creation date"),
    ("closure_date", "DATE", "Dissolution / closure date"),
    ("industry_code_local", "STRING", "Activity code as published"),
    ("industry_scheme", "STRING", "Classification of industry_code_local"),
    ("nace_code", "STRING", "NACE Rev. 2 class, e.g. 03.11 (NULL if not mappable)"),
    ("employees_min", "INT", "Lower bound of headcount (exact count when min = max)"),
    ("employees_max", "INT", "Upper bound of headcount (NULL = open-ended band)"),
    ("vat_number", "STRING", "VAT id when published"),
    ("address_line", "STRING", "Street address. NULL for natural persons"),
    ("postal_code", "STRING", "Normalised postal code"),
    ("city", "STRING", "Town / locality, upper case"),
    ("municipality_code", "STRING", "Official municipality code (INSEE, EHAK, kommunenummer, INE)"),
    ("region", "STRING", "County / province / maakond"),
    ("address_country", "STRING", "ISO-2 country of the address (can differ from the register)"),
    ("source_x", "DOUBLE", "Projected X coordinate supplied by the register"),
    ("source_y", "DOUBLE", "Projected Y coordinate supplied by the register"),
    ("source_crs", "STRING", "CRS of source_x/source_y, e.g. EPSG:2154"),
]
MAPPED = [c[0] for c in MAPPED_COLUMNS]

# Attributes whose change opens a new SCD2 version.
TRACKED = [
    "company_name", "legal_form_local", "legal_form_category", "status", "registration_date", "closure_date",
    "nace_code", "industry_code_local", "employees_min", "employees_max", "vat_number",
    "address_line", "postal_code", "city", "municipality_code", "region", "address_country",
]

DERIVED_COLUMNS: list[tuple[str, str, str]] = [
    ("company_key", "STRING", "sha2(country_code|business_id): surrogate key, stable across snapshots"),
    ("location_key", "STRING", "sha2 of the normalised address; shared by companies at the same address"),
    ("country_code", "STRING", "Register country (ISO-2)"),
    ("source_register", "STRING", "Publisher dataset"),
    ("grain", "STRING", "LEGAL_ENTITY or ESTABLISHMENT"),
    ("nace_division", "STRING", "NACE division, e.g. 03"),
    ("nace_section", "STRING", "NACE section letter, e.g. A"),
    ("is_active", "BOOLEAN", "status = ACTIVE"),
    ("hash_diff", "STRING", "sha2 over tracked attributes: change detection for SCD2"),
    ("snapshot_date", "DATE", "Register snapshot the row comes from"),
]


def pseudonymise(value: Column) -> Column:
    """Keyed hash for personal identifiers (Spanish DNI/NIE...).

    A plain sha2 of a 9-digit DNI can be reversed by brute force, so a secret pepper (a
    Databricks secret in production) is prepended. The same input always yields the same
    output, so joins and SCD2 history still work.
    """
    pepper = os.environ.get("REGISTRY_PSEUDONYM_KEY")
    if not pepper:
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            raise RuntimeError("REGISTRY_PSEUDONYM_KEY missing: pass --secret-scope with a 'pseudonym-key' secret")
        pepper = "local-development-pepper"
    return F.sha2(F.concat(F.lit(pepper), F.lit("|"), value), 256)


def location_key(country: Column, postal: Column, city: Column, address: Column) -> Column:
    norm = F.concat_ws(
        "|", country,
        F.coalesce(postal, F.lit("")),
        F.coalesce(F.upper(city), F.lit("")),
        F.coalesce(F.regexp_replace(F.upper(address), r"[^0-9A-Z\x{00C0}-\x{017F}]", ""), F.lit("")),
    )
    return F.sha2(norm, 256)


def hash_diff(cols: list[str]) -> Column:
    # Explicit NULL marker so (NULL, 'a') and ('', 'a') hash differently.
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("^N")) for c in cols]), 256)


def finalise(mapped: DataFrame, country_code: str, source_register: str, grain: str) -> DataFrame:
    missing = [c for c in MAPPED if c not in mapped.columns]
    if missing:
        raise ValueError(f"[{country_code}] mapper did not produce {missing}")
    nace = valid_nace(F.col("nace_code"))
    df = mapped.select(
        *[F.col(c) for c in MAPPED if c != "nace_code"],
        nace.alias("nace_code"),
        "snapshot_date",
    )
    division = F.substring("nace_code", 1, 2)
    return df.select(
        F.sha2(F.concat_ws("|", F.lit(country_code), F.col("business_id")), 256).alias("company_key"),
        location_key(F.lit(country_code), F.col("postal_code"), F.col("city"), F.col("address_line")).alias(
            "location_key"
        ),
        F.lit(country_code).alias("country_code"),
        F.lit(source_register).alias("source_register"),
        F.lit(grain).alias("grain"),
        *[F.col(c) for c in MAPPED],
        division.alias("nace_division"),
        nace_section(division).alias("nace_section"),
        (F.col("status") == "ACTIVE").alias("is_active"),
        hash_diff(TRACKED).alias("hash_diff"),
        F.col("snapshot_date"),
    )


def silver_ddl() -> str:
    cols = DERIVED_COLUMNS[:5] + MAPPED_COLUMNS + DERIVED_COLUMNS[5:]
    return ", ".join(f"{n} {t} COMMENT '{d.replace(chr(39), '')}'" for n, t, d in cols)


CANONICAL_ORDER = [c[0] for c in DERIVED_COLUMNS[:5] + MAPPED_COLUMNS + DERIVED_COLUMNS[5:]]
