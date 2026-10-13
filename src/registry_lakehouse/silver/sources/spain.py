"""Navarra establishments (yearly panel) -> canonical.

Observed quirks: one file holds several years (``anio``), and each year is treated as its
own snapshot so SCD2 rebuilds the history. Numbers were exported as floats ('5.0' street
numbers, CNAE '311.0' = 03.11 fishing). '0' is used as an unknown postal code. Headcount
comes in text bands ('De 1 a 2', '250 o más', 'Ninguna'). Bilingual municipality names
look like 'Pamplona / Iruña'. There is no establishment id, and about half of the tax ids
are personal DNI/NIE numbers, which are pseudonymised.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from registry_lakehouse.silver import parsers as p
from registry_lakehouse.silver.canonical import pseudonymise
from registry_lakehouse.silver.classify import legal_form_category, status


def employee_band(col: str) -> tuple[Column, Column]:
    s = F.lower(p.clean_text(col))
    lo = F.regexp_extract(s, r"(\d+)", 1)
    hi = F.regexp_extract(s, r"\d+\D+(\d+)", 1)
    mn = F.when(s == "ninguna", F.lit(0)).when(lo != "", lo.try_cast("int"))
    mx = (
        F.when(s == "ninguna", F.lit(0))
        .when(s.rlike(r"o m[aá]s"), F.lit(None).cast("int"))
        .when(hi != "", hi.try_cast("int"))
    )
    return mn, mx


# 'CARRETERA JAVIER KM1', 'CARRETERA JAVIER KM 1' and 'CARRETERA DE JAVIER' are the same trout farm in
# three yearly releases. The establishment key therefore uses the street reduced to its significant letters.
_STREET_NOISE = r"\b(DE|DEL|LA|LAS|LOS|EL|KM)\b|[^A-Z]"


def street_key(col: str) -> Column:
    plain = F.translate(F.upper(p.clean_text(col)), "ÁÉÍÓÚÜÑ", "AEIOUUN")
    return F.regexp_replace(F.regexp_replace(plain, r"KM\s*\d+", ""), _STREET_NOISE, "")


def snapshot_years(bronze: DataFrame) -> list[int]:
    years = bronze.select(p.parse_int("anio").alias("y")).where("y IS NOT NULL").distinct()
    rows = years.collect()  # spark-lint: ok[SPK001] a handful of distinct years
    return sorted(int(r.y) for r in rows)


def map_spain(bronze: DataFrame, snapshot_date: date) -> DataFrame:
    """Map the rows of the panel year that ``snapshot_date`` represents (Dec 31 of that year)."""
    year = snapshot_date.year
    rows = bronze.where(p.parse_int("anio") == F.lit(year))
    tax_id = F.upper(p.clean_text("dnici"))
    kind = p.es_tax_id_kind(tax_id)
    natural = kind == "NATURAL_PERSON"
    street_no = p.parse_int("portalt").cast("string")
    address = F.upper(p.concat_clean(p.clean_text("msiglat"), p.clean_text("callet"), street_no,
                                     p.clean_text("restot", extra_sentinels=["-PBJ-"])))
    municipality = p.clean_text("cod_cmun")
    # No establishment id is published: the establishment is the tax id at a given address.
    natural_key = F.concat_ws("|", tax_id, F.coalesce(p.numeric_code("cod_cmun", 3), F.lit("")),
                              F.coalesce(street_key("callet"), F.lit("")),
                              F.coalesce(street_no, F.lit("")))
    emp_min, emp_max = employee_band("tramo_asal")
    local_code = F.coalesce(p.numeric_code("cnae09_local"), p.numeric_code("cnae09_ppal"))
    return rows.select(
        F.when(natural, pseudonymise(natural_key)).otherwise(natural_key).alias("business_id"),
        F.when(natural, pseudonymise(tax_id)).otherwise(tax_id).alias("legal_entity_id"),
        F.lit(None).cast("boolean").alias("id_checksum_valid"),
        F.when(~natural, p.clean_upper("nombre")).alias("company_name"),
        natural.alias("is_natural_person"),
        F.when(~natural, F.substring(tax_id, 1, 1)).alias("legal_form_local"),
        legal_form_category(F.when(natural, F.lit("NATURAL_PERSON")).otherwise(tax_id), "ES")
        .alias("legal_form_category"),
        F.lit("ACTIVE").alias("status_local"),
        status(F.lit("ACTIVE"), "ES").alias("status"),
        F.lit(None).cast("date").alias("registration_date"),
        F.lit(None).cast("date").alias("closure_date"),
        local_code.alias("industry_code_local"),
        F.lit("CNAE_2009").alias("industry_scheme"),
        p.nace_from_numeric(local_code).alias("nace_code"),
        emp_min.alias("employees_min"),
        emp_max.alias("employees_max"),
        F.lit(None).cast("string").alias("vat_number"),
        F.when(~natural, address).alias("address_line"),
        p.postal_code("codpost", "ES").alias("postal_code"),
        F.upper(p.split_part("nombre_cmun", r"\s*/\s*", 0)).alias("city"),
        F.when(municipality.isNotNull(), F.concat(F.lit("31"), F.lpad(municipality, 3, "0")))
        .alias("municipality_code"),
        F.lit("NAVARRA").alias("region"),
        F.lit("ES").alias("address_country"),
        F.lit(None).cast("double").alias("source_x"),
        F.lit(None).cast("double").alias("source_y"),
        F.lit(None).cast("string").alias("source_crs"),
        F.lit(snapshot_date).cast("date").alias("snapshot_date"),
    )
