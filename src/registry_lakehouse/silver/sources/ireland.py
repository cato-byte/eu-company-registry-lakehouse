"""CRO companies -> canonical.

Observed quirks: 'Normal ' with a trailing space, '********NO ADDRESS DETAILS*******',
NACE codes stored as floats ('311.0' = 03.11), an older NACE Rev.1 column
(princ_object_code) that must not be mixed with Rev. 2, and the town buried in free-text
address lines ('DUBLIN 2, DUBLIN, IRELAND', 'CO. CORK, CORK').
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from registry_lakehouse.silver import parsers as p
from registry_lakehouse.silver.classify import legal_form_category, status


def ie_city(address_lines: list[str]) -> Column:
    """Town from the last populated address line: the first comma-separated token that is not
    the country or a county marker. 'DUBLIN 2' -> 'DUBLIN'."""
    last_line = F.coalesce(*[p.clean_upper(c) for c in reversed(address_lines)])
    tokens = F.transform(F.split(last_line, r"\s*,\s*"), lambda t: F.trim(F.regexp_replace(t, r"\.$", "")))
    candidates = F.filter(
        tokens,
        lambda t: (t != "") & (t != "IRELAND") & ~t.rlike(r"^(CO\.?|COUNTY) "),
    )
    first = F.try_element_at(candidates, F.lit(1))
    return F.regexp_replace(first, r"^DUBLIN\s*\d+W?$", "DUBLIN")


def ie_county(address_lines: list[str]) -> Column:
    last_line = F.coalesce(*[p.clean_upper(c) for c in reversed(address_lines)])
    m = F.regexp_extract(last_line, r"(?:CO\.?|COUNTY)\s+([A-Z]+)", 1)
    return F.when(m != "", m)


def map_ireland(bronze: DataFrame, snapshot_date: date) -> DataFrame:
    lines = ["company_address_1", "company_address_2", "company_address_3", "company_address_4"]
    number = p.parse_int("company_num").cast("string")
    nace_v2 = p.nace_from_numeric("nace_v2_code")
    return bronze.select(
        number.alias("business_id"),
        number.alias("legal_entity_id"),
        F.lit(None).cast("boolean").alias("id_checksum_valid"),  # CRO numbers carry no check digit
        p.clean_upper("company_name").alias("company_name"),
        F.lit(False).alias("is_natural_person"),
        p.clean_text("company_type").alias("legal_form_local"),
        legal_form_category(p.clean_text("company_type"), "IE").alias("legal_form_category"),
        p.clean_text("company_status").alias("status_local"),
        status(p.clean_text("company_status"), "IE").alias("status"),
        p.parse_date("company_reg_date", ["yyyy-MM-dd"], as_of=snapshot_date).alias("registration_date"),
        p.parse_date("comp_dissolved_date", ["yyyy-MM-dd"], as_of=snapshot_date).alias("closure_date"),
        F.coalesce(p.numeric_code("nace_v2_code"), p.clean_text("princ_object_code")).alias("industry_code_local"),
        F.when(p.clean_text("nace_v2_code").isNotNull(), F.lit("NACE_REV2"))
        .when(p.clean_text("princ_object_code").isNotNull(), F.lit("NACE_REV1"))
        .alias("industry_scheme"),
        nace_v2.alias("nace_code"),
        F.lit(None).cast("int").alias("employees_min"),
        F.lit(None).cast("int").alias("employees_max"),
        F.lit(None).cast("string").alias("vat_number"),
        p.clean_upper(p.concat_clean(p.clean_text("company_address_1"), p.clean_text("company_address_2"),
                                     sep=", ")).alias("address_line"),
        p.postal_code("eircode", "IE").alias("postal_code"),
        ie_city(lines).alias("city"),
        F.lit(None).cast("string").alias("municipality_code"),
        ie_county(lines).alias("region"),
        F.lit("IE").alias("address_country"),
        F.lit(None).cast("double").alias("source_x"),
        F.lit(None).cast("double").alias("source_y"),
        F.lit(None).cast("string").alias("source_crs"),
        F.lit(snapshot_date).cast("date").alias("snapshot_date"),
    )
