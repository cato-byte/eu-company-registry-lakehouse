"""RIK e-Business Register basic data -> canonical.

Observed quirks: ';' delimiter, UTF-8 BOM and CRLF (handled at bronze), dd.MM.yyyy dates,
``ettevotja_aadress`` is always empty (the street is in ``asukoht_ettevotja_aadressis``),
and the place is a 'district, city, county' string such as
'Pirita linnaosa, Tallinn, Harju maakond'. Sole traders (FIE) are natural persons.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from registry_lakehouse.silver import parsers as p
from registry_lakehouse.silver.classify import legal_form_category, status


def ee_city(ehak_text: Column) -> Column:
    """'Pirita linnaosa, Tallinn, Harju maakond' -> 'TALLINN'; 'Tartu linn, Tartu linn, ...' -> 'TARTU';
    'Viimsi küla, Viimsi vald, Harju maakond' -> 'VIIMSI'."""
    parts = F.split(ehak_text, r"\s*,\s*")
    first = F.try_element_at(parts, F.lit(1))
    second = F.try_element_at(parts, F.lit(2))
    settlement = F.when(first.rlike(r"linnaosa$"), second).otherwise(first)
    stripped = F.regexp_replace(settlement, r"\s+(linn|küla|alevik|alev|vald)$", "")
    return F.upper(F.trim(stripped))


def map_estonia(bronze: DataFrame, snapshot_date: date) -> DataFrame:
    code = p.clean_text("ariregistri_kood")
    form = p.clean_text("ettevotja_oiguslik_vorm")
    natural = form.rlike("(?i)füüsilisest isikust")
    ehak = p.clean_text("asukoha_ehak_tekstina")
    return bronze.select(
        code.alias("business_id"),
        code.alias("legal_entity_id"),
        p.ee_registry_code_valid(code).alias("id_checksum_valid"),
        F.when(~natural, p.clean_upper("nimi")).alias("company_name"),
        natural.alias("is_natural_person"),
        form.alias("legal_form_local"),
        legal_form_category(form, "EE").alias("legal_form_category"),
        p.clean_text("ettevotja_staatus").alias("status_local"),
        status(p.clean_text("ettevotja_staatus"), "EE").alias("status"),
        p.parse_date("ettevotja_esmakande_kpv", ["dd.MM.yyyy"], as_of=snapshot_date).alias("registration_date"),
        F.lit(None).cast("date").alias("closure_date"),
        F.lit(None).cast("string").alias("industry_code_local"),
        F.lit(None).cast("string").alias("industry_scheme"),
        F.lit(None).cast("string").alias("nace_code"),  # EMTAK codes are in a separate RIK file
        F.lit(None).cast("int").alias("employees_min"),
        F.lit(None).cast("int").alias("employees_max"),
        p.clean_upper("kmkr_nr").alias("vat_number"),
        F.when(~natural, p.clean_upper(
            F.coalesce(p.clean_text("ettevotja_aadress"), p.clean_text("asukoht_ettevotja_aadressis"))
        )).alias("address_line"),
        p.postal_code("indeks_ettevotja_aadressis", "EE").alias("postal_code"),
        ee_city(ehak).alias("city"),
        p.clean_text("asukoha_ehak_kood").alias("municipality_code"),
        F.upper(p.split_part(ehak, r"\s*,\s*", -1)).alias("region"),
        F.lit("EE").alias("address_country"),
        F.lit(None).cast("double").alias("source_x"),
        F.lit(None).cast("double").alias("source_y"),
        F.lit(None).cast("string").alias("source_crs"),
        F.lit(snapshot_date).cast("date").alias("snapshot_date"),
    )
