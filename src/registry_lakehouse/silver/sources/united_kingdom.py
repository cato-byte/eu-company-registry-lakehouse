"""Companies House Basic Company Data -> canonical.

Observed quirks: header names with leading spaces and dots (fixed at bronze), dd/MM/yyyy
dates, SIC codes as '03110 - Marine fishing' text, 'None Supplied', the 99999 dormant
placeholder, and RegAddress.Country holding ENGLAND / SCOTLAND / UNITED KINGDOM / blank.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from registry_lakehouse.silver import parsers as p
from registry_lakehouse.silver.classify import legal_form_category, status

UK_COUNTRIES = ["ENGLAND", "WALES", "SCOTLAND", "NORTHERN IRELAND", "UNITED KINGDOM", "GREAT BRITAIN",
                "ENGLAND AND WALES", "UK", "GB"]


def map_united_kingdom(bronze: DataFrame, snapshot_date: date) -> DataFrame:
    number = F.upper(p.clean_text("CompanyNumber"))
    country_raw = p.clean_upper("RegAddress_Country")
    return bronze.select(
        number.alias("business_id"),
        number.alias("legal_entity_id"),
        p.gb_company_number_valid(number).alias("id_checksum_valid"),
        p.clean_upper("CompanyName").alias("company_name"),
        F.lit(False).alias("is_natural_person"),
        p.clean_text("CompanyCategory").alias("legal_form_local"),
        legal_form_category(p.clean_text("CompanyCategory"), "GB").alias("legal_form_category"),
        p.clean_text("CompanyStatus").alias("status_local"),
        status(p.clean_text("CompanyStatus"), "GB").alias("status"),
        p.parse_date("IncorporationDate", ["dd/MM/yyyy"], as_of=snapshot_date).alias("registration_date"),
        p.parse_date("DissolutionDate", ["dd/MM/yyyy"], as_of=snapshot_date).alias("closure_date"),
        p.clean_text("SICCode_SicText_1").alias("industry_code_local"),
        F.lit("UK_SIC_2007").alias("industry_scheme"),
        p.nace_from_uk_sic("SICCode_SicText_1").alias("nace_code"),
        F.lit(None).cast("int").alias("employees_min"),
        F.lit(None).cast("int").alias("employees_max"),
        F.lit(None).cast("string").alias("vat_number"),
        p.clean_upper(p.concat_clean(p.clean_text("RegAddress_AddressLine1"),
                                     p.clean_text("RegAddress_AddressLine2"), sep=", ")).alias("address_line"),
        p.postal_code("RegAddress_PostCode", "GB").alias("postal_code"),
        p.clean_upper("RegAddress_PostTown").alias("city"),
        F.lit(None).cast("string").alias("municipality_code"),
        p.clean_upper("RegAddress_County").alias("region"),
        F.when(country_raw.isNull() | country_raw.isin(UK_COUNTRIES), F.lit("GB")).alias("address_country"),
        F.lit(None).cast("double").alias("source_x"),
        F.lit(None).cast("double").alias("source_y"),
        F.lit(None).cast("string").alias("source_crs"),
        F.lit(snapshot_date).cast("date").alias("snapshot_date"),
    )
