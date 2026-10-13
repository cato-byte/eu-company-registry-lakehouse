"""Brønnøysund Enhetsregisteret -> canonical.

Bronze keeps nested objects as raw JSON text, which is parsed here with explicit schemas.
Observed quirks: optional keys (only 6.5% of units report a headcount), SN2007 codes with five
digits ('03.110'), status spread over boolean flags, foreign business addresses, and sole
proprietorships (ENK) named after the owner (natural persons).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from registry_lakehouse.silver import parsers as p
from registry_lakehouse.silver.classify import legal_form_category, status

CODE = StructType([StructField("kode", StringType()), StructField("beskrivelse", StringType())])
ADDRESS = StructType([
    StructField("land", StringType()),
    StructField("landkode", StringType()),
    StructField("postnummer", StringType()),
    StructField("poststed", StringType()),
    StructField("adresse", ArrayType(StringType())),
    StructField("kommune", StringType()),
    StructField("kommunenummer", StringType()),
])


def _flag(col: str) -> Column:
    return F.coalesce(p.parse_bool(col), F.lit(False))


def map_norway(bronze: DataFrame, snapshot_date: date) -> DataFrame:
    form = F.from_json("organisasjonsform", CODE)
    nace1 = F.from_json("naeringskode1", CODE)
    addr = F.coalesce(F.from_json("forretningsadresse", ADDRESS), F.from_json("postadresse", ADDRESS))
    orgnr = p.clean_text("organisasjonsnummer")
    natural = form["kode"] == "ENK"
    derived_status = (
        F.when(p.clean_text("slettedato").isNotNull(), F.lit("CLOSED"))
        .when(_flag("konkurs"), F.lit("INSOLVENCY"))
        .when(_flag("underAvvikling") | _flag("underTvangsavviklingEllerTvangsopplosning"), F.lit("LIQUIDATION"))
        .otherwise(F.lit("ACTIVE"))
    )
    employees = p.parse_int("antallAnsatte").cast("int")
    has_count = _flag("harRegistrertAntallAnsatte")
    return bronze.select(
        orgnr.alias("business_id"),
        orgnr.alias("legal_entity_id"),
        p.no_orgnr_valid(orgnr).alias("id_checksum_valid"),
        F.when(~natural, p.clean_upper("navn")).alias("company_name"),
        natural.alias("is_natural_person"),
        p.clean_text(form["beskrivelse"]).alias("legal_form_local"),
        legal_form_category(p.clean_text(form["kode"]), "NO").alias("legal_form_category"),
        derived_status.alias("status_local"),
        status(derived_status, "NO").alias("status"),
        F.coalesce(
            p.parse_date("stiftelsesdato", ["yyyy-MM-dd"], as_of=snapshot_date),
            p.parse_date("registreringsdatoEnhetsregisteret", ["yyyy-MM-dd"], as_of=snapshot_date),
        ).alias("registration_date"),
        p.parse_date("slettedato", ["yyyy-MM-dd"], as_of=snapshot_date).alias("closure_date"),
        p.clean_text(nace1["kode"]).alias("industry_code_local"),
        F.lit("SN2007").alias("industry_scheme"),
        p.nace_from_dotted(nace1["kode"]).alias("nace_code"),
        # Units without a registered count have 0 employees by definition of the register.
        F.when(has_count, employees).otherwise(F.lit(0)).alias("employees_min"),
        F.when(has_count, employees).otherwise(F.lit(0)).alias("employees_max"),
        F.when(_flag("registrertIMvaregisteret"), F.concat(F.lit("NO"), orgnr, F.lit("MVA"))).alias("vat_number"),
        F.when(~natural, F.upper(p.clean_text(F.array_join(addr["adresse"], ", ")))).alias("address_line"),
        p.postal_code(addr["postnummer"], "NO").alias("postal_code"),
        p.clean_upper(addr["poststed"]).alias("city"),
        p.clean_text(addr["kommunenummer"]).alias("municipality_code"),
        p.clean_upper(addr["kommune"]).alias("region"),
        F.upper(p.clean_text(addr["landkode"])).alias("address_country"),
        F.lit(None).cast("double").alias("source_x"),
        F.lit(None).cast("double").alias("source_y"),
        F.lit(None).cast("string").alias("source_crs"),
        F.lit(None).cast("boolean").alias("is_dormant"),
        F.lit(None).cast("boolean").alias("pending_strike_off"),
        F.lit(snapshot_date).cast("date").alias("snapshot_date"),
    )
