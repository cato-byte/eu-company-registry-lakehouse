"""INSEE SIRENE establishments -> canonical.

Observed quirks: '[ND]' placeholders on non-diffusible rows (statutDiffusion = 'P'), creation
date 1900-01-01 meaning "unknown", four activity nomenclatures (only NAFRev2 maps to NACE),
Lambert-93 coordinates that are only valid in metropolitan France, and arrondissement
names such as 'PARIS 15'.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from registry_lakehouse.silver import parsers as p
from registry_lakehouse.silver.classify import legal_form_category, status

# INSEE headcount bands (trancheEffectifs) -> (min, max)
EMPLOYEE_BANDS = {
    "00": (0, 0), "01": (1, 2), "02": (3, 5), "03": (6, 9), "11": (10, 19), "12": (20, 49),
    "21": (50, 99), "22": (100, 199), "31": (200, 249), "32": (250, 499), "41": (500, 999),
    "42": (1000, 1999), "51": (2000, 4999), "52": (5000, 9999), "53": (10000, None),
}


def _band(col: str, idx: int):
    m = F.create_map(*[x for k, v in EMPLOYEE_BANDS.items()
                       for x in (F.lit(k), F.lit(v[idx]).cast("int"))])
    return m[p.clean_text(col)]


def map_france(bronze: DataFrame, snapshot_date: date) -> DataFrame:
    diffusible = F.col("statutDiffusionEtablissement") == "O"
    commune = p.clean_text("codeCommuneEtablissement")
    metropolitan = ~commune.rlike(r"^(97|98)")  # overseas uses other projections
    city = p.clean_upper("libelleCommuneEtablissement")
    street = p.concat_clean(
        p.clean_text("numeroVoieEtablissement"),
        p.clean_text("indiceRepetitionEtablissement"),
        p.clean_text("typeVoieEtablissement"),
        p.clean_text("libelleVoieEtablissement"),
    )
    lambert_ok = diffusible & metropolitan
    return bronze.select(
        p.clean_text("siret").alias("business_id"),
        p.clean_text("siren").alias("legal_entity_id"),
        p.siret_valid(p.clean_text("siret")).alias("id_checksum_valid"),
        p.clean_upper(
            F.coalesce(p.clean_text("denominationUsuelleEtablissement"), p.clean_text("enseigne1Etablissement"))
        ).alias("company_name"),
        # Natural persons cannot be told apart in the establishment file (that needs StockUniteLegale).
        # Non-diffusible rows are treated as personal and handled as such.
        F.when(~diffusible, F.lit(True)).alias("is_natural_person"),
        F.lit(None).cast("string").alias("legal_form_local"),
        legal_form_category(F.lit(None).cast("string"), "FR").alias("legal_form_category"),
        p.clean_text("etatAdministratifEtablissement").alias("status_local"),
        status(p.clean_text("etatAdministratifEtablissement"), "FR").alias("status"),
        p.parse_date("dateCreationEtablissement", ["yyyy-MM-dd"], as_of=snapshot_date).alias("registration_date"),
        F.when(
            F.col("etatAdministratifEtablissement") == "F",
            p.parse_date("dateDebut", ["yyyy-MM-dd"], as_of=snapshot_date),
        ).alias("closure_date"),
        p.clean_text("activitePrincipaleEtablissement").alias("industry_code_local"),
        p.clean_text("nomenclatureActivitePrincipaleEtablissement").alias("industry_scheme"),
        F.when(
            F.col("nomenclatureActivitePrincipaleEtablissement") == "NAFRev2",
            p.nace_from_dotted("activitePrincipaleEtablissement"),
        ).alias("nace_code"),
        _band("trancheEffectifsEtablissement", 0).alias("employees_min"),
        _band("trancheEffectifsEtablissement", 1).alias("employees_max"),
        F.lit(None).cast("string").alias("vat_number"),
        F.when(diffusible, F.upper(street)).alias("address_line"),
        p.postal_code("codePostalEtablissement", "FR").alias("postal_code"),
        F.regexp_replace(city, r"^(PARIS|LYON|MARSEILLE) \d+.*$", "$1").alias("city"),
        commune.alias("municipality_code"),
        F.substring(commune, 1, 2).alias("region"),  # departement
        F.when(p.clean_text("codePaysEtrangerEtablissement").isNull(), F.lit("FR")).otherwise(F.lit(None))
        .alias("address_country"),
        F.when(lambert_ok, p.parse_decimal("coordonneeLambertAbscisseEtablissement")).alias("source_x"),
        F.when(lambert_ok, p.parse_decimal("coordonneeLambertOrdonneeEtablissement")).alias("source_y"),
        F.when(lambert_ok & p.parse_decimal("coordonneeLambertAbscisseEtablissement").isNotNull(),
               F.lit("EPSG:2154")).alias("source_crs"),
        F.lit(None).cast("boolean").alias("is_dormant"),
        F.lit(None).cast("boolean").alias("pending_strike_off"),
        F.lit(snapshot_date).cast("date").alias("snapshot_date"),
    )
