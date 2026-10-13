"""Gold insight tables: questions the registers can answer beyond "how many companies, where".

* company_activity_summary: how many registered companies are really operating? (all countries,
  signals vary)
* registered_office_hotspots: which addresses host mass registrations (formation agents)?
* company_demography: companies born / dying per year and industry. Births FR GB IE EE NO (ES panel
  entries), deaths FR IE NO + register exits
* cohort_survival: share of a year's new companies alive 1/3/5/10 years later (FR IE only:
  registers that keep closed units)
* fishing_hubs: where fishing really matters, beyond raw counts (FR GB IE NO ES)
* employment_by_region_industry: jobs, not just companies (NO exact, FR ES bands)
* distress_by_industry: liquidation / insolvency / strike-off rates (GB IE EE NO)
* insight_coverage: which country feeds which insight, and why not

A country is excluded from an insight when its register does not publish the needed fields,
so no zero is ever shown for "unknown". The coverage table states each exclusion.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from registry_lakehouse.geo.reference import norm_place

# Registers that keep closed / dissolved units with their closure date (needed for survival analysis).
KEEPS_CLOSED_UNITS = ("FR", "IE")
# Live-only registers: older cohorts look smaller because their dead companies are gone.
SURVIVORSHIP_BIASED = ("GB", "EE", "NO")
DISTRESS_VOCABULARY = ("GB", "IE", "EE", "NO")  # FR publishes only open/closed, Navarra only presence
SURVIVAL_HORIZONS = (1, 3, 5, 10)
MIN_YEAR = 1980


# --------------------------------------------------------------------------------------------
# Operating vs merely registered
# --------------------------------------------------------------------------------------------
def mass_addresses(dim: DataFrame, min_companies: int) -> DataFrame:
    """Street addresses hosting at least ``min_companies`` active companies (formation agents,
    virtual offices, accountants). Natural persons have no street, so they can never form a hotspot."""
    with_street = dim.where("is_active AND has_street_address")
    return (
        with_street.groupBy("country_code", "location_key")
        .agg(
            F.count(F.lit(1)).alias("companies"),
            F.first("address_line", ignorenulls=True).alias("address_line"),
            F.first("postal_code", ignorenulls=True).alias("postal_code"),
            F.first("city", ignorenulls=True).alias("city"),
            F.sum(F.coalesce(F.col("is_dormant").cast("int"), F.lit(0))).alias("dormant_companies"),
            F.sum(F.coalesce(F.col("pending_strike_off").cast("int"), F.lit(0))).alias("pending_strike_off"),
            F.countDistinct("nace_section").alias("distinct_industries"),
            F.round(F.avg("latitude"), 5).alias("latitude"),
            F.round(F.avg("longitude"), 5).alias("longitude"),
        )
        .where(F.col("companies") >= min_companies)
    )


def flag_operating(dim: DataFrame, hotspots: DataFrame) -> DataFrame:
    """Add at_mass_address and is_operating = active, not dormant, not being struck off, and not
    registered at a mass address (where the registered office says nothing about real location)."""
    hot = hotspots.select("location_key", F.lit(True).alias("_hot"))
    return (
        dim.join(F.broadcast(hot), "location_key", "left")
        .withColumn("at_mass_address", F.coalesce("_hot", F.lit(False)))
        .withColumn("is_operating", F.col("is_active") & ~F.coalesce("is_dormant", F.lit(False))
                    & ~F.coalesce("pending_strike_off", F.lit(False)) & ~F.col("at_mass_address"))
        .drop("_hot")
    )


def company_activity_summary(flagged: DataFrame) -> DataFrame:
    def share(c: str) -> F.Column:
        return F.round(F.col(c) / F.col("active_companies"), 4)

    agg = flagged.where("is_active").groupBy("country_code").agg(
        F.count(F.lit(1)).alias("active_companies"),
        # NULL (not 0) when the register does not publish the signal
        F.sum(F.col("is_dormant").cast("int")).alias("dormant"),
        F.sum(F.col("pending_strike_off").cast("int")).alias("pending_strike_off"),
        F.sum(F.col("at_mass_address").cast("int")).alias("at_mass_addresses"),
        F.sum(F.col("is_operating").cast("int")).alias("operating_estimate"),
    )
    return agg.select("*", share("dormant").alias("dormant_share"),
                      share("pending_strike_off").alias("pending_strike_off_share"),
                      share("at_mass_addresses").alias("mass_address_share"),
                      share("operating_estimate").alias("operating_share"))


# --------------------------------------------------------------------------------------------
# Business demography
# --------------------------------------------------------------------------------------------
def company_demography(history: DataFrame, as_of: date) -> DataFrame:
    """Births and deaths per country, year and NACE section, from the full SCD2 history.

    Births: registration year where the register publishes it. For Navarra (no dates) the first
    appearance in the yearly panel, except in the first panel year, which is only the starting stock.
    Deaths: closure_date where published, otherwise the year a unit left the register (SCD2 exit).
    """
    w = Window.partitionBy("company_key").orderBy(F.col("valid_from").desc())
    latest = history.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    first_seen = history.groupBy("company_key").agg(F.min("valid_from").alias("first_seen"))
    first_panel = history.groupBy("country_code").agg(F.min("valid_from").alias("first_panel"))
    units = latest.join(first_seen, "company_key").join(first_panel, "country_code")

    birth_year = F.coalesce(
        F.year("registration_date"),
        F.when((F.col("country_code") == "ES") & (F.col("first_seen") > F.col("first_panel")), F.year("first_seen")),
    )
    exited = ~F.col("is_current")
    death_year = F.coalesce(F.year("closure_date"), F.when(exited, F.year("valid_to")))

    births = units.where(birth_year.isNotNull()).select(
        "country_code", birth_year.alias("year"), "nace_section", F.lit(1).alias("births"), F.lit(0).alias("deaths"))
    deaths = units.where(death_year.isNotNull()).select(
        "country_code", death_year.alias("year"), "nace_section", F.lit(0).alias("births"), F.lit(1).alias("deaths"))
    return (
        births.unionByName(deaths)
        .where(F.col("year").between(MIN_YEAR, as_of.year))
        .groupBy("country_code", "year", "nace_section")
        .agg(F.sum("births").alias("births"), F.sum("deaths").alias("deaths"))
        .withColumn("net_formation", F.col("births") - F.col("deaths"))
        .withColumn("survivorship_biased", F.col("country_code").isin(*SURVIVORSHIP_BIASED))
    )


def cohort_survival(current: DataFrame, as_of: date) -> DataFrame:
    """Share of each registration-year cohort still alive k years later (FR, IE only).

    Only cohorts that can be observed for k full years count towards survival_<k>y, so recent
    cohorts are not penalised for being young.
    """
    base = current.where(F.col("country_code").isin(*KEEPS_CLOSED_UNITS) & F.col("registration_date").isNotNull()
                         & (F.year("registration_date") >= MIN_YEAR))
    cols = []
    for k in SURVIVAL_HORIZONS:
        horizon = F.add_months(F.col("registration_date"), 12 * k)
        observable = horizon <= F.lit(as_of)
        alive = F.col("closure_date").isNull() | (F.col("closure_date") > horizon)
        cols.append(F.round(F.sum(F.when(observable & alive, 1).otherwise(0))
                            / F.nullif(F.sum(F.when(observable, 1).otherwise(0)), F.lit(0)), 4)
                    .alias(f"survival_{k}y"))
    by_year = base.groupBy("country_code", F.year("registration_date").alias("cohort_year")).agg(
        F.count(F.lit(1)).alias("cohort_size"), *cols).withColumn("nace_section", F.lit("ALL"))
    by_section = base.groupBy("country_code", F.lit(None).cast("int").alias("cohort_year"), "nace_section").agg(
        F.count(F.lit(1)).alias("cohort_size"), *cols).where("nace_section IS NOT NULL")
    return by_year.unionByName(by_section)


# --------------------------------------------------------------------------------------------
# Fishing specialisation
# --------------------------------------------------------------------------------------------
def fishing_hubs(dim: DataFrame, towns: DataFrame | None, min_fishing: int) -> DataFrame:
    """Location quotient: a town's share of fishing companies divided by its country's share.
    LQ 10 means fishing is ten times more prominent there than nationally. Fishing per 10,000
    inhabitants uses GeoNames town populations where the town is matched."""
    active = dim.where("is_active AND city IS NOT NULL AND nace_section IS NOT NULL")
    country = active.groupBy("country_code").agg(
        (F.sum(F.when(F.col("nace_division") == "03", 1).otherwise(0)) / F.count(F.lit(1))).alias("country_share"))
    town = active.groupBy("country_code", "city").agg(
        F.count(F.lit(1)).alias("active_companies"),
        F.sum(F.when(F.col("nace_division") == "03", 1).otherwise(0)).alias("fishing_companies"),
        F.round(F.avg("latitude"), 5).alias("latitude"), F.round(F.avg("longitude"), 5).alias("longitude"),
    )
    hubs = (
        town.join(country.where("country_share > 0"), "country_code")
        .where(F.col("fishing_companies") >= min_fishing)
        .withColumn("fishing_share", F.round(F.col("fishing_companies") / F.col("active_companies"), 4))
        .withColumn("location_quotient", F.round(F.col("fishing_share") / F.col("country_share"), 2))
        .withColumn("country_share", F.round("country_share", 5))
    )
    if towns is None:
        return hubs.withColumn("population", F.lit(None).cast("bigint")).withColumn(
            "fishing_per_10k_inhabitants", F.lit(None).cast("double"))
    pop = towns.groupBy("country_code", "place_key").agg(F.max("population").alias("population"))
    return (
        hubs.withColumn("place_key", norm_place(F.col("city")))
        .join(pop, ["country_code", "place_key"], "left").drop("place_key")
        .withColumn("population", F.when(F.col("population") > 0, F.col("population")))
        .withColumn("fishing_per_10k_inhabitants",
                    F.round(F.col("fishing_companies") * 10_000 / F.col("population"), 3))
    )


# --------------------------------------------------------------------------------------------
# Employment and distress
# --------------------------------------------------------------------------------------------
def employment_by_region_industry(dim: DataFrame) -> DataFrame:
    """Jobs from headcount: exact for Norway, bands for France and Navarra (low/high bounds;
    open-ended top bands count at their lower bound)."""
    reporting = dim.where("is_active AND employees_min IS NOT NULL")
    return reporting.groupBy("country_code", "region", "nace_section", "nace_division").agg(
        F.count(F.lit(1)).alias("companies_reporting"),
        F.sum("employees_min").alias("employees_low"),
        F.sum(F.coalesce("employees_max", "employees_min")).alias("employees_high"),
        F.max(F.col("employees_min") == F.col("employees_max")).alias("has_exact_counts"),
    ).withColumn("employees_mid", F.round((F.col("employees_low") + F.col("employees_high")) / 2))


def distress_by_industry(dim: DataFrame) -> DataFrame:
    live = dim.where(F.col("country_code").isin(*DISTRESS_VOCABULARY) & (F.col("status") != "CLOSED"))

    def n(cond: str) -> F.Column:
        return F.sum(F.when(F.expr(cond), 1).otherwise(0))

    agg = live.groupBy("country_code", "nace_section").agg(
        F.count(F.lit(1)).alias("registered"),
        n("status = 'LIQUIDATION'").alias("in_liquidation"),
        n("status = 'INSOLVENCY'").alias("insolvent"),
        n("coalesce(pending_strike_off, false)").alias("pending_strike_off"),
    )
    return agg.select(
        "*",
        F.round((F.col("in_liquidation") + F.col("insolvent")) / F.col("registered"), 4).alias("distress_rate"),
        F.round(F.col("pending_strike_off") / F.col("registered"), 4).alias("strike_off_rate"),
    )


# --------------------------------------------------------------------------------------------
COVERAGE = [
    # (insight, country, included, reason)
    ("company_activity_summary", "GB", True, "dormant accounts, strike-off proposals, mass addresses"),
    ("company_activity_summary", "IE", True, "strike-off list, mass addresses"),
    ("company_activity_summary", "FR", True, "mass addresses only (no dormancy signal)"),
    ("company_activity_summary", "EE", True, "mass addresses only"),
    ("company_activity_summary", "NO", True, "mass addresses only"),
    ("company_activity_summary", "ES", True, "mass addresses only"),
    ("company_demography", "GB", True, "births only. Live-only register: older years under-counted"),
    ("company_demography", "EE", True, "births only. Live-only register"),
    ("company_demography", "NO", True, "births. Deaths only for units deleted between snapshots"),
    ("company_demography", "FR", True, "births and closures (establishments)"),
    ("company_demography", "IE", True, "births and dissolutions"),
    ("company_demography", "ES", True, "panel entries and exits between yearly releases"),
    ("cohort_survival", "FR", True, "closed establishments kept with closure date"),
    ("cohort_survival", "IE", True, "dissolved companies kept with dissolution date"),
    ("cohort_survival", "GB", False, "Basic Company Data lists live companies only"),
    ("cohort_survival", "EE", False, "basic data lists registered companies only"),
    ("cohort_survival", "NO", False, "deleted units are not in the bulk file"),
    ("cohort_survival", "ES", False, "no registration dates"),
    ("fishing_hubs", "EE", False, "no activity code in the basic-data file"),
    ("employment_by_region_industry", "NO", True, "exact headcount"),
    ("employment_by_region_industry", "FR", True, "headcount bands"),
    ("employment_by_region_industry", "ES", True, "headcount bands"),
    ("employment_by_region_industry", "GB", False, "no headcount published"),
    ("employment_by_region_industry", "IE", False, "no headcount published"),
    ("employment_by_region_industry", "EE", False, "no headcount in basic data"),
    ("distress_by_industry", "FR", False, "only open/closed published"),
    ("distress_by_industry", "ES", False, "no status published"),
]


def insight_coverage(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(COVERAGE, "insight string, country_code string, included boolean, reason string")
