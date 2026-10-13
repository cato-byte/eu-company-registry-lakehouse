"""Gold insight tables, on small hand-built tables with known answers."""

from datetime import date

import pytest

from registry_lakehouse.gold import insights as ins

pytestmark = pytest.mark.spark

DIM = ("country_code string, company_key string, location_key string, is_active boolean, has_street_address boolean, "
       "address_line string, postal_code string, city string, is_dormant boolean, pending_strike_off boolean, "
       "nace_section string, nace_division string, latitude double, longitude double, status string, "
       "employees_min int, employees_max int, region string")


def row(cc, key, loc="x", active=True, street=True, city="TOWN", dormant=None, strike=None, section="G",
        division="47", status="ACTIVE", emin=None, emax=None, region="R"):
    return (cc, key, loc, active, street, "1 HIGH ST" if street else None, "PC", city, dormant, strike, section,
            division, 50.0, 1.0, status, emin, emax, region)


@pytest.fixture
def dim(spark):
    rows = [
        # a formation-agent address in London: 4 companies, 2 dormant, 1 being struck off
        *[row("GB", f"agent{i}", "agent", city="LONDON", dormant=i < 2, strike=i == 3) for i in range(4)],
        row("GB", "real1", "a1", city="LEEDS", dormant=False, strike=False),
        row("GB", "real2", "a2", city="LEEDS", dormant=False, strike=False),
        # sole traders share a postcode-only location: never a hotspot
        *[row("NO", f"enk{i}", "pc-only", street=False, section="A", division="03", city="VARDO") for i in range(5)],
        row("NO", "shop", "s1", city="OSLO", emin=12, emax=12),
        row("NO", "fishco", "f1", city="VARDO", section="A", division="03", emin=20, emax=20),
        row("NO", "bankrupt", "b1", city="OSLO", status="INSOLVENCY", active=False),
        row("FR", "fr1", "f", city="PARIS", emin=10, emax=19),
        row("FR", "fr2", "g", city="PARIS", emin=250, emax=None),
    ]
    return spark.createDataFrame(rows, DIM)


def test_mass_addresses_and_operating_flags(spark, dim):
    hot = ins.mass_addresses(dim, min_companies=3)
    rows = hot.collect()
    assert [(r.country_code, r.city, r.companies, r.dormant_companies, r.pending_strike_off) for r in rows] == [
        ("GB", "LONDON", 4, 2, 1)]
    flagged = {r.company_key: (r.at_mass_address, r.is_operating) for r in ins.flag_operating(dim, hot).collect()}
    assert flagged["agent0"] == (True, False) and flagged["real1"] == (False, True)
    assert flagged["enk0"] == (False, True)  # no street: cannot be a mass address
    assert flagged["bankrupt"] == (False, False)


def test_activity_summary_keeps_unknown_as_null(spark, dim):
    flagged = ins.flag_operating(dim, ins.mass_addresses(dim, 3))
    s = {r.country_code: r for r in ins.company_activity_summary(flagged).collect()}
    assert s["GB"].active_companies == 6 and s["GB"].dormant == 2 and s["GB"].at_mass_addresses == 4
    assert s["GB"].operating_estimate == 2 and s["GB"].operating_share == pytest.approx(0.3333)
    assert s["NO"].dormant is None  # not published in Norway: NULL, not zero


def test_fishing_hubs_location_quotient(spark, dim):
    towns = spark.createDataFrame([("NO", "VARDO", 2000)], "country_code string, place_key string, population long")
    hubs = {r.city: r for r in ins.fishing_hubs(dim, towns, min_fishing=3).collect()}
    vardo = hubs["VARDO"]
    # Norway: 6 fishing of 7 active (0.857). Vardø: 6 of 6 -> LQ = 1 / 0.857 = 1.17
    assert vardo.fishing_companies == 6 and vardo.location_quotient == pytest.approx(1.17)
    assert vardo.fishing_per_10k_inhabitants == pytest.approx(30.0)
    assert "PARIS" not in hubs and "LEEDS" not in hubs  # no fishing in those countries -> excluded


def test_employment_bounds(spark, dim):
    e = {(r.country_code, r.nace_division): r for r in ins.employment_by_region_industry(dim).collect()}
    assert (e[("NO", "47")].employees_low, e[("NO", "47")].employees_high, e[("NO", "47")].has_exact_counts) == (
        12, 12, True)
    fr = e[("FR", "47")]
    assert (fr.employees_low, fr.employees_high, fr.has_exact_counts) == (260, 269, False)  # 250+ counted at 250
    assert not any(cc == "GB" for cc, _ in e)


def test_distress_rates_only_where_vocabulary_exists(spark, dim):
    d = {(r.country_code, r.nace_section): r for r in ins.distress_by_industry(dim).collect()}
    assert d[("NO", "G")].insolvent == 1 and d[("NO", "G")].distress_rate == pytest.approx(0.5)
    assert d[("GB", "G")].pending_strike_off == 1
    assert not any(cc == "FR" for cc, _ in d)


HIST = ("company_key string, country_code string, nace_section string, registration_date date, closure_date date, "
        "valid_from date, valid_to date, is_current boolean")


def test_demography_births_deaths_and_panel_exits(spark):
    rows = [
        ("ie1", "IE", "G", date(2015, 3, 1), date(2018, 6, 1), date(2026, 7, 1), None, True),
        ("ie2", "IE", "G", date(2015, 5, 1), None, date(2026, 7, 1), None, True),
        # Navarra: no dates. es1 in both years, es2 only 2021 (exit), es3 enters 2022 (birth)
        ("es1", "ES", "C", None, None, date(2021, 12, 31), None, True),
        ("es2", "ES", "C", None, None, date(2021, 12, 31), date(2022, 12, 31), False),
        ("es3", "ES", "C", None, None, date(2022, 12, 31), None, True),
    ]
    out = {(r.country_code, r.year): (r.births, r.deaths) for r in
           ins.company_demography(spark.createDataFrame(rows, HIST), date(2026, 7, 31)).collect()}
    assert out[("IE", 2015)] == (2, 0) and out[("IE", 2018)] == (0, 1)
    assert out[("ES", 2022)] == (1, 1)  # es3 born, es2 left the register
    assert ("ES", 2021) not in out  # the first panel year is starting stock, not births


def test_cohort_survival_counts_only_observable_years(spark):
    cur = spark.createDataFrame([
        ("IE", "G", date(2015, 1, 1), date(2016, 6, 1)),   # died after 1.5 years
        ("IE", "G", date(2015, 1, 1), None),
        ("IE", "G", date(2024, 1, 1), None),               # too young for 3y/5y/10y
        ("GB", "G", date(2015, 1, 1), None),               # live-only register: excluded
    ], "country_code string, nace_section string, registration_date date, closure_date date")
    rows = {(r.country_code, r.cohort_year, r.nace_section): r for r in
            ins.cohort_survival(cur, date(2026, 7, 31)).collect()}
    c2015 = rows[("IE", 2015, "ALL")]
    assert (c2015.survival_1y, c2015.survival_3y, c2015.survival_10y) == (1.0, 0.5, 0.5)
    assert rows[("IE", 2024, "ALL")].survival_3y is None
    assert not any(k[0] == "GB" for k in rows)


def test_coverage_table_is_consistent():
    keys = [(i, c) for i, c, _, _ in ins.COVERAGE]
    assert len(keys) == len(set(keys))
    excluded_survival = {c for i, c, inc, _ in ins.COVERAGE if i == "cohort_survival" and not inc}
    assert excluded_survival.isdisjoint(ins.KEEPS_CLOSED_UNITS)
