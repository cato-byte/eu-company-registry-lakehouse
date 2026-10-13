"""Gold aggregates: fishing view, town/industry counts, privacy guard in the dimension."""

import pytest

from registry_lakehouse.gold.build import agg_city_industry, dim_company, dim_industry, fishing_by_city

pytestmark = pytest.mark.spark

DIM_COLS = ("country_code string, city string, nace_code string, nace_division string, nace_section string, "
            "section_label string, is_active boolean, latitude double, longitude double, employees_min int")


@pytest.fixture
def dim(spark):
    rows = [
        ("NO", "SANDNES", "03.11", "03", "A", "Agri", True, 58.85, 5.73, 3),
        ("NO", "SANDNES", "03.21", "03", "A", "Agri", True, 58.87, 5.75, 1),
        ("NO", "SANDNES", "03.11", "03", "A", "Agri", False, 58.85, 5.73, 0),  # inactive
        ("NO", "SANDNES", "47.11", "47", "G", "Trade", True, 58.85, 5.73, 2),
        ("GB", "FRASERBURGH", "03.11", "03", "A", "Agri", True, 57.69, -2.01, None),
        ("ES", "PAMPLONA", "35.18", "35", "D", "Energy", True, 42.81, -1.65, 0),  # electricity, not fishing
    ]
    return spark.createDataFrame(rows, DIM_COLS)


def test_fishing_by_city_counts_only_active_division_03(dim):
    out = {(r.country_code, r.city): r for r in fishing_by_city(dim).collect()}
    assert set(out) == {("NO", "SANDNES"), ("GB", "FRASERBURGH")}
    sandnes = out[("NO", "SANDNES")]
    assert sandnes.fishing_companies == 2
    assert sandnes.marine_fishing == 1 and sandnes.marine_aquaculture == 1


def test_city_industry_aggregates_active_companies(dim):
    out = {(r.city, r.nace_section): r.active_companies for r in agg_city_industry(dim).collect()}
    assert out[("SANDNES", "A")] == 2 and out[("SANDNES", "G")] == 1


def test_dim_company_never_exposes_natural_person_names(spark):
    company = spark.createDataFrame(
        [("k1", "loc", "EE", "LEGAL_ENTITY", "R", "1", "LEAKED NAME", True, "SOLE_TRADER", "ACTIVE", True, None,
          None, None, None, None, None, None, None, None, None)],
        "company_key string, location_key string, country_code string, grain string, source_register string, "
        "business_id string, company_name string, is_natural_person boolean, legal_form_category string, "
        "status string, is_active boolean, registration_date date, nace_code string, nace_division string, "
        "nace_section string, employees_min int, employees_max int, postal_code string, city string, "
        "region string, valid_from date")
    location = spark.createDataFrame([("loc", 59.4, 24.7, "CITY", "GEONAMES_PLACE")],
                                     "location_key string, latitude double, longitude double, geo_precision string, "
                                     "geo_source string")
    row = dim_company(company, location, dim_industry(spark)).first()
    assert row.company_name is None and row.latitude == 59.4
