"""Per-register mapping, run on the synthetic fixtures through the real bronze loader.

Each assertion pins a data-cleaning decision for a real quirk of that register.
"""

from datetime import date

import pytest
from pyspark.sql import functions as F

from registry_lakehouse.bronze.load import bronze_table_name, load_bronze
from registry_lakehouse.silver.build import canonical_snapshot, deduplicate
from registry_lakehouse.silver.canonical import CANONICAL_ORDER

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def bronze(spark, tmp_path_factory):
    from registry_lakehouse.config import Settings
    from tests.conftest import LANDING

    s = Settings(landing_root=str(LANDING), bronze_schema="src_bronze", snapshot_date=date(2026, 7, 31),
                 run_id="test-sources")
    results = {cc: load_bronze(spark, s, cc) for cc in ["FR", "GB", "IE", "EE", "ES", "NO"]}
    return {cc: spark.table(bronze_table_name(s, cc)) for cc in results}, results


def canon(bronze, cc, snap=date(2026, 7, 1)):
    tables, _ = bronze
    return canonical_snapshot(tables[cc].where(F.col("_corrupt_record").isNull()), cc, snap)


# ---------------------------------------------------------------------------- bronze
def test_bronze_keeps_everything_as_string_with_lineage(bronze):
    tables, results = bronze
    for cc, df in tables.items():
        typed = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        raw_cols = [c for c in typed if not c.startswith("_")]
        assert all(typed[c] == "string" for c in raw_cols), cc
        assert {"_source_file", "_snapshot_date", "_ingested_at", "_rescued_data", "_corrupt_record"} <= set(typed)
        assert results[cc].rows > 0


def test_bronze_captures_malformed_lines_instead_of_shifting_columns(bronze):
    tables, results = bronze
    assert results["IE"].corrupt_rows == 1
    good = tables["IE"].where(F.col("_corrupt_record").isNull())
    # every parsed company number is numeric: nothing shifted into the wrong column
    assert good.where(~F.col("company_num").rlike(r"^\d+$")).count() == 0


def test_bronze_drops_pii_and_rescues_unexpected_keys(bronze):
    tables, results = bronze
    no = tables["NO"]
    assert not {"epostadresse", "telefon", "mobil", "hjemmeside"} & set(no.columns)
    assert results["NO"].drift.added == ("somethingNew",)
    assert no.where(F.col("_rescued_data").contains("unexpected key")).count() == 1


def test_bronze_handles_bom_crlf_and_semicolons(bronze):
    tables, _ = bronze
    ee = tables["EE"]
    assert "nimi" in ee.columns  # BOM did not end up in the first column name
    assert ee.where(F.col("teabesysteemi_link").endswith("\r")).count() == 0  # CRLF stripped


def test_bronze_is_idempotent(spark, bronze):
    from registry_lakehouse.config import Settings
    from tests.conftest import LANDING

    tables, _ = bronze
    before = tables["GB"].count()
    s = Settings(landing_root=str(LANDING), bronze_schema="src_bronze", snapshot_date=date(2026, 7, 31))
    load_bronze(spark, s, "GB")
    assert spark.table(bronze_table_name(s, "GB")).count() == before


# ---------------------------------------------------------------------------- canonical shape
@pytest.mark.parametrize("cc", ["FR", "GB", "IE", "EE", "NO"])
def test_canonical_columns_and_unique_keys(bronze, cc):
    df = canon(bronze, cc)
    assert df.columns == CANONICAL_ORDER
    deduped = deduplicate(df)
    assert deduped.count() == deduped.select("company_key").distinct().count()


def test_natural_persons_never_carry_name_or_street(bronze):
    for cc, snap in [("EE", date(2026, 7, 1)), ("NO", date(2026, 7, 1)), ("ES", date(2022, 12, 31))]:
        persons = canon(bronze, cc, snap).where("is_natural_person")
        assert persons.count() > 0, cc
        assert persons.where("company_name IS NOT NULL OR address_line IS NOT NULL").count() == 0, cc


# ---------------------------------------------------------------------------- per register
def test_france(bronze):
    df = canon(bronze, "FR")
    fishing = df.where("nace_division = '03'")
    assert fishing.count() >= 5
    # old nomenclatures (NAP, NAF1993) are kept as local codes but never mapped to NACE
    assert df.where("industry_scheme IN ('NAP','NAF1993') AND nace_code IS NOT NULL").count() == 0
    assert df.where("industry_scheme IN ('NAP','NAF1993')").count() >= 2
    # non-diffusible establishments: no street, treated as personal
    assert df.where("is_natural_person AND address_line IS NOT NULL").count() == 0
    # overseas departments never get Lambert-93 coordinates
    assert df.where("municipality_code RLIKE '^(97|98)' AND source_x IS NOT NULL").count() == 0
    # arrondissements collapse to the city
    assert df.where("city RLIKE '^PARIS [0-9]'").count() == 0
    # 1900-01-01 is "unknown", not a date
    assert df.where("registration_date = DATE'1900-01-01'").count() == 0
    # the deliberately broken SIRET is flagged
    assert df.where("id_checksum_valid = false").count() == 1


def test_united_kingdom(bronze):
    df = canon(bronze, "GB")
    assert df.where("nace_code = '03.11'").count() >= 1
    assert df.where("industry_code_local LIKE '99999%' AND nace_code IS NOT NULL").count() == 0
    assert df.where("postal_code IS NOT NULL AND postal_code NOT RLIKE '^[A-Z0-9]{2,4} [0-9][A-Z]{2}$'").count() == 0
    assert df.where("registration_date > DATE'2026-07-01'").count() == 0  # 01/01/2099 rejected
    assert df.where("status = 'UNKNOWN'").count() == 0


def test_ireland(bronze):
    df = canon(bronze, "IE")
    # '311.0' -> 03.11 and friends; '3511.0' (electricity) stays out of fishing
    fishing = {r.nace_code for r in df.where("nace_division = '03'").collect()}
    assert fishing <= {"03.11", "03.12", "03.21", "03.22"} and fishing
    assert df.where("industry_code_local = '3511.0' AND nace_division = '03'").count() == 0
    # town comes from the address lines, not a hard-coded DUBLIN
    assert df.select("city").distinct().count() > 3
    assert df.where("city LIKE 'DUBLIN %'").count() == 0
    # trailing-space 'Normal ' is ACTIVE; the CRO placeholder address is NULL
    assert df.where("status_local = 'Normal' AND status = 'ACTIVE'").count() > 0
    assert df.where("address_line LIKE '%NO ADDRESS%'").count() == 0
    # NACE Rev.1 codes are kept locally but not mapped
    assert df.where("industry_scheme = 'NACE_REV1' AND nace_code IS NOT NULL").count() == 0


def test_estonia(bronze):
    df = canon(bronze, "EE")
    # the street is in asukoht_ettevotja_aadressis (ettevotja_aadress is always empty)
    assert df.where("NOT is_natural_person AND address_line IS NOT NULL").count() > 10
    assert df.where("city = 'TALLINN'").count() >= 4  # district names roll up to the city
    assert df.where("status IN ('LIQUIDATION','INSOLVENCY')").count() >= 3
    assert df.where("id_checksum_valid").count() == df.count()
    # the duplicated fixture row collapses to one company
    assert df.count() - deduplicate(df).count() == 1


def test_spain_two_panel_years_and_float_codes(bronze):
    y21 = canon(bronze, "ES", date(2021, 12, 31))
    y22 = canon(bronze, "ES", date(2022, 12, 31))
    assert y21.count() > 0 and y22.count() > 0
    # Navarra is landlocked: its only "fishing" units are trout farms, '322.0' -> 03.22
    assert y21.where("nace_code = '03.22'").count() >= 1
    assert y21.where("industry_code_local = '3518' AND nace_code = '35.18'").count() >= 1
    assert y21.where("postal_code = '0'").count() == 0
    big = y21.where("employees_min = 250").collect()
    assert all(r.employees_max is None for r in big)  # '250 o más' is open-ended
    # personal tax ids are pseudonymised: 64-hex keyed hashes, never the DNI
    persons = y21.where("is_natural_person").collect()
    assert persons and all(len(r.business_id) == 64 for r in persons)
    # mojibake repaired: no 'Ã' sequences survive in silver
    assert y22.where("address_line RLIKE '[ÂÃ][\\\\x80-\\\\xBF]'").count() == 0


def test_norway(bronze):
    df = canon(bronze, "NO")
    assert df.where("nace_code = '03.11'").count() >= 3  # '03.110' -> 03.11
    assert df.where("status = 'INSOLVENCY'").count() >= 2
    assert df.where("status = 'LIQUIDATION'").count() >= 2
    assert df.where("status = 'CLOSED'").count() == 1  # slettedato
    assert df.where("address_country <> 'NO'").count() >= 1  # foreign addresses kept but flagged
    assert df.where("id_checksum_valid").count() == df.count()
    assert df.where("vat_number IS NOT NULL AND vat_number NOT RLIKE '^NO[0-9]{9}MVA$'").count() == 0


def test_latest_reference_file_per_source(spark, tmp_path):
    from registry_lakehouse.bronze.landing import latest_reference_files

    for rel in ["REF/FR/ingest_date=2026-08-01/FR.txt", "REF/FR/ingest_date=2026-09-01/FR.txt",
                "REF/FR/ingest_date=2026-09-01/readme.txt", "REF/GB/ingest_date=2026-07-01/GB_full.txt"]:
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    files = latest_reference_files(spark, str(tmp_path), "REF")
    assert [f.split(str(tmp_path))[-1] for f in files] == [
        "/REF/FR/ingest_date=2026-09-01/FR.txt", "/REF/GB/ingest_date=2026-07-01/GB_full.txt"]


def test_spain_year_without_municipality_keeps_identity_and_town(spark):
    """Navarra's 2020 release has no municipality (code and name empty) in 100% of rows."""
    from registry_lakehouse.contracts import get_contract

    cols = list(get_contract("ES").bronze_columns)

    def row(year, cod, name):
        r = dict.fromkeys(cols, None)
        r.update({"_id": "1", "anio": str(year), "dnici": "B31234567", "nombre": "EXAMPLE SL", "msiglat": "Calle",
                  "callet": "MAYOR", "portalt": "5.0", "codpost": "31001", "cod_cmun": cod, "nombre_cmun": name,
                  "tramo_asal": "Ninguna", "cnae09_ppal": "4711.0", "cnae09_local": "4711.0"})
        return r

    bronze = spark.createDataFrame([row(2019, "201", "Pamplona / Iruña"), row(2020, "", "")],
                                   ", ".join(f"`{c}` string" for c in cols))
    y19 = canonical_snapshot(bronze, "ES", date(2019, 12, 31)).first()
    y20 = canonical_snapshot(bronze, "ES", date(2020, 12, 31)).first()
    assert y20.company_key == y19.company_key  # same establishment, no spurious SCD2 version
    assert (y20.city, y20.municipality_code) == ("PAMPLONA", "31201")  # filled from the other year
    assert y20.hash_diff == y19.hash_diff
