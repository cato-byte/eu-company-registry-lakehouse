"""Parsing of dates, numbers, codes and text, with cases taken from the real registers.

Each parametrised case is a value actually observed in a landing file (see
docs/data-quality.md). Everything runs with ANSI mode on, like Databricks serverless:
a parser that throws instead of returning NULL fails these tests.
"""

from datetime import date

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from registry_lakehouse.silver import parsers as p

pytestmark = pytest.mark.spark


# ---------------------------------------------------------------------------- text
def test_clean_text_maps_register_sentinels_to_null(eval_col):
    values = ["  Normal ", "[ND]", "None Supplied", "********NO ADDRESS DETAILS*******", "", "  a   b ", None, ". ."]
    assert eval_col(p.clean_text, values) == ["Normal", None, None, None, None, "a b", None, None]


def test_mojibake_is_repaired_without_ever_throwing(eval_col):
    values = ["TravesÃ\xada", "1Âº  A", "SÃO PAULO", "Ã©t€", "PolÃ\xadgono Â", "ÁRBOL", "Ã"]
    assert eval_col(p.repair_mojibake, values) == [
        "Travesía", "1º  A", "SÃO PAULO", "Ã©t€", "PolÃ\xadgono Â", "ÁRBOL", "Ã"]


def test_split_part(eval_col):
    out = eval_col(lambda c: p.split_part(c, r"\s*/\s*", 0), ["Pamplona / Iruña", "Tudela", None])
    assert out == ["Pamplona", "Tudela", None]


# ---------------------------------------------------------------------------- dates
@pytest.mark.parametrize(
    ("raw", "formats", "expected"),
    [
        ("11/09/2012", ["dd/MM/yyyy"], date(2012, 9, 11)),  # Companies House
        ("05.06.2023", ["dd.MM.yyyy"], date(2023, 6, 5)),  # RIK
        ("2010-07-03", ["yyyy-MM-dd"], date(2010, 7, 3)),  # INSEE, CRO, Brreg
        ("1900-01-01", ["yyyy-MM-dd"], None),  # INSEE "unknown" sentinel
        ("31/02/2020", ["dd/MM/yyyy"], None),  # impossible date
        ("01/01/2099", ["dd/MM/yyyy"], None),  # after the snapshot date
        ("1066-10-14", ["yyyy-MM-dd"], None),  # implausibly old
        ("2012-09-11", ["dd/MM/yyyy"], None),  # wrong format for the source -> NULL, never a crash
        ("", ["yyyy-MM-dd"], None),
        ("garbage", ["yyyy-MM-dd"], None),
    ],
)
def test_parse_date(eval_col, raw, formats, expected):
    assert eval_col(lambda c: p.parse_date(c, formats, as_of=date(2026, 7, 31)), [raw]) == [expected]


# ---------------------------------------------------------------------------- numbers
def test_parse_int_handles_float_exports_and_junk(eval_col):
    values = ["5.0", "12", " 1 234 ", "7.5", "abc", "", None, "-3", "1e3"]
    assert eval_col(p.parse_int, values) == [5, 12, 1234, None, None, None, None, -3, 1000]


def test_parse_decimal_accepts_comma_decimal(eval_col):
    assert eval_col(p.parse_decimal, ["851150.0982", "12,5", "[ND]", "1.2.3"]) == [851150.0982, 12.5, None, None]


def test_parse_bool_multilingual(eval_col):
    assert eval_col(p.parse_bool, ["true", "False", "ja", "nei", "1", "maybe", None]) == [
        True, False, True, False, True, None, None]


# ---------------------------------------------------------------------------- NACE
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("311.0", "03.11"),  # Navarra / CRO: fishing that lost its leading zero
        ("3518", "35.18"),  # electricity: must NOT become fishing
        ("3518.0", "35.18"),
        ("6420.0", "64.20"),
        ("111.0", "01.11"),
        ("99", None),  # too short to be a class
        ("12345", None),
        ("", None),
        ("abc", None),
    ],
)
def test_nace_from_numeric(eval_col, raw, expected):
    assert eval_col(p.nace_from_numeric, [raw]) == [expected]


def test_numeric_code_is_format_stable(eval_col):
    assert eval_col(p.numeric_code, ["4782.0", "4782", "311.0", "0311", "x", None]) == [
        "4782", "4782", "0311", "0311", None, None]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("03.11Z", "03.11"), ("03.110", "03.11"), ("43.210", "43.21"), ("70.2C", None), ("00.000", None),
     ("0311", "03.11"), ("", None)],
)
def test_nace_from_dotted(eval_col, raw, expected):
    assert eval_col(p.nace_from_dotted, [raw]) == [expected]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("03110 - Marine fishing", "03.11"), ("62020 - Information technology consultancy activities", "62.02"),
     ("99999 - Dormant Company", None), ("74990 - Non-trading company", None), ("None Supplied", None)],
)
def test_nace_from_uk_sic(eval_col, raw, expected):
    assert eval_col(p.nace_from_uk_sic, [raw]) == [expected]


# ---------------------------------------------------------------------------- postcodes
@pytest.mark.parametrize(
    ("country", "raw", "expected"),
    [
        ("GB", "ec1v2nx", "EC1V 2NX"), ("GB", "HG1 1ND", "HG1 1ND"), ("GB", "W1W 7LT", "W1W 7LT"),
        ("GB", "NOT A PC", None),
        ("IE", "D02Y512", "D02 Y512"), ("IE", "d16 a4w6", "D16 A4W6"), ("IE", "D6W1234", "D6W 1234"),
        ("IE", "B12 3456", None),  # 'B' is not a valid routing-key letter
        ("FR", "1000", "01000"), ("FR", "84000", "84000"), ("FR", "123456", None),
        ("ES", "0", None), ("ES", "31007", "31007"), ("ES", "31007.0", "31007"),
        ("NO", "4307", "4307"), ("EE", "11911", "11911"),
    ],
)
def test_postal_codes(eval_col, country, raw, expected):
    assert eval_col(lambda c: p.postal_code(c, country), [raw]) == [expected]


# ---------------------------------------------------------------------------- identifiers
def py_luhn(s: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(s)):
        d = int(ch)
        if i % 2 == 1:
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        total += d
    return total % 10 == 0


def py_no_orgnr(s: str) -> bool:
    r = 11 - sum(int(a) * w for a, w in zip(s[:8], [3, 2, 7, 6, 5, 4, 3, 2], strict=True)) % 11
    r = 0 if r == 11 else r
    return r != 10 and r == int(s[8])


def py_ee_code(s: str) -> bool:
    d = [int(c) for c in s]
    r = sum(a * b for a, b in zip(d[:7], [1, 2, 3, 4, 5, 6, 7], strict=True)) % 11
    if r == 10:
        r = sum(a * b for a, b in zip(d[:7], [3, 4, 5, 6, 7, 8, 9], strict=True)) % 11
        r = 0 if r == 10 else r
    return r == d[7]


def test_checksums_on_known_real_world_format_examples(eval_col):
    # Public, well-known identifiers (INSEE's own SIREN/SIRET, Brreg's own orgnr).
    assert eval_col(lambda c: p.luhn_valid(c, 9), ["120027016", "120027017"]) == [True, False]
    # La Poste establishments (SIREN 356000000) use a digit-sum mod 5 rule instead of Luhn.
    assert eval_col(p.siret_valid, ["12002701600357", "35600000000051", "35600000000052"]) == [True, True, False]
    assert eval_col(p.no_orgnr_valid, ["974760673", "974760674", "97476067"]) == [True, False, False]


@settings(max_examples=25, deadline=None)
@given(st.lists(st.text(alphabet="0123456789", min_size=9, max_size=9), min_size=50, max_size=50))
def test_spark_checksum_expressions_match_python_reference(spark, values):
    """Property: the Spark (no-UDF) checksum expressions agree with straightforward Python."""
    df = spark.createDataFrame([(v, v[:8]) for v in values], "v9 string, v8 string")
    rows = df.select(
        "v9", "v8",
        p.luhn_valid("v9", 9).alias("luhn"),
        p.no_orgnr_valid("v9").alias("no"),
        p.ee_registry_code_valid("v8").alias("ee"),
    ).collect()
    for r in rows:
        assert r.luhn == py_luhn(r.v9), r.v9
        assert r.no == py_no_orgnr(r.v9), r.v9
        assert r.ee == py_ee_code(r.v8), r.v8


def test_spanish_tax_id_kind(eval_col):
    assert eval_col(p.es_tax_id_kind, ["072677139", "X1234567L", "B31234567", "A3123456J", "??"]) == [
        "NATURAL_PERSON", "NATURAL_PERSON", "LEGAL_ENTITY", "LEGAL_ENTITY", "UNKNOWN"]


def test_gb_company_numbers(eval_col):
    assert eval_col(p.gb_company_number_valid, ["08209948", "SC257245", "NI123456", "1234567", "OC12345X"]) == [
        True, True, True, False, False]
