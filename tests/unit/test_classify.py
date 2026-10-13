"""Shared vocabularies: status, legal form, NACE hierarchy."""

import pytest
from pyspark.sql import functions as F

from registry_lakehouse.silver.classify import legal_form_category, nace_divisions, nace_section, status, valid_nace

pytestmark = pytest.mark.spark


@pytest.mark.parametrize(
    ("country", "raw", "expected"),
    [
        ("GB", "Active", "ACTIVE"),
        ("GB", "Active - Proposal to Strike off", "ACTIVE"),
        ("GB", "Liquidation", "LIQUIDATION"),
        ("GB", "In Administration/Administrative Receiver", "INSOLVENCY"),
        ("GB", "Live but Receiver Manager on at least one charge", "INSOLVENCY"),
        ("GB", "Something New", "UNKNOWN"),
        ("IE", "Normal", "ACTIVE"),
        ("IE", "Strike Off Listed", "ACTIVE"),
        ("IE", "Dissolved-20 years", "CLOSED"),
        ("IE", "Deleted CB Merger", "CLOSED"),
        ("IE", "Liquidation (UK)", "LIQUIDATION"),
        ("IE", "Examinership", "INSOLVENCY"),
        ("FR", "A", "ACTIVE"),
        ("FR", "F", "CLOSED"),
        ("EE", "R", "ACTIVE"),
        ("EE", "L", "LIQUIDATION"),
        ("EE", "N", "INSOLVENCY"),
        ("NO", "CLOSED", "CLOSED"),
        ("NO", "bogus", "UNKNOWN"),
    ],
)
def test_status(eval_col, country, raw, expected):
    assert eval_col(lambda c: status(F.col(c), country), [raw]) == [expected]


@pytest.mark.parametrize(
    ("country", "raw", "expected"),
    [
        ("GB", "Private Limited Company", "LIMITED_COMPANY"),
        ("GB", "PRI/LTD BY GUAR/NSC (Private, limited by guarantee, no share capital)", "NON_PROFIT"),
        ("GB", "Limited Liability Partnership", "PARTNERSHIP"),
        ("GB", "Charitable Incorporated Organisation", "NON_PROFIT"),
        ("IE", "LTD - Private Company Limited by Shares", "LIMITED_COMPANY"),
        ("IE", "CLG - Company Limited by Guarantee", "NON_PROFIT"),
        ("EE", "Osaühing", "LIMITED_COMPANY"),
        ("EE", "Füüsilisest isikust ettevõtja", "SOLE_TRADER"),
        ("EE", "Korteriühistu", "COOPERATIVE"),
        ("EE", "Välismaa äriühingu filiaal", "BRANCH"),
        ("NO", "ENK", "SOLE_TRADER"),
        ("NO", "AS", "LIMITED_COMPANY"),
        ("NO", "SA", "COOPERATIVE"),  # samvirkeforetak, not a "société anonyme"
        ("NO", "NUF", "BRANCH"),
        ("ES", "NATURAL_PERSON", "SOLE_TRADER"),
        ("ES", "B31234567", "LIMITED_COMPANY"),
        ("ES", "F31234567", "COOPERATIVE"),
        ("ES", "G31234567", "NON_PROFIT"),
    ],
)
def test_legal_form(eval_col, country, raw, expected):
    assert eval_col(lambda c: legal_form_category(F.col(c), country), [raw]) == [expected]


def test_nace_reference_is_complete():
    divisions = {r["division"]: r["section"] for r in nace_divisions()}
    assert len(divisions) == 88
    assert divisions["03"] == "A" and divisions["35"] == "D" and divisions["99"] == "U"
    assert "04" not in divisions  # NACE Rev. 2 has no division 04


def test_nace_section_and_validation(eval_col):
    assert eval_col(lambda c: nace_section(F.col(c)), ["03", "47", "04", None]) == ["A", "G", None, None]
    assert eval_col(lambda c: valid_nace(F.col(c)), ["03.11", "04.11", "99.00"]) == ["03.11", None, "99.00"]
