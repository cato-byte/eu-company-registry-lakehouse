"""Harmonised vocabularies: NACE sections, company status, legal-form category.

Every register uses its own words ("Normal ", "Registrisse kantud", "Active - Proposal to
Strike off", konkurs=true, ...). Each source maps to one small shared vocabulary, using ordered
regex rules (first match wins). The rules are data, so the tests can cover them one by one.
"""

from __future__ import annotations

import csv
from functools import cache
from importlib import resources

from pyspark.sql import Column
from pyspark.sql import functions as F

STATUSES = ("ACTIVE", "LIQUIDATION", "INSOLVENCY", "CLOSED", "UNKNOWN")
LEGAL_FORM_CATEGORIES = (
    "LIMITED_COMPANY", "SOLE_TRADER", "PARTNERSHIP", "COOPERATIVE", "NON_PROFIT",
    "PUBLIC_BODY", "BRANCH", "OTHER", "UNKNOWN",
)

Rules = list[tuple[str, str]]


def classify(col: Column, rules: Rules, default: str) -> Column:
    """Ordered, case-insensitive regex rules -> category."""
    expr = None
    for pattern, category in rules:
        cond = col.rlike(f"(?i){pattern}")
        expr = F.when(cond, F.lit(category)) if expr is None else expr.when(cond, F.lit(category))
    return F.lit(default) if expr is None else F.when(col.isNull(), F.lit("UNKNOWN")).otherwise(
        expr.otherwise(F.lit(default))
    )


# --------------------------------------------------------------------------------------
# Status
# --------------------------------------------------------------------------------------
STATUS_RULES: dict[str, Rules] = {
    "GB": [
        (r"^active", "ACTIVE"),  # includes "Active - Proposal to Strike off"
        (r"liquidation", "LIQUIDATION"),
        (r"administra|receiver|voluntary arrangement|insolvency", "INSOLVENCY"),
        (r"dissolved|closed|converted|removed", "CLOSED"),
        (r"^live", "ACTIVE"),
    ],
    "IE": [
        (r"^normal", "ACTIVE"),
        (r"strike off listed", "ACTIVE"),  # still registered until struck off
        (r"liquidation", "LIQUIDATION"),
        (r"examinership|receivership|administration", "INSOLVENCY"),
        (r"dissolved|struck off|ceased|deleted|de-registered|conv\. to", "CLOSED"),
    ],
    "FR": [(r"^A$", "ACTIVE"), (r"^F$", "CLOSED")],
    "EE": [(r"^R$", "ACTIVE"), (r"^L$", "LIQUIDATION"), (r"^N$", "INSOLVENCY"), (r"^K$", "CLOSED")],
    # Spain has no status column: an establishment is ACTIVE while it appears in the yearly
    # panel, and SCD2 closes it when it disappears. Norway derives status from flags (mapper).
    "ES": [(r"^ACTIVE$", "ACTIVE")],
}


def status(col: Column, country: str) -> Column:
    if country == "NO":
        return F.when(col.isin(*STATUSES), col).otherwise(F.lit("UNKNOWN"))
    return classify(col, STATUS_RULES[country], "UNKNOWN")


# --------------------------------------------------------------------------------------
# Legal form
# --------------------------------------------------------------------------------------
LEGAL_FORM_RULES: dict[str, Rules] = {
    "GB": [
        (r"limited liability partnership|limited partnership|scottish partnership", "PARTNERSHIP"),
        (r"guarant|charitable|\bCIO\b", "NON_PROFIT"),
        (r"public limited|private limited|community interest|european public", "LIMITED_COMPANY"),
        (r"society|industrial and provident", "COOPERATIVE"),
        (r"overseas|other company type.*branch", "BRANCH"),
    ],
    "IE": [
        (r"partnership", "PARTNERSHIP"),
        (r"guarantee|\bCLG\b", "NON_PROFIT"),
        (r"limited by shares|\bLTD\b|\bDAC\b|\bPLC\b|public limited|societas europaea", "LIMITED_COMPANY"),
        (r"society", "COOPERATIVE"),
        (r"external|branch|foreign", "BRANCH"),
    ],
    "EE": [
        (r"füüsilisest isikust", "SOLE_TRADER"),
        (r"^osaühing|^aktsiaselts|euroopa äriühing", "LIMITED_COMPANY"),
        (r"usaldusühing|täisühing|majandushuviühing", "PARTNERSHIP"),
        (r"ühistu", "COOPERATIVE"),
        (r"mittetulundus|sihtasutus|erakond|ametiühing|kogudus", "NON_PROFIT"),
        (r"filiaal", "BRANCH"),
        (r"asutus|avalik-õiguslik|riigi", "PUBLIC_BODY"),
    ],
    "NO": [  # organisasjonsform codes
        (r"^ENK$", "SOLE_TRADER"),
        (r"^(AS|ASA|SE)$", "LIMITED_COMPANY"),
        (r"^(ANS|DA|KS|IS)$", "PARTNERSHIP"),
        (r"^(SA|BRL|BA)$", "COOPERATIVE"),
        (r"^(FLI|STI|ORGL)$", "NON_PROFIT"),
        (r"^(NUF|UTLA)$", "BRANCH"),
        (r"^(KOMM|FYLK|STAT|SF|IKS|KF|FKF|KIRK|SÆR)$", "PUBLIC_BODY"),
    ],
    # Spain: the first character of the tax id encodes the legal form.
    "ES": [
        (r"^NATURAL_PERSON$", "SOLE_TRADER"),
        (r"^[AB]", "LIMITED_COMPANY"),
        (r"^[CDJE]", "PARTNERSHIP"),
        (r"^F", "COOPERATIVE"),
        (r"^[GR]", "NON_PROFIT"),
        (r"^[PQS]", "PUBLIC_BODY"),
        (r"^[NW]", "BRANCH"),
    ],
}


def legal_form_category(col: Column, country: str) -> Column:
    if country == "FR":  # not published in the establishment file
        return F.lit("UNKNOWN")
    return classify(col, LEGAL_FORM_RULES[country], "OTHER")


# --------------------------------------------------------------------------------------
# NACE
# --------------------------------------------------------------------------------------
@cache
def nace_divisions() -> list[dict[str, str]]:
    text = resources.files("registry_lakehouse.reference").joinpath("nace_rev2_divisions.csv").read_text("utf-8")
    return list(csv.DictReader(text.splitlines()))


def nace_section(division: Column) -> Column:
    """'03' -> 'A'. Unknown divisions (e.g. '00', '04') -> NULL, which flags an invalid code."""
    mapping = F.create_map(*[F.lit(x) for row in nace_divisions() for x in (row["division"], row["section"])])
    return mapping[division]


def valid_nace(nace_code: Column) -> Column:
    """Keep only codes whose division exists in NACE Rev. 2."""
    return F.when(nace_section(F.substring(nace_code, 1, 2)).isNotNull(), nace_code)
