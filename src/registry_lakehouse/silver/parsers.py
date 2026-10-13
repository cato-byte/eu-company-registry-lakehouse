"""Typed parsing of raw register strings: dates, numbers, codes, text.

Every function here returns a Spark Column built from native expressions, with no Python UDFs.
Under ANSI mode (the default on Databricks serverless) a plain ``cast`` throws on bad input and
kills the job, so all parsing goes through ``try_*`` functions. Unparseable values become NULL,
and the data-quality layer counts them.

Each quirk handled below was observed in the real landing files; see docs/data-quality.md.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from datetime import date

from pyspark.sql import Column
from pyspark.sql import functions as F

# Values registers use to mean "no value".
GLOBAL_SENTINELS: tuple[str, ...] = (
    "",
    "[ND]",  # INSEE: non-diffusible (the person opted out of publication)
    "NULL",
    "NONE",
    "N/A",
    "NONE SUPPLIED",  # Companies House SIC
    "********NO ADDRESS DETAILS*******",  # CRO
    ". .",
    "-",
)


def _c(col: str | Column) -> Column:
    return F.col(col) if isinstance(col, str) else col


def let(value: str | Column, body: Callable[[Column], Column]) -> Column:
    """Bind ``value`` once and use it many times inside ``body``.

    Column functions that mention their input several times (``when(x...).otherwise(x)``)
    copy the input's whole expression tree each time. Composed functions (clean -> parse ->
    validate) then grow exponentially: building the Spanish plan ran the driver out of memory.
    Routing the value through a one-element array and a lambda turns every use into a cheap
    variable reference, so the plan grows linearly.
    """
    return F.try_element_at(F.transform(F.array(_c(value)), body), F.lit(1))


# --------------------------------------------------------------------------------------
# Text
# --------------------------------------------------------------------------------------
# UTF-8 text that was decoded as Latin-1 somewhere upstream ('TravesÃ\xada' instead of 'Travesía').
# Navarra's 2022-2024 rows have it on about 16% of rows. If it is not repaired, the same
# establishment flips between two spellings and SCD2 opens a spurious version every year.
# The repair (encode Latin-1 -> decode UTF-8) THROWS in Spark 4 / serverless on any string it
# cannot round-trip ('SÃO PAULO', '€'), so it only runs on strings made entirely of ASCII
# and well-formed two-byte sequences, for which the round trip cannot fail.
_MOJIBAKE_SAFE = r"^(?:[\x00-\x7F]|[\xC2-\xDF][\x80-\xBF])*$"
_MOJIBAKE_HINT = r"[\xC2\xC3][\x80-\xBF]"


def repair_mojibake(col: str | Column) -> Column:
    def body(s: Column) -> Column:
        fixable = s.rlike(_MOJIBAKE_HINT) & s.rlike(_MOJIBAKE_SAFE)
        return F.when(fixable, F.decode(F.encode(s, "ISO-8859-1"), "UTF-8")).otherwise(s)

    return let(col, body)


def clean_text(col: str | Column, extra_sentinels: Iterable[str] = ()) -> Column:
    """Repair mojibake, trim, collapse inner whitespace, map sentinel placeholders to NULL."""
    sentinels = [s.upper() for s in (*GLOBAL_SENTINELS, *extra_sentinels)]

    def body(collapsed: Column) -> Column:
        return F.when(F.upper(collapsed).isin(sentinels) | collapsed.isNull(), F.lit(None)).otherwise(collapsed)

    return let(F.regexp_replace(F.trim(repair_mojibake(col)), r"\s+", " "), body)


def clean_upper(col: str | Column, extra_sentinels: Iterable[str] = ()) -> Column:
    return F.upper(clean_text(col, extra_sentinels))


def first_non_null(*cols: Column) -> Column:
    return F.coalesce(*cols)


def concat_clean(*cols: Column, sep: str = " ") -> Column:
    """Join non-null parts. Returns NULL instead of an empty string when all parts are NULL."""
    return let(F.trim(F.concat_ws(sep, *cols)), lambda j: F.when(F.length(j) == 0, F.lit(None)).otherwise(j))


def split_part(col: str | Column, sep_regex: str, index: int) -> Column:
    """0-based element of a regex split, trimmed; NULL if missing."""
    return clean_text(F.try_element_at(F.split(_c(col), sep_regex), F.lit(index + 1 if index >= 0 else index)))


# --------------------------------------------------------------------------------------
# Dates
# --------------------------------------------------------------------------------------
# INSEE uses 1900-01-01 as "creation date unknown".
DATE_SENTINELS: tuple[str, ...] = ("1900-01-01",)


def parse_date(
    col: str | Column,
    formats: Sequence[str],
    *,
    as_of: date | Column | None = None,
    min_date: date = date(1800, 1, 1),
    sentinels: Sequence[str] = DATE_SENTINELS,
) -> Column:
    """Parse with the source's explicit format(s). Values that are unparseable, sentinel,
    before ``min_date`` or after ``as_of`` (the snapshot date) become NULL."""
    upper = F.lit(as_of) if isinstance(as_of, date) else as_of

    def from_raw(raw: Column) -> Column:
        parsed = F.coalesce(*[F.to_date(F.try_to_timestamp(raw, F.lit(fmt))) for fmt in formats])
        return F.when(~raw.isin(list(sentinels)), parsed)

    def check_range(parsed: Column) -> Column:
        bad = parsed < F.lit(min_date)
        if upper is not None:
            bad = bad | (parsed > upper)
        return F.when(~bad, parsed)

    return let(let(clean_text(col), from_raw), check_range)


# --------------------------------------------------------------------------------------
# Numbers
# --------------------------------------------------------------------------------------
def parse_decimal(col: str | Column) -> Column:
    """'851150.0982' -> 851150.0982, '12,5' -> 12.5, '1 234' -> 1234.0, junk -> NULL."""
    compact = F.regexp_replace(clean_text(col), r"[\s\x{00A0}]", "")
    return let(compact, lambda s: F.when(s.rlike(r"^-?\d+,\d+$"), F.regexp_replace(s, ",", ".")).otherwise(s)
               .try_cast("double"))


def parse_int(col: str | Column) -> Column:
    """'5.0' -> 5 (registers exported via pandas turn ints into floats), '7,5' -> NULL."""
    return let(parse_decimal(col),
               lambda d: F.when(d == F.floor(d), d.try_cast("bigint")).otherwise(F.lit(None).cast("bigint")))


def parse_bool(col: str | Column) -> Column:
    s = F.lower(clean_text(col))
    return (
        F.when(s.isin("true", "t", "1", "yes", "y", "ja", "j", "oui"), F.lit(True))
        .when(s.isin("false", "f", "0", "no", "n", "nei", "non"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


# --------------------------------------------------------------------------------------
# Industry codes -> NACE Rev. 2 class ("03.11")
# --------------------------------------------------------------------------------------
def _nace_from_digits(digits: Column) -> Column:
    return let(digits, lambda d: F.when(
        d.rlike(r"^\d{4}$") & (d != "0000"),
        F.concat(F.substring(d, 1, 2), F.lit("."), F.substring(d, 3, 2)),
    ))


def numeric_code(col: str | Column, width: int = 4) -> Column:
    """Canonical text of a numeric code: '311.0', '311', '0311' -> '0311'.

    Navarra exported the same CNAE as '4782.0' in 2021 and '4782' in 2022. Storing the raw text
    would make every establishment look changed and open spurious SCD2 versions.
    """
    return let(parse_int(col), lambda n: F.when(n.between(0, 10**width - 1), F.lpad(n.cast("string"), width, "0")))


def nace_from_numeric(col: str | Column) -> Column:
    """Float-mangled codes (Navarra CNAE-2009, CRO): '311.0' -> '03.11', '3518' -> '35.18'.

    The leading zero is lost when the publisher stored the code as a number. Without the
    left-pad, fishing (03.11) would be read as 31.1 (furniture) and electricity 35.18 would look
    like fishing. NACE classes have exactly four digits.
    """
    return let(parse_int(col),
               lambda n: F.when(n.between(100, 9999), _nace_from_digits(F.lpad(n.cast("string"), 4, "0"))))


def nace_from_dotted(col: str | Column) -> Column:
    """France NAF rev.2 '03.11Z', Norway SN2007 '03.110', NACE '03.11' -> '03.11'.

    Norway's '00.000' (unknown) and anything not matching the pattern -> NULL.
    """
    s = clean_text(col)
    digits = F.regexp_extract(s, r"^(\d{2})\.?(\d{2})", 0)
    return _nace_from_digits(F.regexp_replace(digits, r"\.", ""))


def nace_from_uk_sic(col: str | Column) -> Column:
    """'03110 - Marine fishing' -> '03.11'. UK SIC 2007 refines NACE Rev. 2 with a 5th digit.

    '99999 - Dormant Company' and '74990 - Non-trading company' are UK-only placeholders.
    """
    code = F.regexp_extract(clean_text(col), r"^(\d{5})\b", 1)
    return let(code, lambda c: F.when(~c.isin("99999", "74990", ""), _nace_from_digits(F.substring(c, 1, 4))))


# --------------------------------------------------------------------------------------
# Postal codes
# --------------------------------------------------------------------------------------
def postal_code(col: str | Column, country: str) -> Column:
    compact = F.upper(F.regexp_replace(clean_text(col), r"\s+", ""))
    if country == "GB":
        # 'EC1V2NX' -> 'EC1V 2NX' (the inward code is always the last three characters)
        return let(compact, lambda s: F.when(s.rlike(r"^[A-Z]{1,2}\d[A-Z\d]?\d[A-Z]{2}$"),
                                             F.regexp_replace(s, r"^(.+)(\d[A-Z]{2})$", "$1 $2")))
    if country == "IE":
        # Eircode: routing key (3) + unique identifier (4): 'D02Y512' -> 'D02 Y512'
        return let(compact, lambda s: F.when(s.rlike(r"^([AC-FHKNPRTV-Y]\d{2}|D6W)[0-9AC-FHKNPRTV-Y]{4}$"),
                                             F.concat(F.substring(s, 1, 3), F.lit(" "), F.substring(s, 4, 4))))
    widths = {"FR": 5, "ES": 5, "EE": 5, "NO": 4}
    if country in widths:
        w = widths[country]
        # A lost leading zero is repaired ('1000' -> '01000'); placeholders like Navarra's '0'
        # and over-long values are rejected rather than truncated by lpad.
        digits = let(compact, lambda s: F.when(s.rlike(r"^\d+(\.0)?$"), F.regexp_replace(s, r"\.0$", "")))
        return let(digits, lambda d: F.when(
            F.length(d).between(w - 1, w) & ~F.lpad(d, w, "0").rlike(r"^0+$"), F.lpad(d, w, "0")))
    raise ValueError(f"no postal code rule for {country}")


def eircode_routing_key(col: str | Column) -> Column:
    return F.substring(_c(col), 1, 3)


def gb_outward_code(col: str | Column) -> Column:
    return F.split(_c(col), " ").getItem(0)


# --------------------------------------------------------------------------------------
# Identifier checksums (pure expressions; reference implementations live in tests)
# --------------------------------------------------------------------------------------
def _digit(s: Column, pos: int) -> Column:
    """1-based digit at ``pos`` as int."""
    return F.substring(s, pos, 1).cast("int")


def luhn_valid(col: str | Column, length: int) -> Column:
    s = _c(col)
    total = F.lit(0)
    for i in range(length):  # i = position from the right, 0-based
        d = _digit(s, length - i)
        if i % 2 == 1:
            d = F.when(d * 2 > 9, d * 2 - 9).otherwise(d * 2)
        total = total + d
    return s.rlike(rf"^\d{{{length}}}$") & ((total % 10) == 0)


def siret_valid(col: str | Column) -> Column:
    s = _c(col)
    digit_sum = sum((_digit(s, i) for i in range(1, 15)), F.lit(0))
    la_poste = F.substring(s, 1, 9) == "356000000"  # La Poste establishments use a mod-5 rule
    return F.when(la_poste, s.rlike(r"^\d{14}$") & (digit_sum % 5 == 0)).otherwise(luhn_valid(s, 14))


def no_orgnr_valid(col: str | Column) -> Column:
    s = _c(col)
    weights = [3, 2, 7, 6, 5, 4, 3, 2]
    total = sum((_digit(s, i + 1) * w for i, w in enumerate(weights)), F.lit(0))
    check = 11 - (total % 11)
    expected = F.when(check == 11, 0).otherwise(check)
    return s.rlike(r"^\d{9}$") & (expected != 10) & (expected == _digit(s, 9))


def ee_registry_code_valid(col: str | Column) -> Column:
    s = _c(col)
    r1 = sum((_digit(s, i + 1) * w for i, w in enumerate([1, 2, 3, 4, 5, 6, 7])), F.lit(0)) % 11
    r2 = sum((_digit(s, i + 1) * w for i, w in enumerate([3, 4, 5, 6, 7, 8, 9])), F.lit(0)) % 11
    check = F.when(r1 != 10, r1).when(r2 != 10, r2).otherwise(0)
    return s.rlike(r"^\d{8}$") & (check == _digit(s, 8))


def gb_company_number_valid(col: str | Column) -> Column:
    # 8 digits, or a 2-letter jurisdiction prefix (SC, NI, OC, LP, SL, FC, ...) + 6 digits.
    return _c(col).rlike(r"^(\d{8}|[A-Z]{2}\d{6}|R\d{7})$")


def es_tax_id_kind(col: str | Column) -> Column:
    """NATURAL_PERSON for DNI/NIE (personal ids), LEGAL_ENTITY for CIF, else UNKNOWN.

    Navarra pads DNIs to 9 digits and drops the control letter, so a DNI is all digits.
    """
    s = F.upper(clean_text(col))
    return (
        F.when(s.rlike(r"^\d{8,9}[A-Z]?$") | s.rlike(r"^[XYZKLM]\d{7}[A-Z]$"), F.lit("NATURAL_PERSON"))
        .when(s.rlike(r"^[ABCDEFGHJNPQRSUVW]\d{7}[0-9A-J]$"), F.lit("LEGAL_ENTITY"))
        .otherwise(F.lit("UNKNOWN"))
    )
