"""Build synthetic, committable test fixtures from the local landing samples.

    python tools/make_fixtures.py          # reads data/samples (git-ignored), writes tests/fixtures

What is kept from the real data (so the tests exercise reality):
  * file format details: delimiter, BOM, CRLF, quoting, header quirks, column order
  * value *shapes*: date formats, float-mangled codes, sentinels ('[ND]', 'None Supplied', '0'),
    status vocabularies, legal forms, activity codes, postcodes and town names (public places)
  * row mix: fishing companies, sole traders, closed/insolvent units, foreign addresses, ...

What is replaced (so nothing identifies a real company or person):
  * every name, previous name, street name, free-text purpose, e-mail, phone
  * every identifier: regenerated with a valid check digit where the register uses one
    (SIREN/SIRET Luhn, Norwegian mod-11, Estonian registry code)
  * French coordinates are moved randomly by up to 1.5 km

tests/test_fixtures_privacy.py then checks that no real identifier or name from the samples
appears anywhere in the fixtures.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import os
import random
import re
import shutil
import sys
from collections import defaultdict

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SAMPLES = os.path.join(ROOT, "data", "samples")
GEONAMES = os.path.join(ROOT, "data", "reference")
OUT = os.path.join(ROOT, "tests", "fixtures")
SNAPSHOT = "2026-07-01"
SNAPSHOT_2 = "2026-08-01"

rng = random.Random(20260701)

WORDS_A = ["NORDIC", "ATLANTIC", "BLUE", "HARBOUR", "GREEN", "SUMMIT", "PIONEER", "ALPINE", "CORAL", "MERIDIAN",
           "EVERGREEN", "OAKFIELD", "FJORD", "BALTIC", "IBERIA", "CELTIC", "NORTHSTAR", "NOVA", "DELTA", "APEX",
           "SILVERLINE", "WESTWIND", "BRIGHT", "TIDAL", "GRANITE"]
WORDS_B = ["TRADING", "CONSULTING", "FISHERIES", "HOLDINGS", "LOGISTICS", "SERVICES", "DESIGN", "FOODS", "MARINE",
           "TECH", "PROPERTIES", "SEAFOOD", "BUILDERS", "VENTURES", "STUDIO", "SOLUTIONS", "AQUA", "PARTNERS"]
SUFFIXES = ["LIMITED", "LTD", "LTD.", "LLP", "PLC", "CIC", "DAC", "CLG", "UC", "OÜ", "AS", "ASA", "ANS", "DA", "SL",
            "SA", "SAS", "SARL", "MTÜ", "osaühing", "aktsiaselts", "FIE", "UNLIMITED COMPANY"]
FIRST = ["ALEX", "SAM", "ROBIN", "KIM", "NOA", "ELI", "MAR", "ANDREA", "JULES", "TEO", "ILO", "RENE"]
LAST = ["EXAMPLE", "SAMPLE", "TESTER", "DEMO", "PLACEHOLDER", "MOCK", "FICTION", "SYNTH", "DUMMY", "FAUX"]
STREETS = {
    "GB": ["HIGH STREET", "STATION ROAD", "CHURCH LANE", "MILL ROAD", "HARBOUR VIEW", "QUAYSIDE"],
    "IE": ["MAIN STREET", "CHURCH ROAD", "HARBOUR ROAD", "THE SQUARE", "PIER ROAD"],
    "FR": ["DE LA REPUBLIQUE", "DES LILAS", "DU PORT", "DES ECOLES", "DE LA MARINE", "DES PECHEURS"],
    "EE": ["Sadama tn", "Kase tn", "Pargi tn", "Mere pst", "Kalda tn"],
    "NO": ["Storgata", "Kaigata", "Skolevegen", "Fjordvegen", "Havnegata"],
    "ES": ["MAYOR", "DEL PUERTO", "SAN JUAN", "LAS HERAS", "DEL RIO"],
}


# ---------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------
def fake_company(original: str) -> str:
    while (name := _fake_company(original)).upper() in REAL_NAMES:
        pass
    return name


def _fake_company(original: str) -> str:
    lead = original[0] if original and not original[0].isalnum() and original[0] != '"' else ""
    tokens = original.split()
    suffix = tokens[-1] if tokens and tokens[-1].strip(".,").upper() in {s.upper() for s in SUFFIXES} else ""
    core = f"{rng.choice(WORDS_A)} {rng.choice(WORDS_B)}"
    if "&" in original and rng.random() < 0.5:
        core = f"{rng.choice(WORDS_A)} & {rng.choice(WORDS_A)}"
    return " ".join(x for x in (lead + core, suffix) if x)


def fake_person(style: str = "plain") -> str:
    f, l1, l2 = rng.choice(FIRST), rng.choice(LAST), rng.choice(LAST)
    return f"{l1},{l2},{f}" if style == "es" else f"{f.title()} {l1.title()}"


# Identifiers and names seen in the private samples: generated ones must never equal a real one.
REAL_IDS: set[str] = set()
REAL_NAMES: set[str] = set()


def digits(n: int, first: str | None = None) -> str:
    s = "".join(rng.choice("0123456789") for _ in range(n))
    return (first + s[1:]) if first else s


def collect_real_ids() -> None:
    for name, delim, cols in [("ESTONIA.part", ";", (1,)), ("IRELAND.part", ",", (0,)), ("SPAIN.part", ",", (2,)),
                              ("UK.head.part", ",", (1,)), ("UK.mid.part", ",", (1,)),
                              ("FRANCE.head.part", ",", (0, 2)), ("FRANCE.mid.part", ",", (0, 2))]:
        _, rows = read_csv(os.path.join(SAMPLES, name), delim, skip_first_partial=name.endswith("mid.part"))
        name_col = {"ESTONIA.part": 0, "IRELAND.part": 1, "SPAIN.part": 3}.get(name, 0)
        for r in rows:
            REAL_IDS.update(r[c].strip() for c in cols if len(r) > c)
            if len(r) > name_col:
                REAL_NAMES.add(r[name_col].strip().upper())
    text = open(os.path.join(SAMPLES, "NORWAY.json.part"), encoding="utf-8", errors="ignore").read(60_000_000)
    REAL_IDS.update(re.findall(r'"organisasjonsnummer" : "(\d{9})"', text))
    REAL_NAMES.update(n.upper() for n in re.findall(r'"navn" : "([^"]+)"', text))


def luhn_check_digit(partial: str) -> str:
    total = 0
    for i, ch in enumerate(reversed(partial)):  # positions from the right, check digit not yet appended
        d = int(ch)
        if i % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return str((10 - total % 10) % 10)


def fresh(make) -> str:
    """Call ``make`` until the generated identifier is not a real one."""
    while (value := make()) in REAL_IDS:
        pass
    return value


def fake_siren() -> str:
    def make() -> str:
        base = digits(8, first=rng.choice("3456789"))
        return base + luhn_check_digit(base)

    return fresh(make)


def fake_siret(siren: str) -> str:
    def make() -> str:
        base = siren + digits(4, first="0")
        return base + luhn_check_digit(base)

    return fresh(make)


def fake_no_orgnr() -> str:
    def make() -> str:
        while True:
            base = digits(8, first=rng.choice("89"))
            r = 11 - sum(int(a) * w for a, w in zip(base, [3, 2, 7, 6, 5, 4, 3, 2], strict=True)) % 11
            r = 0 if r == 11 else r
            if r != 10:
                return base + str(r)

    return fresh(make)


def fake_ee_code() -> str:
    def make() -> str:
        base = digits(7, first="1")
        d = [int(c) for c in base]
        r = sum(a * b for a, b in zip(d, [1, 2, 3, 4, 5, 6, 7], strict=True)) % 11
        if r == 10:
            r = sum(a * b for a, b in zip(d, [3, 4, 5, 6, 7, 8, 9], strict=True)) % 11
            r = 0 if r == 10 else r
        return base + str(r)

    return fresh(make)


def read_csv(path: str, delim: str = ",", skip_first_partial: bool = False) -> tuple[list[str], list[list[str]]]:
    text = open(path, encoding="utf-8-sig", errors="replace").read()
    if skip_first_partial:
        text = text[text.find("\n") + 1:]
    text = text[: text.rfind("\n")]
    rows = list(csv.reader(io.StringIO(text), delimiter=delim))
    return rows[0], rows[1:]


def pick(rows: list, cond, n: int) -> list:
    matches = [r for r in rows if cond(r)]
    rng.shuffle(matches)
    return matches[:n]


def unique(rows: list[list[str]], key_idx: int) -> list[list[str]]:
    seen, out = set(), []
    for r in rows:
        if r[key_idx] not in seen:
            seen.add(r[key_idx])
            out.append(r)
    return out


def landing_dir(country: str, snapshot: str = SNAPSHOT) -> str:
    d = os.path.join(OUT, "landing", country, f"ingest_date={snapshot}")
    os.makedirs(d, exist_ok=True)
    return d


def write_csv(path: str, header: list[str], rows: list[list[str]], delim: str = ",", quote_all: bool = False,
              bom: bool = False, crlf: bool = False, raw_header: str | None = None) -> None:
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=delim, quoting=csv.QUOTE_ALL if quote_all else csv.QUOTE_MINIMAL,
                   lineterminator="\r\n" if crlf else "\n")
    for r in rows:
        w.writerow(r)
    eol = "\r\n" if crlf else "\n"
    head = raw_header if raw_header is not None else delim.join(header)
    with open(path, "w", encoding="utf-8-sig" if bom else "utf-8", newline="") as fh:
        fh.write(head + eol + buf.getvalue())


# ---------------------------------------------------------------------------------------------
# France
# ---------------------------------------------------------------------------------------------
def build_france(used_places: dict) -> None:
    h, a = read_csv(os.path.join(SAMPLES, "FRANCE.head.part"))
    _, b = read_csv(os.path.join(SAMPLES, "FRANCE.mid.part"), skip_first_partial=True)
    rows = [r for r in a + b if len(r) == len(h)]
    ix = {c: i for i, c in enumerate(h)}
    naf, nom = ix["activitePrincipaleEtablissement"], ix["nomenclatureActivitePrincipaleEtablissement"]
    lam = ix["coordonneeLambertAbscisseEtablissement"]
    chosen = (
        pick(rows, lambda r: r[naf].startswith("03.") and r[nom] == "NAFRev2" and r[lam] not in ("", "[ND]"), 6)
        + pick(rows, lambda r: r[ix["statutDiffusionEtablissement"]] == "P", 4)
        + pick(rows, lambda r: r[ix["codeCommuneEtablissement"]].startswith(("97", "98")), 2)
        + pick(rows, lambda r: r[ix["dateCreationEtablissement"]] == "1900-01-01", 2)
        + pick(rows, lambda r: r[nom] in ("NAP", "NAF1993"), 3)
        + pick(rows, lambda r: r[ix["etatAdministratifEtablissement"]] == "F", 4)
        + pick(rows, lambda r: r[ix["libelleCommuneEtablissement"]].startswith("PARIS"), 2)
        + pick(rows, lambda r: r[nom] == "NAFRev2" and r[lam] not in ("", "[ND]")
               and r[ix["etatAdministratifEtablissement"]] == "A", 12)
    )
    chosen = unique(chosen, ix["siret"])
    siren_map: dict[str, str] = {}
    out = []
    for r in chosen:
        r = list(r)
        siren = siren_map.setdefault(r[ix["siren"]], fake_siren())
        siret = fake_siret(siren)
        r[ix["siren"]], r[ix["siret"]], r[ix["nic"]] = siren, siret, siret[9:]
        for col in ("denominationUsuelleEtablissement", "enseigne1Etablissement", "enseigne2Etablissement",
                    "enseigne3Etablissement"):
            if r[ix[col]] not in ("", "[ND]"):
                r[ix[col]] = fake_company(r[ix[col]])
        for col in ("libelleVoieEtablissement", "libelleVoie2Etablissement"):
            if r[ix[col]] not in ("", "[ND]"):
                r[ix[col]] = rng.choice(STREETS["FR"])
        if r[ix["complementAdresseEtablissement"]] not in ("", "[ND]"):
            r[ix["complementAdresseEtablissement"]] = rng.choice(["ZONE INDUSTRIELLE", "BATIMENT B", "ZI DU PORT"])
        if r[ix["identifiantAdresseEtablissement"]] not in ("", "[ND]"):
            r[ix["identifiantAdresseEtablissement"]] = f"{r[ix['codeCommuneEtablissement']]}{digits(4)}_B"
        for col in ("coordonneeLambertAbscisseEtablissement", "coordonneeLambertOrdonneeEtablissement"):
            if r[ix[col]] not in ("", "[ND]"):
                r[ix[col]] = f"{float(r[ix[col]]) + rng.uniform(-1500, 1500):.4f}"
        used_places["FR"].add((r[ix["codePostalEtablissement"]], r[ix["libelleCommuneEtablissement"]]))
        out.append(r)
    # Two establishments of the same company (SIREN shared, different SIRET)
    twin = list(out[-1])
    twin_siret = fake_siret(twin[ix["siren"]])
    twin[ix["siret"]], twin[ix["nic"]] = twin_siret, twin_siret[9:]
    out.append(twin)
    # A row whose SIRET fails the Luhn check (data-quality rule id_checksum_valid)
    bad = list(out[0])
    bad[ix["siret"]] = bad[ix["siret"]][:-1] + str((int(bad[ix["siret"]][-1]) + 1) % 10)
    out.append(bad)
    write_csv(os.path.join(landing_dir("FRANCE"), "StockEtablissement_utf8.csv"), h, out)


# ---------------------------------------------------------------------------------------------
# United Kingdom (+ a second snapshot to exercise SCD2)
# ---------------------------------------------------------------------------------------------
def build_uk(used_places: dict) -> None:
    raw_header = open(os.path.join(SAMPLES, "UK.head.part"), encoding="utf-8").readline().rstrip("\r\n")
    h, a = read_csv(os.path.join(SAMPLES, "UK.head.part"))
    _, b = read_csv(os.path.join(SAMPLES, "UK.mid.part"), skip_first_partial=True)
    rows = [r for r in a + b if len(r) == len(h) and r[0] != "CompanyName"]
    hs = [c.strip() for c in h]
    ix = {c: i for i, c in enumerate(hs)}
    sic, st = ix["SICCode.SicText_1"], ix["CompanyStatus"]
    chosen = unique(
        pick(rows, lambda r: re.match(r"^03\d{3} ", r[sic]) is not None, 7)
        + pick(rows, lambda r: r[sic] == "None Supplied", 2)
        + pick(rows, lambda r: r[sic].startswith("99999"), 2)
        + pick(rows, lambda r: r[st] in ("Liquidation", "In Administration"), 3)
        + pick(rows, lambda r: r[st].startswith("Active - Proposal"), 1)
        + pick(rows, lambda r: r[ix["CountryOfOrigin"]] not in ("United Kingdom", ""), 2)
        + pick(rows, lambda r: r[ix["CompanyNumber"]].startswith(("SC", "NI")), 2)
        + pick(rows, lambda r: r[ix["RegAddress.PostCode"]] == "", 1)
        + pick(rows, lambda r: r[ix["PreviousName_1.CompanyName"]] != "", 2)
        + pick(rows, lambda r: r[st] == "Active" and r[ix["RegAddress.PostCode"]] != "", 10),
        ix["CompanyNumber"],
    )
    out = []
    for r in chosen:
        r = list(r)
        num = r[ix["CompanyNumber"]]
        prefix = re.match(r"^[A-Z]*", num).group(0)
        new = fresh(lambda p=prefix: p + digits(8 - len(p)))
        r[ix["CompanyNumber"]] = new
        r[ix["CompanyName"]] = fake_company(r[ix["CompanyName"]])
        r[ix["URI"]] = f"http://business.data.gov.uk/id/company/{new}"
        if r[ix["RegAddress.CareOf"]]:
            r[ix["RegAddress.CareOf"]] = "EXAMPLE ACCOUNTANTS LTD"
        if r[ix["RegAddress.AddressLine1"]]:
            r[ix["RegAddress.AddressLine1"]] = f"{rng.randint(1, 250)} {rng.choice(STREETS['GB'])}"
        if r[ix["RegAddress.AddressLine2"]]:
            r[ix["RegAddress.AddressLine2"]] = rng.choice(["UNIT 2", "FIRST FLOOR", "THE OLD MILL", ""])
        for k in range(1, 11):
            col = f"PreviousName_{k}.CompanyName"
            if r[ix[col]]:
                r[ix[col]] = fake_company(r[ix[col]])
        used_places["GB"].add((r[ix["RegAddress.PostCode"]], r[ix["RegAddress.PostTown"]]))
        out.append(r)
    # Edge cases: incorporation date in the future, and a lowercase postcode without a space.
    out[-1][ix["IncorporationDate"]] = "01/01/2099"
    out[-2][ix["RegAddress.PostCode"]] = out[-2][ix["RegAddress.PostCode"]].replace(" ", "").lower()
    write_csv(os.path.join(landing_dir("UNITED_KINGDOM"), "BasicCompanyDataAsOneFile-2026-07-01.csv"), h, out,
              quote_all=True, raw_header=raw_header)

    # Snapshot 2: one company enters liquidation, one moves, one disappears, one is new.
    snap2 = [list(r) for r in out]
    active = [r for r in snap2 if r[st] == "Active"]
    active[0][st] = "Liquidation"
    active[1][ix["RegAddress.AddressLine1"]] = f"{rng.randint(1, 250)} {rng.choice(STREETS['GB'])}"
    removed = snap2.pop(snap2.index(active[2]))
    newco = list(removed)
    newco[ix["CompanyNumber"]] = fresh(lambda: digits(8, first="1"))
    newco[ix["CompanyName"]] = fake_company("NEWCO LTD")
    newco[ix["URI"]] = f"http://business.data.gov.uk/id/company/{newco[ix['CompanyNumber']]}"
    snap2.append(newco)
    write_csv(os.path.join(landing_dir("UNITED_KINGDOM", SNAPSHOT_2), "BasicCompanyDataAsOneFile-2026-08-01.csv"),
              h, snap2, quote_all=True, raw_header=raw_header)


# ---------------------------------------------------------------------------------------------
# Ireland
# ---------------------------------------------------------------------------------------------
def build_ireland(used_places: dict) -> None:
    h, rows = read_csv(os.path.join(SAMPLES, "IRELAND.part"))
    ix = {c: i for i, c in enumerate(h)}
    nace, st, a1 = ix["nace_v2_code"], ix["company_status"], ix["company_address_1"]
    chosen = unique(
        pick(rows, lambda r: r[nace] in ("311.0", "312.0", "321.0", "322.0"), 5)
        + pick(rows, lambda r: r[nace] in ("3511.0", "3312.0"), 2)  # look like fishing without the zero pad
        + pick(rows, lambda r: "NO ADDRESS" in r[a1], 2)
        + pick(rows, lambda r: r[st] == "Normal " and r[ix["eircode"]] != "", 8)
        + pick(rows, lambda r: r[st] == "Normal", 1)
        + pick(rows, lambda r: r[st].startswith("Dissolved"), 3)
        + pick(rows, lambda r: r[st] in ("Liquidation", "Strike Off Listed", "Examinership"), 3)
        + pick(rows, lambda r: r[nace] == "" and r[ix["princ_object_code"]] != "", 2),
        ix["company_num"],
    )
    out = []
    for r in chosen:
        r = list(r)
        r[ix["company_num"]] = fresh(lambda: str(rng.randint(100000, 799999)))
        r[ix["company_name"]] = fake_company(r[ix["company_name"]])
        if "NO ADDRESS" not in r[a1]:
            r[a1] = f"{rng.randint(1, 99)} {rng.choice(STREETS['IE'])}"
            # Lines 2-3 are free text (estates, buildings): keep only the town taken from line 4.
            town = (r[ix["company_address_4"]] or "").split(",")[0].strip()
            r[ix["company_address_2"]] = town
            r[ix["company_address_3"]] = ""
        e = r[ix["eircode"]].replace(" ", "")
        if len(e) == 7:
            r[ix["eircode"]] = e[:3] + " " + "".join(rng.choice("ACDEFHKNPRTVWXY0123456789") for _ in range(4))
        used_places["IE"].add((r[ix["eircode"]][:3], r[ix["company_address_4"]] or r[ix["company_address_3"]]))
        out.append(r)
    # A malformed line (too many fields): must end up in _corrupt_record, not shift columns.
    malformed = list(out[0])
    malformed[ix["company_num"]] = fresh(lambda: str(rng.randint(100000, 799999)))
    out.append(malformed + ["EXTRA", "FIELDS"])
    write_csv(os.path.join(landing_dir("IRELAND"), "companies.csv"), h, out)


# ---------------------------------------------------------------------------------------------
# Estonia
# ---------------------------------------------------------------------------------------------
def build_estonia(used_places: dict) -> None:
    h, rows = read_csv(os.path.join(SAMPLES, "ESTONIA.part"), delim=";")
    ix = {c: i for i, c in enumerate(h)}
    form, st = ix["ettevotja_oiguslik_vorm"], ix["ettevotja_staatus"]
    chosen = unique(
        pick(rows, lambda r: r[form].startswith("Füüsilisest"), 5)
        + pick(rows, lambda r: r[st] == "L", 2)
        + pick(rows, lambda r: r[st] == "N", 2)
        + pick(rows, lambda r: r[ix["asukoha_ehak_tekstina"]] == "", 1)
        + pick(rows, lambda r: r[form] == "Mittetulundusühing", 2)
        + pick(rows, lambda r: r[form] == "Aktsiaselts", 2)
        + pick(rows, lambda r: r[form] == "Osaühing" and "Tallinn" not in r[ix["asukoha_ehak_tekstina"]], 5)
        + pick(rows, lambda r: r[form] == "Osaühing" and "Tallinn" in r[ix["asukoha_ehak_tekstina"]], 6),
        ix["ariregistri_kood"],
    )
    out = []
    for r in chosen:
        r = list(r)
        code = fake_ee_code()
        natural = r[form].startswith("Füüsilisest")
        r[ix["nimi"]] = f"{fake_person()} FIE" if natural else fake_company(r[ix["nimi"]])
        r[ix["ariregistri_kood"]] = code
        if r[ix["kmkr_nr"]]:
            r[ix["kmkr_nr"]] = "EE10" + digits(7)
        if r[ix["asukoht_ettevotja_aadressis"]]:
            street = f"{rng.choice(STREETS['EE'])} {rng.randint(1, 60)}"
            r[ix["asukoht_ettevotja_aadressis"]] = street
            parts = [p.strip() for p in r[ix["asukoha_ehak_tekstina"]].split(",") if p.strip()]
            r[ix["ads_normaliseeritud_taisaadress"]] = ", ".join([*reversed(parts), street])
        if r[ix["ads_adr_id"]]:
            r[ix["ads_adr_id"]] = digits(7, first="2")
        r[ix["teabesysteemi_link"]] = f"https://ariregister.rik.ee/est/company/{code}"
        used_places["EE"].add((r[ix["indeks_ettevotja_aadressis"]], r[ix["asukoha_ehak_tekstina"]]))
        out.append(r)
    out.append(list(out[-1]))  # exact duplicate row: deduplication must keep one
    write_csv(os.path.join(landing_dir("ESTONIA"), "ettevotja_rekvisiidid__lihtandmed.csv"), h, out, delim=";",
              bom=True, crlf=True)


# ---------------------------------------------------------------------------------------------
# Spain (two panel years)
# ---------------------------------------------------------------------------------------------
def build_spain(used_places: dict) -> None:
    h, rows = read_csv(os.path.join(SAMPLES, "SPAIN.part"))
    ix = {c: i for i, c in enumerate(h)}
    by_year: dict[str, dict[tuple, list[str]]] = defaultdict(dict)
    for r in rows:
        if len(r) == len(h):
            # '5.0' (2021) and '5' (2022) are the same street number: normalise like the pipeline does
            number = r[ix["portalt"]].removesuffix(".0")
            street = re.sub(r"\b(DE|DEL|LA|LAS|LOS|EL|KM)\b|[^A-Z]", "", re.sub(r"KM\s*\d+", "", r[ix["callet"]].upper()))
            key = (r[ix["dnici"]], r[ix["cod_cmun"]].lstrip("0"), street, number)
            by_year[r[ix["anio"]]][key] = r
    y1, y2 = by_year["2021"], by_year["2022"]
    both = [k for k in y1 if k in y2]
    fish = [k for k in both if re.match(r"^3[12][12](\.0)?$", y1[k][ix["cnae09_local"]])]
    elec = [k for k in both if y1[k][ix["cnae09_local"]].startswith("3518")]
    big = [k for k in both if y1[k][ix["tramo_asal"]] in ("250 o más", "De 100 a 249")]
    zero_pc = [k for k in both if y1[k][ix["codpost"]] == "0"]
    cif = [k for k in both if re.match(r"^[A-Z]", k[0])]
    nat = [k for k in both if k[0].isdigit()]
    for lst in (fish, elec, big, zero_pc, cif, nat):
        rng.shuffle(lst)
    keep_both = list(dict.fromkeys(fish[:5] + elec[:2] + big[:2] + zero_pc[:1] + cif[:6] + nat[:6]))
    only_1 = [k for k in y1 if k not in y2][:3]
    only_2 = [k for k in y2 if k not in y1][:3]

    id_map: dict[str, str] = {}
    name_map: dict[str, str] = {}
    street_map: dict[str, str] = {}
    floor_map: dict[tuple[str, str], str] = {}

    def anon(r: list[str]) -> list[str]:
        r = list(r)
        tax = r[ix["dnici"]]
        if tax not in id_map:
            natural = tax[:1].isdigit() or tax[:1] in "XYZKLM"  # DNI / NIE
            id_map[tax] = fresh(lambda t=tax: "0" + digits(8) if t[:1].isdigit()
                                else t[0] + digits(7) + rng.choice("0123456789ABCDEFGHIJ"))
            name_map[tax] = fake_person("es") if natural else fake_company(r[ix["nombre"]] or "X SL")
        r[ix["dnici"]] = id_map[tax]
        if r[ix["nombre"]]:
            r[ix["nombre"]] = name_map[tax]
        original_street = r[ix["callet"]]
        norm = re.sub(r"\b(DE|DEL|LA|LAS|LOS|EL|KM)\b|[^A-Z]", "", re.sub(r"KM\s*\d+", "", original_street.upper()))
        base = street_map.setdefault(norm, rng.choice(STREETS["ES"]))
        # Keep the register's spelling variants ('X KM1', 'X KM 1', 'DE X') on the fake street name.
        km = re.search(r"KM\s*\d*", original_street.upper())
        fake = ("DE " if re.search(r"\bDE\b", original_street.upper()) else "") + base + (f" {km.group(0)}" if km else "")
        r[ix["callet"]] = fake
        if r[ix["restot"]] and r[ix["restot"]] not in ("BAJO", "BJ", "PBJ", "-PBJ-", ". ."):
            # Same establishment -> same floor in every year; keep the source's mojibake if present.
            floor = floor_map.setdefault((tax, norm), rng.choice(["1º A", "2ºC", "BAJO"]))
            r[ix["restot"]] = floor.encode("utf-8").decode("latin-1") if "Â" in r[ix["restot"]] else floor
        used_places["ES"].add((r[ix["codpost"]], r[ix["nombre_cmun"]]))
        return r

    out, n = [], 1
    for year, keys in (("2021", keep_both + only_1), ("2022", keep_both + only_2)):
        src = y1 if year == "2021" else y2
        for k in keys:
            r = anon(src[k])
            r[ix["_id"]] = str(n)
            n += 1
            out.append(r)
    write_csv(os.path.join(landing_dir("SPAIN"), "0c4b4747-026f-479a-9966-6568a88957f5"), h, out)


# ---------------------------------------------------------------------------------------------
# Norway (raw JSON array -> NDJSON parts through the real converter)
# ---------------------------------------------------------------------------------------------
def build_norway(used_places: dict) -> None:
    sys.path.insert(0, os.path.join(ROOT, "src"))
    from registry_lakehouse.ingestion.norway_ndjson import convert_json_array_to_ndjson_parts, iter_records

    recs = []
    try:
        for i, rec in enumerate(iter_records(os.path.join(SAMPLES, "NORWAY.json.part"))):
            recs.append(rec)
            if i > 120_000:
                break
    except Exception:
        pass

    def code(r: dict) -> str:
        return (r.get("naeringskode1") or {}).get("kode", "")

    def form(r: dict) -> str:
        return (r.get("organisasjonsform") or {}).get("kode", "")

    chosen = (
        pick(recs, lambda r: code(r).startswith("03.") and form(r) == "AS", 5)
        + pick(recs, lambda r: code(r).startswith("03.") and form(r) == "ENK", 2)
        + pick(recs, lambda r: form(r) == "ENK" and not code(r).startswith("03."), 4)
        + pick(recs, lambda r: r.get("konkurs") is True, 2)
        + pick(recs, lambda r: r.get("underAvvikling") is True, 2)
        + pick(recs, lambda r: (r.get("forretningsadresse") or {}).get("landkode", "NO") != "NO", 2)
        + pick(recs, lambda r: "naeringskode1" not in r, 2)
        + pick(recs, lambda r: "antallAnsatte" in r and form(r) == "AS", 3)
        + pick(recs, lambda r: form(r) in ("FLI", "NUF", "SA"), 3)
        + pick(recs, lambda r: form(r) == "AS", 8)
    )
    seen, out = set(), []
    for r in chosen:
        if r["organisasjonsnummer"] in seen:
            continue
        seen.add(r["organisasjonsnummer"])
        r = json.loads(json.dumps(r, default=float))
        r["organisasjonsnummer"] = fake_no_orgnr()
        natural = form(r) == "ENK"
        r["navn"] = fake_person().upper() if natural else fake_company(r["navn"])
        r["historiskeNavn"] = [{**h, "navn": fake_company(h.get("navn", "X AS"))} for h in r.get("historiskeNavn", [])]
        for key in ("forretningsadresse", "postadresse"):
            if key in r and r[key].get("adresse"):
                r[key]["adresse"] = [f"{rng.choice(STREETS['NO'])} {rng.randint(1, 80)}"]
        for key in ("vedtektsfestetFormaal", "aktivitet"):
            if key in r:
                r[key] = ["Synthetic purpose text for tests."]
        r["paategninger"] = []  # annotations can carry free text
        if "overordnetEnhet" in r:
            r["overordnetEnhet"] = fake_no_orgnr()
        if "utenlandskRegisterAdresse" in r:
            r["utenlandskRegisterAdresse"] = {"adresse": ["1 Example Road"], "land": r["utenlandskRegisterAdresse"].get("land")}
        if "registreringsnummerIHjemlandet" in r:
            r["registreringsnummerIHjemlandet"] = digits(8)
        # Contact fields exist in the source: keep obviously fake values so the test can prove they are dropped.
        for key in ("epostadresse", "telefon", "mobil", "hjemmeside"):
            if key in r:
                r[key] = {"epostadresse": "post@example.invalid", "hjemmeside": "www.example.invalid"}.get(key, "00 00 00 00")
        fa = r.get("forretningsadresse") or r.get("postadresse") or {}
        used_places["NO"].add((fa.get("postnummer", ""), fa.get("poststed", "")))
        out.append(r)
    out[-1]["slettedato"] = "2026-06-15"  # a deleted unit
    out[-1]["somethingNew"] = "unexpected key"  # additive schema drift -> _rescued_data
    raw_dir = os.path.join(OUT, "raw", "NORWAY")
    os.makedirs(raw_dir, exist_ok=True)
    raw = os.path.join(raw_dir, "enheter_sample.json.gz")
    with gzip.open(raw, "wt", encoding="utf-8") as fh:
        fh.write(json.dumps(out, ensure_ascii=False, indent=2))
    land = landing_dir("NORWAY")
    for part in convert_json_array_to_ndjson_parts(raw, land, records_per_part=20):
        print("  norway part", os.path.relpath(part, ROOT))


# ---------------------------------------------------------------------------------------------
# GeoNames subset for the places used above (public gazetteer data, CC BY 4.0)
# ---------------------------------------------------------------------------------------------
def build_geonames(used_places: dict) -> None:
    out_dir = os.path.join(OUT, "landing", "REFERENCE", "geonames")
    os.makedirs(out_dir, exist_ok=True)
    files = {"FR": "FR/FR.txt", "ES": "ES/ES.txt", "EE": "EE/EE.txt", "NO": "NO/NO.txt", "IE": "IE/IE.txt",
             "GB": "GB_full/GB_full.txt"}
    for cc, rel in files.items():
        wanted_pc = {pc.strip().upper() for pc, _ in used_places[cc] if pc}
        wanted_pc |= {pc.replace(" ", "").upper()[:-3] + " " + pc.replace(" ", "").upper()[-3:]
                      for pc, _ in used_places[cc] if cc == "GB" and len(pc.replace(" ", "")) >= 5}
        wanted_place = set()
        for _, place in used_places[cc]:
            for token in re.split(r"[,/]", place or ""):
                t = re.sub(r"\s+(linnaosa|linn|vald|maakond|küla)$", "", token.strip(), flags=re.I).upper()
                if t:
                    wanted_place.add(t)
        by_pc, by_place = [], []
        with open(os.path.join(GEONAMES, rel), encoding="utf-8") as fh:
            for line in fh:
                f = line.rstrip("\n").split("\t")
                if f[1].upper() in wanted_pc:
                    by_pc.append(line)
                elif f[2].upper() in wanted_place or (f[7] or "").upper() in wanted_place:
                    by_place.append(line)
        # All postcode matches; place-name matches capped (Tallinn alone has hundreds of postcodes).
        keep = by_pc + by_place[:150]
        with open(os.path.join(out_dir, f"{cc}.txt"), "w", encoding="utf-8") as fh:
            fh.writelines(keep)
        print(f"  geonames {cc}: {len(by_pc)} postcode + {min(len(by_place), 150)} place rows")


def build_cities(used_places: dict) -> None:
    """Towns from GeoNames cities500 whose name matches a town used in the fixtures."""
    wanted = set()
    for cc, places in used_places.items():
        for _, place in places:
            for token in re.split(r"[,/]", place or ""):
                t = re.sub(r"\s+(linnaosa|linn|vald|maakond|küla|alevik)$", "", token.strip(), flags=re.I).upper()
                t = re.sub(r"^(CO\.?|COUNTY)\s+", "", t)
                if t:
                    wanted.add((cc, t))
    out_dir = os.path.join(OUT, "landing", "REFERENCE", "geonames_cities")
    os.makedirs(out_dir, exist_ok=True)
    keep = []
    with open(os.path.join(GEONAMES, "cities500.txt"), encoding="utf-8") as fh:
        for line in fh:
            f = line.split("\t")
            if (f[8], f[1].upper()) in wanted or (f[8], f[2].upper()) in wanted:
                keep.append(line)
    with open(os.path.join(out_dir, "cities500_subset.txt"), "w", encoding="utf-8") as fh:
        fh.writelines(keep)
    print(f"  geonames towns: {len(keep)} rows")


def main() -> int:
    if not os.path.isdir(SAMPLES):
        print("data/samples missing: run tools/sample_landing.py first", file=sys.stderr)
        return 2
    collect_real_ids()
    shutil.rmtree(os.path.join(OUT, "landing"), ignore_errors=True)
    shutil.rmtree(os.path.join(OUT, "raw"), ignore_errors=True)
    used: dict[str, set] = defaultdict(set)
    for fn in (build_france, build_uk, build_ireland, build_estonia, build_spain, build_norway):
        fn(used)
        print("built", fn.__name__)
    build_geonames(used)
    build_cities(used)
    return 0


if __name__ == "__main__":
    sys.exit(main())
