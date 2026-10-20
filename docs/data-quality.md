# Data quality: what the registers actually contain, and how each issue is handled

The rules in `silver/parsers.py`, `silver/sources/*.py` and `quality/rules.py` come from what
turned up while profiling the real landing files (first and middle byte ranges of each
file, sampled locally). Rows with a test name are pinned by that test. The two without one record
a source gap and a choice of business key; the Navarra end-to-end tests cover the key indirectly.

## Strategy

1. **Bronze keeps raw text.** Every column lands as STRING, so a bad value can never fail the load,
   and silver can always be re-run with better parsing.
2. **Silver parses explicitly, per source**, with the source's own formats, and **ANSI-safe**:
   `try_to_timestamp`, `try_cast` and guarded `encode/decode`. Serverless runs with ANSI mode on, where
   a plain `cast` or `to_date` with a format throws on the first bad row and kills the job. The
   local test session uses ANSI too, and the linter flags the throwing variants (SPK009).
3. **Unparseable → NULL, then counted.** Values that cannot be parsed become NULL, and the data-quality
   engine counts them per rule and snapshot (`ops.dq_results`).
4. **Severity decides the row's fate.** WARN rows stay. ERROR rows go to `silver.quarantine` with
   the failed rules and the full record as JSON. A failure-rate threshold breach aborts the run.
   Volume collapse against the previous snapshot (< 80%) aborts too: that is what a truncated
   download looks like. Lines the CSV reader could not split never reach the rules, so they are
   counted separately (`malformed_source_line`, 1% threshold) and quarantined with their raw text.

## Observed quirks and their handling

| Register | Quirk (observed) | Handling | Test |
|---|---|---|---|
| all | ANSI casts throw on bad input | `try_*` functions everywhere, linter rule SPK009 | `test_parse_date`, `test_parse_int…` |
| FR | `[ND]` in any field (non-diffusible person, opted out of publication) | global sentinel → NULL. Row treated as a natural person: no street, no name, no coordinates | `test_france` |
| FR | creation date `1900-01-01` = unknown | date sentinel → NULL | `test_parse_date` |
| FR | four activity nomenclatures (NAP, NAF1993, NAFRev1, NAFRev2) | only NAFRev2 is mapped to NACE. Others kept as local code | `test_france` |
| FR | Lambert-93 coordinates, but overseas departments (97x/98x) use other projections | converted only for metropolitan communes. Bounding-box check | `test_overseas_…`, `test_france` |
| FR | `PARIS 15`, `MARSEILLE 8` arrondissements | collapsed to the city | `test_france` |
| FR | establishment file has no company name for most rows (it is in StockUniteLegale) | usage name or sign when present. Documented gap | — |
| GB | header names with leading spaces and dots (` CompanyNumber`, `RegAddress.PostTown`) | header normalised, Delta-safe names | `test_normalise_header…` |
| GB | `dd/MM/yyyy` dates. Some in the future | explicit format. Dates after the snapshot → NULL | `test_parse_date`, `test_united_kingdom` |
| GB | SIC as text `03110 - Marine fishing`, `None Supplied`, `99999 - Dormant Company` | 5-digit code → NACE class. Placeholders → NULL | `test_nace_from_uk_sic` |
| GB | lower-case / unspaced postcodes | normalised to `AB1 2CD`, validated | `test_postal_codes` |
| IE | status `Normal ` with trailing space | trimmed before mapping | `test_status`, `test_ireland` |
| IE | `********NO ADDRESS DETAILS*******` | sentinel → NULL | `test_clean_text…` |
| IE | NACE stored as float: `311.0` is **03.11 fishing** | integer → zero-pad to 4 digits → `03.11`. Without the pad, `311.0` reads as 31.1 (furniture) | `test_nace_from_numeric` |
| IE | old NACE Rev.1 column next to Rev. 2 | kept as local code, never mixed into NACE Rev. 2 | `test_ireland` |
| IE | town hidden in free-text address lines, previously hard-coded to `DUBLIN` | parsed from the last address line (`DUBLIN 2` → `DUBLIN`, county markers skipped) | `test_ireland` |
| IE | malformed CSV lines | kept in `_corrupt_record` instead of shifting columns. Silver quarantines them as `malformed_source_line` and aborts above 1% of lines | `test_bronze_captures_malformed…`, `test_quarantine_holds_…` |
| EE | `;` delimiter, UTF-8 BOM, CRLF | contract delimiter, BOM/CR stripped | `test_bronze_handles_bom…` |
| EE | `ettevotja_aadress` always empty: the street is in `asukoht_ettevotja_aadressis` (old code mapped the empty column) | correct column, fallback kept | `test_estonia` |
| EE | place as `Pirita linnaosa, Tallinn, Harju maakond` | district → city, county kept as region | `test_estonia` |
| EE | `dd.MM.yyyy` dates | explicit format | `test_parse_date` |
| EE | sole traders (FIE) are natural persons | no name / street stored | `test_natural_persons…` |
| ES | one file = 8 yearly snapshots (2018-2025) | each year loaded in order as its own snapshot → SCD2 history | `test_spain…`, e2e |
| ES | numbers exported as floats: `portalt=5.0`, CNAE `3518.0` | integer parsing, zero-padded codes | `test_parse_int…` |
| ES | **format drift between years**: CNAE `4782.0` (2021) vs `4782` (2022) | code canonicalised *before* hashing, otherwise every establishment gets a spurious SCD2 version | `test_numeric_code_is_format_stable` |
| ES | **mojibake** in 2022-2024 (~16% of rows): `TravesÃ­a` for `Travesía` | guarded Latin-1→UTF-8 repair that can never throw | `test_mojibake…` |
| ES | **street spelling drifts between years** for the same establishment (`CARRETERA JAVIER KM1` / `KM 1` / `CARRETERA DE JAVIER`), and there is no establishment id | key uses the street reduced to significant letters (articles, `KM n`, punctuation removed) | `test_spain…`, e2e |
| ES | **ambiguous code `322`**: as an integer it is 0322 (freshwater aquaculture), but the publisher's own description column says 3220 (musical instruments), so its lookup right-padded the number | integer reading kept (`03.22`), consistent with every other code in the file. Documented as a known source ambiguity | `test_nace_from_numeric` |
| ES | landlocked region: the only "fishing" units are a couple of 03.22 trout farms | expected. Tests assert ≥ 1, not a coastal-sized count | `test_spain…` |
| ES | postcode `0` = unknown | placeholder rejected | `test_postal_codes` |
| ES | headcount bands `De 1 a 2`, `250 o más`, `Ninguna` | `employees_min/max`, open upper bound | `test_spain…` |
| ES | DNI/NIE of natural persons as tax id | keyed pseudonymisation, name and street dropped | `test_spain…` |
| ES | no establishment id | business key = tax id + municipality + street + number | — |
| NO | one 2.8 GB pretty-printed JSON array (unsplittable) | Lambda rewrites it as NDJSON parts | `test_json_array_becomes_ndjson…` |
| NO | keys optional per record (only 6.5% report headcount) | drift checked on required keys only. Missing keys → NULL | `test_json_sources_only_require…` |
| NO | SN2007 codes `03.110` | first four digits → `03.11` | `test_nace_from_dotted` |
| NO | status spread over boolean flags + `slettedato` | derived status with explicit precedence | `test_norway` |
| NO | e-mail / phone / web in the records | dropped at ingestion and at bronze | `test_bronze_drops_pii…` |
| NO | foreign business addresses | kept, flagged, excluded from the map | `test_norway` |

## Identifier checksums

Validated with native Spark expressions (no UDF), checked by property-based tests against plain Python:

* SIREN / SIRET: Luhn (La Poste's SIRETs use a digit-sum mod 5 rule)
* Norwegian organisation number: mod 11, weights 3 2 7 6 5 4 3 2
* Estonian registry code: two-pass mod 11 (weights 1-7, then 3-9)

On the real samples 100% of 77k Estonian, 130k French and 17k Norwegian identifiers pass, so a rise
in failures signals a structural problem (shifted columns). That is why the rule has a 1% abort
threshold, which only applies from 1,000 rows up.

## Schema drift

Before reading a file, its header (CSV) or key set (NDJSON) is compared with the contract in
`contracts/sources.yml`:

* Reordered columns are harmless, because columns are mapped by name. They are logged.
* Added columns are allowed under the `additive` policy (kept in `_rescued_data`, with an alert)
  and fatal under `strict`.
* Removed or renamed columns are always fatal. The error suggests likely renames
  (`codePostalEtablissement → codePostalEtablissment?`).

## What data is most likely to fail

From profiling, in order of likelihood: (1) format drift between releases of the same register
(Navarra), (2) encoding problems (mojibake, BOM), (3) new code values in status / legal form lists
(caught by `status_known`), (4) truncated downloads (volume guard + Lambda size check),
(5) renamed or added columns (drift check). Each of these has a test built on a real example.
