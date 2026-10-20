# Privacy and sensitive data

The registers are open data, but they still contain personal data: sole traders registered under
their own name (Estonian FIE, Norwegian ENK, Spanish DNI holders), home addresses and contact
details. The repository is public too. That gives two separate problems.

## 1. Keeping sensitive data out of the repository

| Risk | Control |
|---|---|
| Real register rows in tests | Fixtures are synthetic. `tools/make_fixtures.py` keeps the file structure and value shapes but replaces every name, street, identifier (regenerated with valid check digits, never equal to a real one) and free text. French coordinates are moved by up to 1.5 km |
| A later fixture leaking real data | `tests/test_privacy.py` compares fixture identifiers and names with the private samples whenever those are present locally |
| Real samples committed by accident | `data/` and `*.part` are git-ignored, and the pre-commit hook `check_sensitive.py` rejects any file under `data/` |
| Infrastructure identifiers (bucket, account id), keys, tokens, e-mails | pre-commit `gitleaks` and `check_sensitive.py`. CI re-runs both over the full history |
| Environment config | Bucket, volume and alert e-mail are deploy-time variables (`--var`, `BUNDLE_VAR_*`, SAM parameters). `.env` is ignored; `.env.example` lists the variables |
| Old history | The first commit hard-coded the bucket name, so the public history starts from a clean commit |

## 2. Data minimisation inside the lakehouse

* Norwegian e-mail, phone and website fields are dropped by the ingestion Lambda and again at
  bronze (contract `pii_columns`), so they never reach a table.
* Natural persons (`is_natural_person = true`) keep no name and no street in silver; their
  location is a postcode or town. Gold blanks their names a second time in case a mapper regresses.
* Spanish DNI/NIE numbers are replaced by a keyed SHA-256 (the pepper is a Databricks secret), which
  stays stable across snapshots for SCD2. The DNI space is small enough to enumerate, so the hash
  is only as safe as the key: it lives in the secret scope and is never logged or exported.
* INSEE establishments marked non-diffusible (`statutDiffusion = P`) are treated as personal: no
  street, no coordinates.
* Publishing exports only aggregates (counts per town and industry). The row-level stage tour on
  the website is built from the synthetic fixtures only.
