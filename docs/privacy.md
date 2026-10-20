# Privacy and sensitive data

The registers are public open data, but they still contain **personal data**: sole traders
registered under their own name (Estonian FIE, Norwegian ENK, Spanish DNI holders), home addresses,
and contact details. This repository is public, so two separate concerns are handled.

## 1. Nothing sensitive in the repository

| Risk | Control |
|---|---|
| Real register rows in tests | Fixtures are **synthetic**: `tools/make_fixtures.py` keeps the file structure and value shapes but replaces every name, street, identifier (re-generated with valid check digits, never equal to a real one) and free text. French coordinates are moved by up to 1.5 km |
| Fixture leak regression | `tests/test_privacy.py` compares the fixtures' identifier and name columns with the private samples whenever they are present locally |
| Real samples committed by accident | `data/` and `*.part` are git-ignored. The pre-commit hook `check_sensitive.py` rejects any file under `data/` |
| Infrastructure identifiers (bucket, account id), keys, tokens, e-mails | pre-commit `gitleaks` + `check_sensitive.py`. CI re-runs both over the full history |
| Environment config | Bucket, volume and alert e-mail are deploy-time variables (`--var`, `BUNDLE_VAR_*`, SAM parameters). `.env` is ignored, and `.env.example` documents the variables |
| Old history | The first commit hard-coded the bucket name. The public history starts from a clean commit |

## 2. Data minimisation inside the lakehouse

* **Contact details** (Norwegian e-mail, phone, website) are dropped by the ingestion Lambda and
  again at bronze (contract `pii_columns`). They never reach the lakehouse.
* **Natural persons** keep no name and no street in silver (`is_natural_person = true`). Their
  location is postcode or town only. Gold has a second guard that blanks names of natural persons.
* **Personal identifiers** (Spanish DNI/NIE) are replaced by a keyed SHA-256 (pepper in a Databricks
  secret), which stays stable across snapshots for SCD2. The DNI space is small enough to enumerate,
  so the hash is only as safe as the key: it lives in the secret scope and is never logged or exported.
* **INSEE non-diffusible establishments** (`statutDiffusion = P`) are treated as personal: no street,
  no coordinates.
* **Publishing** exports only aggregates (counts per town and industry). The row-level stage tour
  uses synthetic fixtures only.
