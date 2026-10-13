# Everything runs locally, for free: nothing here touches Databricks.
PY ?= .venv/bin/python
VENV_BIN := .venv/bin

.PHONY: setup lint test e2e test-all run-local fixtures cost site clean bundle-check

setup:            ## create the virtualenv with Spark + Delta + test tooling
	python3 -m venv .venv && $(VENV_BIN)/pip install -e ".[local,ingestion,dev]" && $(VENV_BIN)/pre-commit install

lint:             ## ruff + PySpark anti-patterns + sensitive-data scan
	$(VENV_BIN)/ruff check src tests tools
	$(PY) tools/spark_lint.py src
	git ls-files | xargs $(PY) tools/check_sensitive.py

test:             ## unit tests (Spark, local)
	$(VENV_BIN)/pytest tests -m "not e2e"

e2e:              ## whole pipeline on the synthetic fixtures
	$(VENV_BIN)/pytest tests -m e2e

test-all: lint test e2e

run-local:        ## run every stage locally and write the demo site data
	rm -rf .local && mkdir -p .local && cp -r tests/fixtures/landing .local/landing
	REGISTRY_LANDING_ROOT=.local/landing REGISTRY_SNAPSHOT_DATE=2026-07-31 REGISTRY_PUBLISH_ROOT=site/data/demo \
		REGISTRY_MASS_ADDRESS_MIN=3 REGISTRY_FISHING_HUB_MIN=1 \
		$(VENV_BIN)/registry-lakehouse all --samples

fixtures:         ## rebuild synthetic fixtures from private samples (needs data/samples)
	$(PY) tools/make_fixtures.py

bundle-check:     ## strict validation of the Databricks bundle (dev + prod), no workspace needed
	$(PY) tools/bundle_check.py databricks

cost:             ## predicted cost of a full production run
	$(VENV_BIN)/registry-cost estimate --sizes FR=9.3GB,GB=2.6GB,IE=184MB,NO=2.8GB,EE=92MB,ES=69MB

site:             ## serve the static site locally on http://localhost:8000
	cd site && python3 -m http.server 8000

clean:
	rm -rf .local spark-warehouse .pytest_cache .ruff_cache dist build
