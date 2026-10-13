"""Schema drift detection: pure Python, no Spark."""

import pytest

from registry_lakehouse.bronze.schema_drift import SchemaDriftError, detect_drift, enforce, normalise_header
from registry_lakehouse.contracts import bronze_column_name, get_contract, load_contracts

UK_HEADER_LINE = (
    '﻿CompanyName, CompanyNumber,RegAddress.CareOf\r\n'
)


def test_normalise_header_strips_bom_spaces_quotes_and_crlf():
    assert normalise_header(UK_HEADER_LINE, ",") == ["CompanyName", "CompanyNumber", "RegAddress.CareOf"]
    assert normalise_header('"a";"b"\n', ";") == ["a", "b"]


def test_bronze_column_names_are_delta_safe():
    assert bronze_column_name(" RegAddress.PostTown") == "RegAddress_PostTown"
    assert bronze_column_name("SICCode.SicText_1") == "SICCode_SicText_1"


def test_identical_header_has_no_drift():
    cols = ["a", "b", "c"]
    report = detect_drift("X", cols, cols)
    assert not report.has_drift and not report.is_breaking


def test_reorder_is_reported_but_not_breaking():
    report = detect_drift("X", ["a", "b", "c"], ["c", "a", "b"])
    assert report.reordered and not report.is_breaking


def test_added_column_breaks_only_under_strict_policy():
    assert detect_drift("X", ["a", "b"], ["a", "b", "new"], "strict").is_breaking
    additive = detect_drift("X", ["a", "b"], ["a", "b", "new"], "additive")
    assert additive.added == ("new",) and not additive.is_breaking


def test_removed_column_always_breaks_and_suggests_rename():
    report = detect_drift("FR", ["siren", "codePostalEtablissement"], ["siren", "codePostalEtablissment"], "additive")
    assert report.is_breaking
    assert ("codePostalEtablissement", "codePostalEtablissment") in report.rename_suspects
    with pytest.raises(SchemaDriftError, match="possible renames"):
        enforce(report)


def test_json_sources_only_require_required_keys():
    report = detect_drift("NO", ["orgnr", "navn", "optional"], ["orgnr", "navn"], "additive", required=["orgnr", "navn"])
    assert not report.is_breaking


def test_duplicate_header_names_fail_fast():
    with pytest.raises(SchemaDriftError, match="duplicate"):
        detect_drift("X", ["a", "b"], ["a", "a", "b"])


def test_every_contract_is_consistent():
    contracts = load_contracts()
    assert set(contracts) == {"FR", "GB", "IE", "EE", "ES", "NO"}
    for code, c in contracts.items():
        assert len(set(c.bronze_columns)) == len(c.bronze_columns), f"{code}: bronze names collide"
        assert set(c.required_columns) <= set(c.columns)
        assert c.drift_policy in {"strict", "additive"}
        assert not set(c.pii_columns) & set(c.bronze_columns)
    assert get_contract("EE").delimiter == ";"
