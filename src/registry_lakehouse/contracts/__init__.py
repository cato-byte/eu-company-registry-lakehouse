"""Source data contracts (see sources.yml)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cache
from importlib import resources

import yaml

COUNTRY_CODES = ("FR", "GB", "IE", "EE", "ES", "NO")


def bronze_column_name(raw: str) -> str:
    """Delta-safe column name: 'RegAddress.PostTown' -> 'RegAddress_PostTown', ' CompanyNumber' -> 'CompanyNumber'."""
    return re.sub(r"[^0-9A-Za-z_]", "_", raw.strip())


@dataclass(frozen=True)
class SourceContract:
    country_code: str
    country: str
    register: str
    format: str
    encoding: str
    grain: str
    cadence: str
    drift_policy: str
    columns: tuple[str, ...]
    required_columns: tuple[str, ...]
    pii_columns: tuple[str, ...]
    delimiter: str = ","
    id_column: str = ""  # bronze column holding the register identifier

    @property
    def bronze_columns(self) -> tuple[str, ...]:
        return tuple(bronze_column_name(c) for c in self.columns if c not in self.pii_columns)


@cache
def load_contracts() -> dict[str, SourceContract]:
    text = resources.files(__package__).joinpath("sources.yml").read_text(encoding="utf-8")
    raw = yaml.safe_load(text)["sources"]
    contracts = {}
    for code, spec in raw.items():
        columns = tuple(spec["columns"])
        contracts[str(code)] = SourceContract(
            country_code=str(code),
            country=spec["country"],
            register=spec["register"],
            format=spec["format"],
            encoding=spec.get("encoding", "UTF-8"),
            grain=spec["grain"],
            cadence=spec["cadence"],
            drift_policy=spec["drift_policy"],
            columns=columns,
            required_columns=tuple(spec.get("required_columns", columns)),
            pii_columns=tuple(spec.get("pii_columns", [])),
            delimiter=spec.get("delimiter", ","),
            id_column=bronze_column_name(spec["id_column"]),
        )
    return contracts


def get_contract(country_code: str) -> SourceContract:
    try:
        return load_contracts()[country_code]
    except KeyError:
        raise KeyError(f"No contract for country '{country_code}'. Known: {sorted(load_contracts())}") from None
