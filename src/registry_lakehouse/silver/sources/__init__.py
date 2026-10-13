"""Registry of source mappers: country code -> (mapper, register label, grain)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame

from registry_lakehouse.silver.sources.estonia import map_estonia
from registry_lakehouse.silver.sources.france import map_france
from registry_lakehouse.silver.sources.ireland import map_ireland
from registry_lakehouse.silver.sources.norway import map_norway
from registry_lakehouse.silver.sources.spain import map_spain
from registry_lakehouse.silver.sources.united_kingdom import map_united_kingdom


@dataclass(frozen=True)
class SourceMapper:
    mapper: Callable[[DataFrame, date], DataFrame]
    register: str
    grain: str
    # True when one landing file holds several snapshots (Navarra's yearly panel).
    embedded_snapshots: bool = False


MAPPERS: dict[str, SourceMapper] = {
    "FR": SourceMapper(map_france, "INSEE_SIRENE", "ESTABLISHMENT"),
    "GB": SourceMapper(map_united_kingdom, "COMPANIES_HOUSE", "LEGAL_ENTITY"),
    "IE": SourceMapper(map_ireland, "CRO", "LEGAL_ENTITY"),
    "EE": SourceMapper(map_estonia, "RIK_ARIREGISTER", "LEGAL_ENTITY"),
    "ES": SourceMapper(map_spain, "NAVARRA_ESTABLECIMIENTOS", "ESTABLISHMENT", embedded_snapshots=True),
    "NO": SourceMapper(map_norway, "BRREG_ENHETSREGISTERET", "LEGAL_ENTITY"),
}
