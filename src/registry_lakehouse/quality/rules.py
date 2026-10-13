"""Data-quality rules for the canonical company rows.

Severity:
  WARN   -> row is kept and the failure is counted (shown in ops.dq_results)
  ERROR  -> row goes to silver.quarantine instead of silver.company
Every rule may also set ``max_failure_rate``. If a snapshot exceeds it, the run aborts (fail
fast). A high failure rate almost always means a structural problem upstream (shifted
columns, new code list, a changed date format), not a few bad rows.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    check: Callable[[], Column]  # True = row passes
    severity: str  # WARN | ERROR
    description: str
    max_failure_rate: float | None = None
    countries: tuple[str, ...] | None = None  # None = all

    def applies_to(self, country: str) -> bool:
        return self.countries is None or country in self.countries


def _c(name: str) -> Column:
    return F.col(name)


RULES: list[Rule] = [
    Rule("business_id_present", lambda: _c("business_id").isNotNull(), "ERROR",
         "Register identifier must exist", max_failure_rate=0.01),
    Rule("id_checksum_valid", lambda: F.coalesce(_c("id_checksum_valid"), F.lit(True)), "ERROR",
         "Identifier passes the register's check-digit algorithm", max_failure_rate=0.01),
    Rule("employees_range_consistent",
         lambda: _c("employees_min").isNull() | _c("employees_max").isNull()
         | (_c("employees_min") <= _c("employees_max")), "ERROR",
         "employees_min <= employees_max"),
    Rule("status_known", lambda: _c("status") != "UNKNOWN", "WARN",
         "Status maps to the shared vocabulary (new status values show up here)", max_failure_rate=0.05),
    Rule("name_present_for_legal_entities",
         lambda: _c("company_name").isNotNull() | F.coalesce(_c("is_natural_person"), F.lit(False)), "WARN",
         "Legal entities have a name", countries=("GB", "IE", "EE", "NO", "ES")),
    Rule("industry_mapped_to_nace",
         lambda: _c("industry_code_local").isNull() | _c("nace_code").isNotNull()
         | ~_c("industry_scheme").isin("NAFRev2", "UK_SIC_2007", "NACE_REV2", "SN2007", "CNAE_2009"),
         "WARN", "A NACE-compatible activity code converts to a valid NACE Rev. 2 class",
         max_failure_rate=0.10),
    Rule("postal_code_valid", lambda: _c("postal_code").isNotNull(), "WARN",
         "Postal code present and well-formed (drives geocoding precision)"),
    Rule("city_present", lambda: _c("city").isNotNull(), "WARN", "Town present (minimum map granularity)",
         max_failure_rate=0.25),
    Rule("address_in_register_country",
         lambda: _c("address_country").isNull() | (_c("address_country") == _c("country_code")), "WARN",
         "Address is in the register's country (foreign addresses are excluded from maps)"),
    Rule("closure_after_registration",
         lambda: _c("closure_date").isNull() | _c("registration_date").isNull()
         | (_c("closure_date") >= _c("registration_date")), "WARN",
         "Closure date is not before registration date"),
]


def rules_for(country: str) -> list[Rule]:
    return [r for r in RULES if r.applies_to(country)]
