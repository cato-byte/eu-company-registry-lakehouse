"""Schema drift detection, run before any landing file is read.

Registers change their exports without notice (a renamed column, a new one, a different order).
Reading such a file with a positional schema silently shifts values into the wrong columns,
which is far worse than failing. So the actual header is compared with the contract first.
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass, field


@dataclass(frozen=True)
class DriftReport:
    source: str
    added: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    reordered: bool = False
    rename_suspects: tuple[tuple[str, str], ...] = ()
    policy: str = "strict"
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def has_drift(self) -> bool:
        return bool(self.added or self.removed or self.reordered or self.notes)

    @property
    def is_breaking(self) -> bool:
        """Removed/renamed columns always break. Additions break only under the strict policy.

        Reordering is harmless because the loader maps columns by header name, never by position.
        """
        if self.removed:
            return True
        return bool(self.added) and self.policy == "strict"

    def summary(self) -> str:
        if not self.has_drift:
            return f"[{self.source}] schema matches contract"
        parts = [f"[{self.source}] schema drift (policy={self.policy}, breaking={self.is_breaking})"]
        if self.added:
            parts.append(f"added={list(self.added)}")
        if self.removed:
            parts.append(f"removed={list(self.removed)}")
        if self.rename_suspects:
            parts.append("possible renames=" + ", ".join(f"{a}->{b}" for a, b in self.rename_suspects))
        if self.reordered:
            parts.append("column order changed (handled: columns are mapped by name)")
        parts.extend(self.notes)
        return " | ".join(parts)


class SchemaDriftError(RuntimeError):
    def __init__(self, report: DriftReport):
        super().__init__(report.summary())
        self.report = report


def normalise_header(raw_line: str, delimiter: str) -> list[str]:
    """Split a CSV header line, dropping a UTF-8 BOM, CR/LF, quotes and padding spaces."""
    line = raw_line.lstrip("﻿").rstrip("\r\n")
    return [c.strip().strip('"').strip() for c in line.split(delimiter)]


def detect_drift(
    source: str,
    expected: list[str] | tuple[str, ...],
    actual: list[str] | tuple[str, ...],
    policy: str = "strict",
    required: list[str] | tuple[str, ...] | None = None,
) -> DriftReport:
    """Compare the contract with what the file actually contains.

    For JSON sources keys are optional per record: pass ``required`` so that only those count as
    "removed" when absent; unknown keys still count as "added".
    """
    if len(set(actual)) != len(actual):
        dupes = sorted({c for c in actual if list(actual).count(c) > 1})
        raise SchemaDriftError(DriftReport(source, removed=tuple(dupes), policy=policy,
                                           notes=("duplicate column names in header",)))
    exp_set, act_set = set(expected), set(actual)
    added = tuple(c for c in actual if c not in exp_set)
    must_exist = required if required is not None else expected
    removed = tuple(c for c in must_exist if c not in act_set)
    common_expected = [c for c in expected if c in act_set]
    common_actual = [c for c in actual if c in exp_set]
    reordered = required is None and common_expected != common_actual
    suspects = []
    for gone in removed:
        match = difflib.get_close_matches(gone, added, n=1, cutoff=0.75)
        if match:
            suspects.append((gone, match[0]))
    return DriftReport(source, added, removed, reordered, tuple(suspects), policy)


def enforce(report: DriftReport) -> DriftReport:
    if report.is_breaking:
        raise SchemaDriftError(report)
    return report
