"""Release tooling: the version a tag must match, and the notes a release publishes."""

import pytest

from tools import release_notes, set_version

CHANGELOG = """# Changelog

## [Unreleased]

## [1.1.0] - 2026-11-01

### Fixed

- Something.

## [1.0.0] - 2026-10-22

First release.

[Unreleased]: https://example.org/compare/v1.1.0...HEAD
[1.1.0]: https://example.org/v1.1.0
"""


def test_section_stops_at_the_next_release():
    assert release_notes.section(CHANGELOG, "1.1.0") == "### Fixed\n\n- Something."


def test_section_stops_at_the_link_definitions():
    assert release_notes.section(CHANGELOG, "1.0.0") == "First release."


@pytest.mark.parametrize("version", ["Unreleased", "2.0.0", "1.0"])
def test_missing_or_empty_section_is_empty(version):
    assert release_notes.section(CHANGELOG, version) == ""


def test_current_version_has_release_notes():
    """A version bump without changelog notes would fail the release workflow after tagging."""
    version = set_version.current()
    assert release_notes.main([f"v{version}"]) == 0


@pytest.mark.parametrize("arg", ["", "1.0", "v1.0.0", "1.0.0-rc1", "+", "a", "1.0.0 "])
def test_set_version_rejects_malformed_versions(arg, capsys):
    assert set_version.main([arg]) == 2
