"""Unit tests for the shared canonical meta builder."""

import re

from optv.shared.meta import build_meta, normalize_electoral_period

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def test_canonical_keys_and_order():
    meta = build_meta(
        "XX",
        session="123",
        electoral_period=21,
        date_start="2025-01-01T10:00:00",
        date_end="2025-01-01T11:00:00",
        processing={"merge": "2025-01-01T11:30:00"},
    )
    assert list(meta) == [
        "schemaVersion", "session",
        "dateStart", "dateEnd", "lastProcessing", "lastUpdate", "processing",
    ]
    assert meta["schemaVersion"] == "1.0"
    assert meta["lastProcessing"] == "merge"
    assert _DT_RE.match(meta["lastUpdate"])


def test_parliament_and_electoral_period_never_in_meta():
    # They live per-speech; meta must not duplicate them, even when passed in.
    meta = build_meta("XX", session="1", processing={}, electoral_period=17)
    assert "parliament" not in meta
    assert "electoralPeriod" not in meta


def test_inherited_parliament_and_period_are_dropped():
    meta = build_meta(
        "XX", session="new", processing={"merge": "t"},
        inherit={"session": "old", "customField": "keep",
                 "parliament": "XX", "electoralPeriod": {"number": 9}},
    )
    assert meta["session"] == "new"
    assert meta["customField"] == "keep"
    assert "parliament" not in meta
    assert "electoralPeriod" not in meta


def test_extra_keys_appended_after_canonical():
    meta = build_meta("XX", session="1", processing={}, extra={"sourceLabel": "PTK 1"})
    assert meta["sourceLabel"] == "PTK 1"
    assert list(meta)[-1] == "sourceLabel"


def test_normalize_helper_handles_none():
    assert normalize_electoral_period(None) is None
    assert normalize_electoral_period({"number": None}) is None
    assert normalize_electoral_period(17) == {"number": 17}
