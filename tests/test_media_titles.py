"""Pure title helpers from optv/parliaments/DE/parsers/media2json.py."""

import pytest

from optv.parliaments.DE.parsers.media2json import extract_title_data, fix_title


def test_extract_title_data_real_format():
    title = "Redebeitrag von Stephan Stracke (CDU/CSU) am 29.01.2010 um 14:05 Uhr (20. Sitzung, TOP ZP 2)"
    data = extract_title_data(title)
    assert data is not None
    assert data["fullname"] == "Stephan Stracke"
    assert data["faction"] == "CDU/CSU"
    assert data["title_date"] == "29.01.2010"
    assert data["title_time"] == "14:05"
    assert "TOP ZP 2" in data["session_info"]


def test_extract_title_data_returns_none_on_garbage():
    assert extract_title_data("not a redebeitrag") is None
    assert extract_title_data("") is None


def test_extract_title_data_handles_empty_faction():
    title = "Redebeitrag von Nationalhymne (), am 01.01.2020 um 12:00 Uhr (1. Sitzung, TOP 1)"
    data = extract_title_data(title)
    assert data is not None
    assert data["faction"] == ""


@pytest.mark.parametrize("raw,expected", [
    ("TOP ZP 2", "Zusatzpunkt 2"),
    ("TOP Epl 04", "Einzelplan 04"),
    ("TOP 5", "Tagesordnungspunkt 5"),
    ("TOP Sitzungsende", "Sitzungsende"),
    ("TOP Sitzungseröffnung", "Sitzungseröffnung"),
    ("Tagesordnungspunkt 1.", "Tagesordnungspunkt 1"),
])
def test_fix_title(raw, expected):
    assert fix_title(raw) == expected
