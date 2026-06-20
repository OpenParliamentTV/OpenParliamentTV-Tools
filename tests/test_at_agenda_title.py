"""AT two-title split — optv/parliaments/AT/parsers/agenda_title.py."""

import pytest

from optv.parliaments.AT.parsers.agenda_title import split_agenda_title


@pytest.mark.parametrize("raw,official,title", [
    # Single TOP → normalized label + descriptive subject
    ("TOP 7 Nächtliche Dauerbeleuchtung von Windrädern",
     "Tagesordnungspunkt 7", "Nächtliche Dauerbeleuchtung von Windrädern"),
    # Contiguous range
    ("TOP 1-3 Corona-Krise: Entlastung für Haushalte",
     "Tagesordnungspunkte 1 bis 3", "Corona-Krise: Entlastung für Haushalte"),
    # Comma list / mixed range
    ("TOP 8-9,10 Budget 2024: Oberste Organe",
     "Tagesordnungspunkte 8 bis 9, 10", "Budget 2024: Oberste Organe"),
    # Subject containing a hyphen must not be eaten by the range parser
    ("TOP 5 Wohn- und Heizkosten",
     "Tagesordnungspunkt 5", "Wohn- und Heizkosten"),
    # Non-TOP items keep both fields identical
    ("Abstimmung über die Tagesordnungspunkte 1 bis 5",
     "Abstimmung über die Tagesordnungspunkte 1 bis 5",
     "Abstimmung über die Tagesordnungspunkte 1 bis 5"),
    ("Fragestunde", "Fragestunde", "Fragestunde"),
    ("Präsidium", "Präsidium", "Präsidium"),
    # Source typo normalized in the non-TOP branch
    ("Abstimmung über die Tageordnungspunkte 12 bis 23",
     "Abstimmung über die Tagesordnungspunkte 12 bis 23",
     "Abstimmung über die Tagesordnungspunkte 12 bis 23"),
    ("", "", ""),
    (None, "", ""),
])
def test_split_agenda_title(raw, official, title):
    assert split_agenda_title(raw) == (official, title)
