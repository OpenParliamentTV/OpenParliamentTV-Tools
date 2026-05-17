"""Pin _TOP_ANNOUNCE_RE / _extract_top_title coverage of the chair's TOP
announcement, including the verb-first inversion.

Regression guard for the DE-17 chair-transition mis-typing: a chair turn
that says "Jetzt rufe ich Tagesordnungspunkt N auf" was not matched by the
subject-first-only regex, so ~76 chair-transition turns were typed `regular`
and shipped as gate-passing cps mis-merges (e.g. 17108 #76). The regex must
catch both word orders without retyping genuine speech text."""

import pytest

from optv.parliaments.DE.parsers.parlamint2json import (
    _TOP_ANNOUNCE_RE,
    _extract_top_title,
)


# Verb-first inversion — the case the original regex missed.
@pytest.mark.parametrize("text,expected", [
    ("Jetzt rufe ich Tagesordnungspunkt 8 auf:", "Tagesordnungspunkt 8"),
    ("Nun rufe ich Tagesordnungspunkt 12 auf.", "Tagesordnungspunkt 12"),
    ("Dann rufe ich Zusatzpunkt 3 auf", "Zusatzpunkt 3"),
    ("Sodann rufe ich den Einzelplan 4 auf", "Einzelplan 4"),
    ("Damit rufe ich die Tagesordnungspunkte 5 und 6 auf", "Tagesordnungspunkt 5"),
])
def test_extract_top_title_verb_first_inversion(text, expected):
    assert _extract_top_title(text) == expected


# Subject-first forms must keep working unchanged.
@pytest.mark.parametrize("text,expected", [
    ("Ich rufe Tagesordnungspunkt 8 auf", "Tagesordnungspunkt 8"),
    ("Ich rufe jetzt Tagesordnungspunkt 8 auf", "Tagesordnungspunkt 8"),
    ("Ich rufe nun den Tagesordnungspunkt 5 auf", "Tagesordnungspunkt 5"),
    ("Ich rufe Zusatzpunkt 2 auf", "Zusatzpunkt 2"),
])
def test_extract_top_title_subject_first_still_matches(text, expected):
    assert _extract_top_title(text) == expected


# Non-announcement text must not match (no over-eager retyping).
@pytest.mark.parametrize("text", [
    "Ich erteile dem Kollegen das Wort.",
    "Wir kommen jetzt zur Abstimmung.",
    "",
    "Die Aussprache ist eröffnet.",
])
def test_extract_top_title_returns_none_for_non_announcement(text):
    assert _extract_top_title(text) is None
    assert _TOP_ANNOUNCE_RE.search(text) is None
