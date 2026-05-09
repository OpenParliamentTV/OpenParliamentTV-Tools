"""Cross-parliament agenda-item classifiers."""

import pytest

from optv.shared.agenda_types import (
    CORE_CLOSING,
    CORE_CURRENT_AFFAIRS,
    CORE_GOVERNMENT_DECLARATION,
    CORE_GOVERNMENT_QUESTIONING,
    CORE_OPENING,
    CORE_QA,
    CORE_REGULAR,
    annotate_agenda_item,
    classify_de_native,
    classify_de_rp,
    classify_parlamint_de,
    classify_se,
    is_de_closing_chair_text,
)


@pytest.mark.parametrize("title,expected_native,expected_core", [
    ("Sitzungsende", "DE-closing", CORE_CLOSING),
    ("Schluss der Sitzung", "DE-closing", CORE_CLOSING),
    ("Befragung der Bundesregierung", "DE-questioning_of_the_government",
     CORE_GOVERNMENT_QUESTIONING),
    ("Fragestunde", "DE-question_time", CORE_QA),
    ("Aktuelle Stunde", "DE-current_affairs", CORE_CURRENT_AFFAIRS),
    ("Regierungserklärung", "DE-government_declaration", CORE_GOVERNMENT_DECLARATION),
    ("Eröffnung der Sitzung", "DE-opening_speech", CORE_OPENING),
    ("Tagesordnungspunkt 5", None, CORE_REGULAR),
    ("", None, CORE_REGULAR),
    (None, None, CORE_REGULAR),
])
def test_classify_de_native(title, expected_native, expected_core):
    native, core = classify_de_native(title)
    assert native == expected_native
    assert core == expected_core


@pytest.mark.parametrize("ana,expected_native,expected_core", [
    ("#DE-question_time", "DE-question_time", CORE_QA),
    ("#DE-current_affairs", "DE-current_affairs", CORE_CURRENT_AFFAIRS),
    # Multi-token: priority picks the most specific core, but native = first DE-* in document order
    ("#DE-motion #DE-current_affairs", "DE-motion", CORE_CURRENT_AFFAIRS),
    ("#DE-debate", "DE-debate", CORE_REGULAR),
    (None, None, CORE_REGULAR),
    ("", None, CORE_REGULAR),
    ("#unknown-token", None, CORE_REGULAR),
])
def test_classify_parlamint_de(ana, expected_native, expected_core):
    native, core = classify_parlamint_de(ana)
    assert native == expected_native
    assert core == expected_core


@pytest.mark.parametrize("text,expected", [
    ("Die Sitzung ist geschlossen.", True),
    ("Ich schließe die Sitzung.", True),
    ("Ich beende die Sitzung hiermit.", True),
    ("Die Sitzung ist beendet.", True),
    ("Sehr geehrte Kolleginnen und Kollegen", False),
    ("", False),
    (None, False),
])
def test_is_de_closing_chair_text(text, expected):
    assert is_de_closing_chair_text(text) is expected


def test_classify_de_rp_question_time():
    assert classify_de_rp("Fragestunde der CDU-Fraktion") == ("DE-RP-question_time", CORE_QA)


def test_classify_se_falls_through():
    # Unknown kammaraktivitet preserves the native string and falls back to "regular"
    native, core = classify_se("ärendedebatt")
    assert native == "ärendedebatt"
    assert core == CORE_REGULAR


def test_annotate_agenda_item_preserves_existing():
    # A parser-set value (e.g. parlamint) wins over a later generic re-classification.
    ag = {"officialTitle": "X", "title": "X", "type": CORE_QA, "nativeType": "DE-question_time"}
    annotate_agenda_item(ag, "DE-current_affairs", CORE_CURRENT_AFFAIRS)
    assert ag["type"] == CORE_QA
    assert ag["nativeType"] == "DE-question_time"


def test_annotate_agenda_item_fills_blanks():
    ag = {"officialTitle": "Aktuelle Stunde", "title": "Aktuelle Stunde"}
    nt, ct = classify_de_native(ag["title"])
    annotate_agenda_item(ag, nt, ct)
    assert ag["type"] == CORE_CURRENT_AFFAIRS
    assert ag["nativeType"] == "DE-current_affairs"


def test_annotate_agenda_item_handles_none_native():
    # Tagesordnungspunkt N → no native type, but core still resolves to "regular".
    ag = {"officialTitle": "Tagesordnungspunkt 5", "title": "Tagesordnungspunkt 5"}
    annotate_agenda_item(ag, None, CORE_REGULAR)
    assert ag["type"] == CORE_REGULAR
    assert "nativeType" not in ag
