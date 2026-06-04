"""Cross-parliament agenda-item classifiers."""

import pytest

from optv.shared.agenda_types import (
    CORE_CLOSING,
    CORE_CURRENT_AFFAIRS,
    CORE_ELECTION,
    CORE_GOVERNMENT_DECLARATION,
    CORE_GOVERNMENT_QUESTIONING,
    CORE_OATH,
    CORE_OPENING,
    CORE_QA,
    CORE_REGULAR,
    CORE_PROCEDURAL,
    CORE_VOTING,
    annotate_agenda_item,
    classify_de_bw,
    classify_de_native,
    classify_de_rp,
    classify_de_sh,
    classify_parlamint_de,
    classify_pt,
    classify_se,
    is_de_closing_chair_text,
)


@pytest.mark.parametrize("itype,expected_native,expected_core", [
    ("Abertura da sessão", "PT-abertura", CORE_OPENING),
    ("Encerramento", "PT-encerramento", CORE_CLOSING),
    ("Votações", "PT-votacoes", CORE_VOTING),
    ("Leitura", "PT-leitura", CORE_PROCEDURAL),
    ("Interpelação à mesa", "PT-interpelacao_a_mesa", CORE_PROCEDURAL),
    ("Protesto", "PT-protesto", CORE_PROCEDURAL),
    ("Pedido de esclarecimento", "PT-pedido_de_esclarecimento", CORE_QA),
    ("Intervenção", "PT-intervencao", CORE_REGULAR),
    ("Declaração política", "PT-declaracao_politica", CORE_REGULAR),
    ("", None, CORE_REGULAR),
    (None, None, CORE_REGULAR),
])
def test_classify_pt(itype, expected_native, expected_core):
    native, core = classify_pt(itype)
    assert (native, core) == (expected_native, expected_core)


@pytest.mark.parametrize("title,expected_native,expected_core", [
    ("Sitzungsende", "DE-closing", CORE_CLOSING),
    ("Schluss der Sitzung", "DE-closing", CORE_CLOSING),
    ("Befragung der Bundesregierung", "DE-questioning_of_the_government",
     CORE_GOVERNMENT_QUESTIONING),
    # ParlaMint abbreviation variant
    ("Befragung der BReg", "DE-questioning_of_the_government",
     CORE_GOVERNMENT_QUESTIONING),
    ("Befragung der BReg | Tagesordnungspunkt 1",
     "DE-questioning_of_the_government", CORE_GOVERNMENT_QUESTIONING),
    ("Fragestunde", "DE-question_time", CORE_QA),
    # ParlaMint per-question slots (count toward question time)
    ("BMVg Frage 01", "DE-question_time", CORE_QA),
    ("BMVg Frage 01 | Tagesordnungspunkt 2", "DE-question_time", CORE_QA),
    ("BMU Frage 03", "DE-question_time", CORE_QA),
    ("BMELV Frage 02", "DE-question_time", CORE_QA),
    ("BMVBS Frage 27 | Tagesordnungspunkt 2", "DE-question_time", CORE_QA),
    # Negative cases: should NOT match the per-question pattern
    ("BMVg Bericht zur Lage", None, CORE_REGULAR),
    ("Frage zur Sache", None, CORE_REGULAR),
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


@pytest.mark.parametrize("title,expected_native,expected_core", [
    # m7k themes observed on the live mediathek
    ("Eröffnung der Sitzung durch den Alterspräsidenten", "DE-SH-opening", CORE_OPENING),
    ("Wahl und Vereidigung der Landtagspräsidentin", "DE-SH-election", CORE_ELECTION),
    # Standard procedural categories
    ("Fragestunde", "DE-SH-question_time", CORE_QA),
    ("Aktuelle Stunde zur Energiewende", "DE-SH-current_affairs", CORE_CURRENT_AFFAIRS),
    ("Aktuelle Debatte zur Bildungspolitik", "DE-SH-current_affairs", CORE_CURRENT_AFFAIRS),
    ("Regierungserklärung des Ministerpräsidenten", "DE-SH-government_declaration",
     CORE_GOVERNMENT_DECLARATION),
    ("Befragung der Landesregierung", "DE-SH-questioning_of_the_government",
     CORE_GOVERNMENT_QUESTIONING),
    ("Wahl des Vizepräsidenten", "DE-SH-election", CORE_ELECTION),
    ("Vereidigung der Ministerin", "DE-SH-oath", CORE_OATH),
    ("Haushaltsgesetz 2026", "DE-SH-budget", None),  # core is CORE_BUDGET — see below
    # Negative / fall-through cases
    ("3 Minuten Beiträge", None, CORE_REGULAR),
    ("Tagesordnungspunkt 5", None, CORE_REGULAR),
    ("", None, CORE_REGULAR),
    (None, None, CORE_REGULAR),
])
def test_classify_de_sh(title, expected_native, expected_core):
    native, core = classify_de_sh(title)
    assert native == expected_native
    # Special-case: the budget core constant isn't imported above to keep the
    # imports lean; assert it positionally instead.
    if expected_native == "DE-SH-budget":
        assert core == "budget"
    else:
        assert core == expected_core


@pytest.mark.parametrize("title,expected_native,expected_core", [
    # mediathek TOP headers observed on the live chapter list
    ("TOP 1 Aktuelle Debatte", "DE-BW-current_affairs", CORE_CURRENT_AFFAIRS),
    ("Beginn der Sitzung", "DE-BW-opening", CORE_OPENING),
    ("Fragestunde", "DE-BW-question_time", CORE_QA),
    ("Regierungsbefragung", "DE-BW-questioning_of_the_government",
     CORE_GOVERNMENT_QUESTIONING),
    ("Regierungserklärung des Ministerpräsidenten", "DE-BW-government_declaration",
     CORE_GOVERNMENT_DECLARATION),
    ("Wahl des Präsidenten", "DE-BW-election", CORE_ELECTION),
    ("Verpflichtung der Abgeordneten", "DE-BW-oath", CORE_OATH),
    # Substantive readings fall through to regular
    ("TOP 3 Zweite Beratung des Gesetzentwurfs", None, CORE_REGULAR),
    ("Tagesordnungspunkt 5", None, CORE_REGULAR),
    ("", None, CORE_REGULAR),
    (None, None, CORE_REGULAR),
])
def test_classify_de_bw(title, expected_native, expected_core):
    native, core = classify_de_bw(title)
    assert native == expected_native
    assert core == expected_core


def test_classify_de_bw_budget():
    # budget core constant not imported above; assert positionally
    native, core = classify_de_bw("TOP 2 Zweite Beratung Staatshaushaltsplan 2025")
    assert native == "DE-BW-budget"
    assert core == "budget"


def test_classify_se_falls_through():
    # Unknown kammaraktivitet preserves the native string and falls back to "regular"
    native, core = classify_se("ärendedebatt")
    assert native == "ärendedebatt"
    assert core == CORE_REGULAR


# ---------------------------------------------------------------------------
# Protokoll-rede announcement detection (parlamint2json)
# ---------------------------------------------------------------------------

import re
from optv.parliaments.DE.parsers.parlamint2json import _PROTOKOLL_ANNOUNCE_RE


@pytest.mark.parametrize("text,expected", [
    # Variations observed in DE-17 ParlaMint XML
    ("Die Reden sollen zu Protokoll genommen werden.", True),
    ("Auch diese Reden sollen zu Protokoll genommen werden.", True),
    ("Die Reden gehen zu Protokoll.", True),
    ("Die Reden gehen zu Protokoll.6", True),
    ("Hierzu ist vereinbart, die Reden zu Protokoll zu nehmen.", True),
    ("Wie in der Tagesordnung ausgewiesen, werden die Reden zu Protokoll genommen.", True),
    ("Die Reden von Susanne Mittag und Sylvia Lehmann werden zu Protokoll gegeben.", True),
    # Negative cases — must NOT match
    ("Ich rufe die Tagesordnungspunkte 21 a und 21 b auf:", False),
    ("Die Sitzung ist geschlossen.", False),
    ("Ich schließe die Aussprache.", False),
    # Tricky: speaker mentions Protokoll in their own speech (not a chair announcement)
    ("Ich habe meine Rede ja zu Protokoll gegeben", True),  # would match — but only fires on chair <u>s in practice
])
def test_protokoll_announce_regex(text, expected):
    assert bool(_PROTOKOLL_ANNOUNCE_RE.search(text)) is expected


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
