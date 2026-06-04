"""Tests for the EP Open Data API client + parser.

Fixtures under ``tests/fixtures/EU/`` were captured live from
``data.europarl.europa.eu/api/v2`` on 2026-05-26.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from optv.parliaments.EU.parsers.proceedings2json import parse_speeches_payload
from optv.parliaments.EU.scraper import ep_api
from optv.parliaments.EU.scraper.ep_api import (
    EPApiClient,
    PLENARY_DEBATE_SPEECH,
    PLENARY_SITTING,
    _activity_type,
    ref_to_id,
    strip_iri_prefix,
)

FIXT = Path(__file__).parent / "fixtures" / "EU"


# ---------- helpers ----------

def _load(name: str) -> dict:
    return json.loads((FIXT / name).read_text())


# ---------- pure helpers ----------

def test_strip_iri_prefix():
    assert strip_iri_prefix("epdata:person/124785", "person/") == "124785"
    assert strip_iri_prefix("person/28150", "person/") == "28150"
    assert strip_iri_prefix(None) is None
    assert strip_iri_prefix("plain-value", "person/") == "plain-value"


def test_ref_to_id():
    assert ref_to_id("eli/dl/event/MTG-PL-2025-10-08") == "MTG-PL-2025-10-08"
    assert ref_to_id("MTG-PL-2025-10-08") == "MTG-PL-2025-10-08"
    assert ref_to_id(None) is None


def test_activity_type_extracts_bare_name():
    assert _activity_type({"had_activity_type": "def/ep-activities/PLENARY_SITTING"}) == "PLENARY_SITTING"
    assert _activity_type({"had_activity_type": ""}) == ""
    assert _activity_type({}) == ""


# ---------- pagination ----------

def test_iter_pages_follows_meta_total(tmp_path):
    """The client should keep paging until meta.total is reached, returning
    every item across all pages."""
    client = EPApiClient(cache_dir=None, min_interval=0.0, burst=1)

    # Two pages of 3 + 2 items.
    pages = {
        0: {"data": [{"i": 0}, {"i": 1}, {"i": 2}], "meta": {"total": 5}},
        3: {"data": [{"i": 3}, {"i": 4}], "meta": {"total": 5}},
    }

    def fake_get_json(path, params=None):
        return pages[int(params["offset"])]

    with patch.object(client, "get_json", side_effect=fake_get_json):
        items = list(client._iter_pages("speeches", {"limit": 3}))
    assert [x["i"] for x in items] == [0, 1, 2, 3, 4]


def test_iter_pages_stops_on_empty(tmp_path):
    client = EPApiClient(cache_dir=None, min_interval=0.0, burst=1)
    pages = {0: {"data": [{"i": 0}], "meta": {"total": 99}}, 1: {"data": []}}

    def fake_get_json(path, params=None):
        return pages[int(params["offset"])]

    with patch.object(client, "get_json", side_effect=fake_get_json):
        items = list(client._iter_pages("speeches", {"limit": 1}))
    assert items == [{"i": 0}]


# ---------- rate limiting ----------

def test_throttle_enforces_min_interval_burst():
    """With burst=2 and 1s min_interval, the third call should sleep at least
    until the first call's stamp ages out."""
    client = EPApiClient(min_interval=0.05, burst=2)
    t0 = time.monotonic()
    for _ in range(3):
        client._throttle()
    elapsed = time.monotonic() - t0
    # Two immediate + one waiting one full window (0.05 * 2 = 0.1s)
    assert elapsed >= 0.1


# ---------- list_plenary_sittings ----------

def test_list_plenary_sittings_filters_non_plenary(tmp_path):
    """Mixed activity types in /meetings — only PLENARY_SITTING should pass."""
    client = EPApiClient(cache_dir=None, min_interval=0.0, burst=1)
    payload = {
        "data": [
            {"activity_id": "MTG-PL-2025-01-20", "activity_date": "2025-01-20",
             "had_activity_type": "def/ep-activities/PLENARY_SITTING"},
            {"activity_id": "MTG-OTHER-2025-01-20",
             "had_activity_type": "def/ep-activities/COMMITTEE_MEETING"},
            {"activity_id": "MTG-PL-2025-01-21", "activity_date": "2025-01-21",
             "had_activity_type": "def/ep-activities/PLENARY_SITTING"},
        ],
    }

    def fake_get_json(path, params=None):
        return payload

    with patch.object(client, "get_json", side_effect=fake_get_json):
        items = client.list_plenary_sittings(2025)
    ids = [it["activity_id"] for it in items]
    assert ids == ["MTG-PL-2025-01-20", "MTG-PL-2025-01-21"]


# ---------- parser ----------

@pytest.fixture
def speeches_fixture() -> dict:
    return _load("speeches_20251008_sample.jsonld")


@pytest.fixture
def meeting_fixture() -> dict:
    raw = _load("meeting_20251008.jsonld")
    agenda_raw = _load("agenda_item_itm10.jsonld")
    return {
        "meeting": raw["data"][0],
        "agenda_items": {agenda_raw["data"][0]["activity_id"]: agenda_raw["data"][0]},
    }


def test_parse_real_fixture_yields_debate_speeches(speeches_fixture, meeting_fixture):
    doc = parse_speeches_payload(speeches_fixture, meeting_fixture, "20251008")
    # We captured 5 speeches; not all of them may be PLENARY_DEBATE_SPEECH.
    assert doc["meta"]["session"] == "20251008"
    assert doc["meta"]["parliament"] == "EU"
    debate_speeches_in_fixture = [
        s for s in speeches_fixture["data"]
        if _activity_type(s) == PLENARY_DEBATE_SPEECH
    ]
    assert len(doc["data"]) <= len(debate_speeches_in_fixture)
    for sp in doc["data"]:
        assert sp["speechId"]
        assert sp["dateStart"], "every parsed speech must carry a TZ-aware start"
        assert sp["dateStart"].endswith("+00:00"), "timestamps normalized to UTC"
        assert sp["textParagraphs"], "non-empty text body required"


def test_parse_extracts_person_epid(speeches_fixture, meeting_fixture):
    """Speeches with structured had_participation should carry epId without
    relying on xml_fragment refersTo parsing."""
    doc = parse_speeches_payload(speeches_fixture, meeting_fixture, "20251008")
    epids = [s["speaker"].get("epId") for s in doc["data"]]
    assert any(e for e in epids), "at least one parsed speech should have an epId"


def test_parse_extracts_faction_abbr_when_present(speeches_fixture, meeting_fixture):
    """If the fixture contains any speech with <organization>, the faction
    abbr should be recognised and mapped to a known label."""
    from optv.parliaments.EU.parsers.common import EU_FACTION_LABELS
    doc = parse_speeches_payload(speeches_fixture, meeting_fixture, "20251008")
    factions = [s["speaker"].get("factionAbbr") for s in doc["data"]
                if s["speaker"].get("factionAbbr")]
    for abbr in factions:
        assert abbr in EU_FACTION_LABELS


def test_parse_resolves_agenda_for_itm10(speeches_fixture, meeting_fixture):
    """Speeches whose parent CRE doc is ITM-010 should pick up the captured
    English agenda title."""
    doc = parse_speeches_payload(speeches_fixture, meeting_fixture, "20251008")
    titles = {(s["agendaItem"] or {}).get("officialTitle")
              for s in doc["data"]
              if (s["agendaItem"] or {}).get("number") == 10}
    # If any of the fixture speeches sits under ITM-010, the resolved title
    # must come from the captured agenda item rather than "Untitled".
    if titles:
        assert "Untitled agenda item" not in titles


def test_parse_uses_en_xml_fragment_by_default(speeches_fixture, meeting_fixture):
    """Sample paragraph should be English (the captured fixture is real EP
    text). Heuristic: at least one Latin-script letter, no Cyrillic."""
    doc = parse_speeches_payload(speeches_fixture, meeting_fixture, "20251008")
    assert doc["data"], "fixture must yield at least one speech"
    text = " ".join(doc["data"][0]["textParagraphs"])
    assert any("a" <= c.lower() <= "z" for c in text)
    assert all("Ѐ" > c or c > "ӿ" for c in text), \
        "EN fixture must not contain Cyrillic"


def test_parse_falls_back_to_first_lang_when_en_missing():
    """Construct a speech whose xml_fragment has only fr+de keys — parser
    should pick fr (next in preference) and flag debug.fallbackLang."""
    fragment_fr = (
        '<oralStatements><speech startTime="2025-10-08T09:00:00+02:00" '
        'endTime="2025-10-08T09:00:30+02:00">'
        '<from><person refersTo="epdata:person/12345">Jean Dupont</person>'
        '(<organization>S&amp;D</organization>).</from>'
        '<blockContainer><p>Bonjour à tous.</p></blockContainer>'
        '</speech></oralStatements>'
    )
    speeches = {"data": [{
        "activity_id": "MTG-PL-2025-10-08-OTH-1",
        "activity_start_date": "2025-10-08T09:00:00+02:00",
        "activity_end_date": "2025-10-08T09:00:30+02:00",
        "had_activity_type": "def/ep-activities/PLENARY_DEBATE_SPEECH",
        "had_participation": {"had_participant_person": ["person/12345"]},
        "recorded_in_a_realization_of": [{
            "identifier": "CRE-10-2025-10-08-OTH-1",
            "notation_speechId": "1",
            "is_part_of": "eli/dl/doc/CRE-10-2025-10-08-ITM-001",
            "api:xmlFragment": {"fr": fragment_fr, "de": "<oralStatements/>"},
        }],
    }]}
    meeting = {"meeting": {}, "agenda_items": {}}
    doc = parse_speeches_payload(speeches, meeting, "20251008")
    assert len(doc["data"]) == 1
    sp = doc["data"][0]
    assert sp["debug"]["fallbackLang"] == "fr"
    assert sp["speaker"]["epId"] == "12345"
    assert sp["speaker"]["factionAbbr"] == "S&D"
    assert "Bonjour" in sp["textParagraphs"][0]


def test_parse_filters_non_debate_activities():
    """Vote results / chair changes should be dropped — only PLENARY_DEBATE_SPEECH."""
    fragment_en = (
        '<oralStatements><speech startTime="2025-10-08T09:00:00+02:00" '
        'endTime="2025-10-08T09:00:30+02:00">'
        '<from><person>Anyone</person></from>'
        '<blockContainer><p>hello</p></blockContainer>'
        '</speech></oralStatements>'
    )
    speeches = {"data": [
        {
            "activity_id": "MTG-PL-2025-10-08-VOT-1",
            "had_activity_type": "def/ep-activities/VOTE_RESULT",
            "recorded_in_a_realization_of": [{"api:xmlFragment": {"en": fragment_en}}],
        },
        {
            "activity_id": "MTG-PL-2025-10-08-OTH-1",
            "activity_start_date": "2025-10-08T09:00:00+02:00",
            "activity_end_date": "2025-10-08T09:00:30+02:00",
            "had_activity_type": "def/ep-activities/PLENARY_DEBATE_SPEECH",
            "recorded_in_a_realization_of": [{
                "notation_speechId": "1",
                "api:xmlFragment": {"en": fragment_en},
            }],
        },
    ]}
    doc = parse_speeches_payload(speeches, {"meeting": {}, "agenda_items": {}}, "20251008")
    assert len(doc["data"]) == 1
    assert doc["data"][0]["speechId"] == "1"


# ---------- nel: epId direct match ----------

def test_link_entities_prefers_epid_over_label():
    """A speaker carrying an epId should get linked even when the label
    fuzzy-match would fail (e.g. nickname vs. canonical name)."""
    from optv.shared.nel import link_entities, _build_ep_id_index

    persons = {
        "real name": {
            "id": "Q42",
            "label": "Real Name",
            "labelAlternative": ["Real Name"],
            "subType": "memberOfParliament",
            "additionalInformation": {"epId": "28150"},
        },
    }
    # The same entity should be indexable by its epId regardless of how many
    # alias keys it appears under.
    idx = _build_ep_id_index(persons)
    assert idx == {"28150": persons["real name"]}

    speeches = [{
        "people": [{
            "label": "A Different Name",          # label match fails
            "additionalInformation": {"epId": "28150"},
        }],
    }]
    out = link_entities(speeches, persons, {})
    assert out[0]["people"][0]["wid"] == "Q42"
    assert out[0]["people"][0]["wtype"] == "PERSON"


def test_link_entities_label_fallback_still_works():
    """epId index empty → cleaned-label match is used as before."""
    from optv.shared.nel import link_entities

    persons = {
        "jane doe": {
            "id": "Q99", "label": "Jane Doe",
            "labelAlternative": [], "subType": "memberOfParliament",
            "additionalInformation": {},
        },
    }
    speeches = [{"people": [{"label": "Jane Doe"}]}]
    out = link_entities(speeches, persons, {})
    assert out[0]["people"][0]["wid"] == "Q99"
