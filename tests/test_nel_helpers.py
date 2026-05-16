"""Pure helpers from optv/shared/nel.py."""

import json

from optv.shared.nel import cleanup, get_nel_data, link_entities


def test_cleanup_normalises_whitespace_punctuation_accents():
    assert cleanup("Dr. Müller-Schmidt") == "dr muller schmidt"
    assert cleanup("  GÖRKE  ") == "gorke"
    assert cleanup("CDU/CSU") == "cdu csu"


def test_cleanup_returns_none_for_empty_or_dict():
    assert cleanup("") is None
    assert cleanup(None) is None
    assert cleanup({"label": "ignored"}) is None


def test_link_entities_fills_wid_for_known_person():
    persons = {"max beispiel": {"id": "Q42"}}
    factions = {"spd": {"id": "Q49768"}}
    speeches = [{
        "people": [{"label": "Max Beispiel", "faction": "SPD"}],
    }]
    out = link_entities(speeches, persons, factions)
    person = out[0]["people"][0]
    assert person["wid"] == "Q42"
    assert person["wtype"] == "PERSON"
    assert person["faction"] == {"label": "SPD", "wid": "Q49768", "wtype": "ORG"}


def test_link_entities_preserves_upstream_wid():
    """ParlaMint parser already sets wid; NEL must not overwrite it."""
    persons = {"max beispiel": {"id": "Q42"}}
    speeches = [{
        "people": [{"label": "Max Beispiel", "wid": "Q999", "wtype": "PERSON"}],
    }]
    out = link_entities(speeches, persons, {})
    assert out[0]["people"][0]["wid"] == "Q999"


def test_link_entities_handles_dict_faction_without_wid():
    factions = {"spd": {"id": "Q49768"}}
    speeches = [{
        "people": [{"label": "X", "faction": {"label": "SPD"}}],
    }]
    out = link_entities(speeches, {}, factions)
    assert out[0]["people"][0]["faction"]["wid"] == "Q49768"


def test_link_entities_no_match_writes_empty_wid_for_string_faction():
    speeches = [{
        "people": [{"label": "Unknown", "faction": "Unknown-Party"}],
    }]
    out = link_entities(speeches, {}, {})
    assert out[0]["people"][0]["faction"] == {
        "label": "Unknown-Party", "wid": "", "wtype": "ORG",
    }


def _write_entities(tmp_path, entities):
    (tmp_path / "entities.json").write_text(
        json.dumps({"meta": {}, "data": entities}), encoding="utf-8")
    return tmp_path


def _ent(qid, label, subType, alts=None):
    return {
        "id": qid, "label": label, "labelAlternative": alts or [],
        "type": "person", "subType": subType,
    }


def test_get_nel_data_loads_member_of_parliament_and_faction(tmp_path):
    _write_entities(tmp_path, [
        _ent("Q42", "Max Beispiel", "memberOfParliament"),
        {"id": "Q49768", "label": "SPD", "labelAlternative": ["SPD-Fraktion"],
         "type": "organisation", "subType": "faction"},
        _ent("Q1", "Some Party", "party"),
    ])
    persons, factions = get_nel_data(tmp_path)
    assert persons["max beispiel"]["id"] == "Q42"
    assert factions["spd"]["id"] == "Q49768"
    assert factions["spd fraktion"]["id"] == "Q49768"
    assert "some party" not in persons  # `party` subType still ignored


def test_get_nel_data_loads_person_subtype_as_fallback(tmp_path):
    """Non-MP speakers (Bundespräsident, ministers) link via subType `person`."""
    _write_entities(tmp_path, [
        _ent("Q76658", "Frank-Walter Steinmeier", "person"),
    ])
    persons, _ = get_nel_data(tmp_path)
    assert persons["frank walter steinmeier"]["id"] == "Q76658"


def test_get_nel_data_member_of_parliament_wins_label_collision(tmp_path):
    """A `person` entry must never override an MP claiming the same label."""
    _write_entities(tmp_path, [
        _ent("Q_MP", "Thomas Schmidt", "memberOfParliament"),
        _ent("Q_OTHER", "Thomas Schmidt", "person"),
    ])
    persons, _ = get_nel_data(tmp_path)
    assert persons["thomas schmidt"]["id"] == "Q_MP"
