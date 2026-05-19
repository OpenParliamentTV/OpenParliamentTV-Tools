"""Pure helpers from optv/shared/nel.py."""

import json

from optv.shared.nel import (
    cleanup,
    get_nel_data,
    link_entities,
    link_entities_from_file,
)


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


def _write_session(path, *, nel_ts="2025-01-01T00:00:00", people=None, extra_meta=None):
    payload = {
        "meta": {
            "processing": {"nel": nel_ts, **(extra_meta or {})},
        },
        "data": [{"people": people or []}],
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def test_link_entities_from_file_is_idempotent_when_nothing_changes(tmp_path):
    """Re-running NEL on an already-linked file must not rewrite it.

    Regression: previously every NEL run bumped meta.processing.nel = now()
    and rewrote every session file unconditionally, producing 5-minute
    timestamp-only commits on the downstream Data repo.
    """
    src = tmp_path / "21001-session.json"
    _write_session(src, nel_ts="2025-01-01T00:00:00", people=[
        {"label": "Max Beispiel", "wid": "Q42", "wtype": "PERSON",
         "faction": {"label": "SPD", "wid": "Q49768", "wtype": "ORG"}},
    ])
    original_bytes = src.read_bytes()

    persons = {"max beispiel": {"id": "Q42"}}
    factions = {"spd": {"id": "Q49768"}}
    link_entities_from_file(src, src, persons, factions)

    assert src.read_bytes() == original_bytes, \
        "NEL rewrote the file even though nothing changed"


def test_link_entities_from_file_rewrites_when_entities_change(tmp_path):
    """When link_entities actually fills new wids, the file must be rewritten
    and meta.processing.nel bumped."""
    src = tmp_path / "21002-session.json"
    _write_session(src, nel_ts="2025-01-01T00:00:00", people=[
        {"label": "Max Beispiel"},  # no wid yet
    ])

    persons = {"max beispiel": {"id": "Q42"}}
    link_entities_from_file(src, src, persons, {})

    result = json.loads(src.read_text(encoding="utf-8"))
    assert result["data"][0]["people"][0]["wid"] == "Q42"
    assert result["meta"]["processing"]["nel"] != "2025-01-01T00:00:00"


def test_link_entities_from_file_writes_first_run_without_prior_nel(tmp_path):
    """Files arriving without a previous nel timestamp must always be written,
    even if link_entities had nothing to fill (the timestamp records the run)."""
    src = tmp_path / "21003-session.json"
    payload = {"meta": {"processing": {}}, "data": [{"people": []}]}
    src.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    link_entities_from_file(src, src, {}, {})

    result = json.loads(src.read_text(encoding="utf-8"))
    assert "nel" in result["meta"]["processing"]
