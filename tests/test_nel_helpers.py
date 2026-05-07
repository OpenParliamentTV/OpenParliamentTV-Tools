"""Pure helpers from optv/shared/nel.py."""

from optv.shared.nel import cleanup, link_entities


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
