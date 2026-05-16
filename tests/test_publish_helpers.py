"""Non-destructive publish helpers from optv/parliaments/DE/common.py."""

from optv.parliaments.DE.common import (
    data_has_timing, data_has_ner, is_demotion, carry_forward_wids,
)


def _speech(origin, people=None, debug=None):
    return {
        "originTextID": origin,
        "people": people or [],
        "debug": debug or {},
    }


def test_data_has_timing_and_ner_read_debug_durations():
    assert data_has_timing([_speech("A", debug={"align-duration": 12.0})])
    assert not data_has_timing([_speech("A", debug={"ner-duration": 3.0})])
    assert data_has_ner([_speech("A", debug={"ner-duration": 3.0})])
    assert not data_has_ner([_speech("A")])


def test_is_demotion_blocks_dropping_alignment_or_ner():
    aligned = [_speech("A", debug={"align-duration": 9.0})]
    nered = [_speech("A", debug={"align-duration": 9.0, "ner-duration": 3.0})]
    bare = [_speech("A")]
    # bare file would drop the published alignment -> demotion
    assert is_demotion(bare, aligned)
    # losing NER is also a demotion
    assert is_demotion(aligned, nered)


def test_is_demotion_allows_equal_or_richer_publish():
    aligned = [_speech("A", debug={"align-duration": 9.0})]
    nered = [_speech("A", debug={"align-duration": 9.0, "ner-duration": 3.0})]
    assert not is_demotion(aligned, aligned)       # same richness
    assert not is_demotion(nered, aligned)          # richer is fine
    assert not is_demotion(aligned, [])             # nothing published yet


def test_carry_forward_fills_missing_wid_from_published():
    published = [_speech("A", people=[
        {"label": "Max Beispiel", "wid": "Q42", "wtype": "PERSON"}])]
    new = [_speech("A", people=[{"label": "Max Beispiel"}])]
    carried = carry_forward_wids(new, published)
    assert carried == 1
    assert new[0]["people"][0]["wid"] == "Q42"
    assert new[0]["people"][0]["wtype"] == "PERSON"


def test_carry_forward_never_overwrites_an_existing_wid():
    published = [_speech("A", people=[{"label": "Max Beispiel", "wid": "Q42"}])]
    new = [_speech("A", people=[{"label": "Max Beispiel", "wid": "Q999"}])]
    assert carry_forward_wids(new, published) == 0
    assert new[0]["people"][0]["wid"] == "Q999"


def test_carry_forward_matches_speeches_by_origin_text_id():
    published = [_speech("A", people=[{"label": "X", "wid": "Q1"}])]
    # same label, different speech -> must not be carried
    new = [_speech("B", people=[{"label": "X"}])]
    assert carry_forward_wids(new, published) == 0
    assert "wid" not in new[0]["people"][0]


def test_carry_forward_enriches_dict_faction_wid():
    published = [_speech("A", people=[{
        "label": "X", "wid": "Q1",
        "faction": {"label": "SPD", "wid": "Q49768", "wtype": "ORG"}}])]
    new = [_speech("A", people=[{
        "label": "X", "wid": "Q1",
        "faction": {"label": "SPD", "wid": "", "wtype": "ORG"}}])]
    carry_forward_wids(new, published)
    assert new[0]["people"][0]["faction"]["wid"] == "Q49768"
