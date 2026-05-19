"""Non-destructive publish helpers from optv/parliaments/DE/common.py."""

from optv.parliaments.DE.common import (
    data_has_timing, data_has_ner, is_demotion, carry_forward_wids,
    carry_forward_enrichments,
)


def _speech(origin, people=None, debug=None, agendaItem=None):
    s = {
        "originTextID": origin,
        "people": people or [],
        "debug": debug or {},
    }
    if agendaItem is not None:
        s["agendaItem"] = agendaItem
    return s


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


# carry_forward_enrichments -- per-speech derived fields are monotonic
# across a publish; stale code can't strip what newer code produced.


def test_enrichments_fill_missing_agenda_item_type():
    """Stale Pi cache scenario: new doc lacks agendaItem.type/nativeType,
    published already has them -> restore both from published."""
    published = [_speech("A",
        agendaItem={"title": "X", "type": "questioning_of_the_government",
                    "nativeType": "DE-questioning_of_the_government"})]
    new = [_speech("A", agendaItem={"title": "X"})]
    carried = carry_forward_enrichments(new, published)
    assert carried == 2
    assert new[0]["agendaItem"]["type"] == "questioning_of_the_government"
    assert new[0]["agendaItem"]["nativeType"] == "DE-questioning_of_the_government"


def test_enrichments_fill_missing_debug_confidence():
    published = [_speech("A", debug={"confidence": 0.5,
                                     "confidence_reason": "qa-agenda-type"})]
    new = [_speech("A", debug={"originalTitle": "..."})]
    carried = carry_forward_enrichments(new, published)
    assert carried == 2
    assert new[0]["debug"]["confidence"] == 0.5
    assert new[0]["debug"]["confidence_reason"] == "qa-agenda-type"


def test_enrichments_never_overwrite_when_new_data_has_value():
    """Newer code's value wins -- the parser is allowed to change its mind."""
    published = [_speech("A", agendaItem={"title": "X", "type": "regular"})]
    new = [_speech("A", agendaItem={"title": "X",
                                    "type": "questioning_of_the_government"})]
    carried = carry_forward_enrichments(new, published)
    assert carried == 0
    assert new[0]["agendaItem"]["type"] == "questioning_of_the_government"


def test_enrichments_match_speeches_by_origin_text_id():
    """Don't carry fields across to a different speech with the same key
    pattern -- speech identity is originTextID, not position."""
    published = [_speech("A", agendaItem={"title": "X", "type": "regular"})]
    new = [_speech("B", agendaItem={"title": "X"})]
    carried = carry_forward_enrichments(new, published)
    assert carried == 0
    assert "type" not in new[0]["agendaItem"]


def test_enrichments_no_published_speech_is_noop():
    """First-time publish: nothing to carry forward."""
    new = [_speech("A", agendaItem={"title": "X"})]
    assert carry_forward_enrichments(new, []) == 0


def test_enrichments_handle_missing_parent_dict_on_either_side():
    """Speech without agendaItem in new (or published) is skipped silently."""
    published = [_speech("A", agendaItem={"title": "X", "type": "regular"})]
    new = [_speech("A")]  # no agendaItem at all
    assert carry_forward_enrichments(new, published) == 0
    assert "agendaItem" not in new[0]
