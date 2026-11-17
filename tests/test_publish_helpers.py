"""Non-destructive publish helpers from optv/shared/publish.py."""

from optv.shared.publish import (
    data_has_timing, data_has_ner, data_has_documents, is_demotion,
    carry_forward_wids, carry_forward_enrichments, carry_forward_documents,
    carry_forward_text_enrichment, text_signature,
)


def _speech(origin, people=None, debug=None, agendaItem=None, documents=None):
    s = {
        "originTextID": origin,
        "people": people or [],
        "debug": debug or {},
    }
    if agendaItem is not None:
        s["agendaItem"] = agendaItem
    if documents is not None:
        s["documents"] = documents
    return s


def _tspeech(origin, sentences, debug=None, documents=None):
    """Speech carrying transcript text (textContents/textBody/sentences)."""
    s = _speech(origin, debug=debug, documents=documents)
    s["textContents"] = [{"textBody": [{
        "type": "speech",
        "sentences": [{"text": t} for t in sentences],
    }]}]
    return s


_DOC = {"type": "officialDocument", "label": "Drucksache 21/1100",
        "sourceURI": "https://dserver.bundestag.de/btd/21/011/2101100.pdf"}


def test_data_has_timing_and_ner_read_debug_durations():
    assert data_has_timing([_speech("A", debug={"alignDuration": 12.0})])
    assert not data_has_timing([_speech("A", debug={"nerDuration": 3.0})])
    assert data_has_ner([_speech("A", debug={"nerDuration": 3.0})])
    assert not data_has_ner([_speech("A")])


def test_is_demotion_blocks_dropping_alignment_or_ner():
    aligned = [_speech("A", debug={"alignDuration": 9.0})]
    nered = [_speech("A", debug={"alignDuration": 9.0, "nerDuration": 3.0})]
    bare = [_speech("A")]
    # bare file would drop the published alignment -> demotion
    assert is_demotion(bare, aligned)
    # losing NER is also a demotion
    assert is_demotion(aligned, nered)


def test_is_demotion_allows_equal_or_richer_publish():
    aligned = [_speech("A", debug={"alignDuration": 9.0})]
    nered = [_speech("A", debug={"alignDuration": 9.0, "nerDuration": 3.0})]
    assert not is_demotion(aligned, aligned)       # same richness
    assert not is_demotion(nered, aligned)          # richer is fine
    assert not is_demotion(aligned, [])             # nothing published yet


def test_is_demotion_allows_text_over_textless_with_empty_ner_stamp():
    # NER over a media-only session stamps nerDuration with no text; the
    # transcript arriving later must still publish (it was refused forever).
    textless = [_speech("A", debug={"nerDuration": 2e-07}), _speech("B")]
    with_text = [_tspeech("A", ["Guten Morgen."]), _tspeech("B", ["Danke."])]
    assert not is_demotion(with_text, textless)
    assert not is_demotion(with_text, textless, allow_text_replace=True)
    # ...but document links are independent of the text and stay guarded.
    textless_docs = [_speech("A", debug={"nerDuration": 2e-07}, documents=[_DOC])]
    with_text_nodocs = [_tspeech("A", ["Guten Morgen."], documents=[])]
    assert is_demotion(with_text_nodocs, textless_docs)


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


def test_carry_forward_keeps_faction_wid_of_an_unlinked_speaker():
    # NEL linked the party but not the person: the faction wid must survive
    # a publish from a cache that predates NEL.
    published = [_speech("A", people=[{
        "label": "Eva Exemplo", "faction": {"label": "PSD", "wid": "Q595575"}}])]
    new = [_speech("A", people=[{
        "label": "Eva Exemplo", "faction": {"label": "PSD"}}])]
    assert carry_forward_wids(new, published) == 1
    assert new[0]["people"][0]["faction"]["wid"] == "Q595575"
    assert "wid" not in new[0]["people"][0]


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
                                     "confidenceReason": "qa-agenda-type"})]
    new = [_speech("A", debug={"originalTitle": "..."})]
    carried = carry_forward_enrichments(new, published)
    assert carried == 2
    assert new[0]["debug"]["confidence"] == 0.5
    assert new[0]["debug"]["confidenceReason"] == "qa-agenda-type"


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


# documents -- linked official documents are append-only across a publish, so a
# stale Tools checkout (pre-period-21 parser) or document-less local cache on
# another machine can never empty a session that already has them.


def test_data_has_documents_detects_any_nonempty_speech():
    assert data_has_documents([_speech("A", documents=[_DOC])])
    assert not data_has_documents([_speech("A", documents=[])])
    assert not data_has_documents([_speech("A")])


def test_is_demotion_blocks_dropping_documents():
    withdocs = [_speech("A", documents=[_DOC])]
    bare = [_speech("A", documents=[])]
    assert is_demotion(bare, withdocs)
    assert not is_demotion(withdocs, withdocs)
    assert not is_demotion(withdocs, bare)   # gaining documents is fine


def test_carry_forward_documents_fills_empty_from_published():
    published = [_speech("A", documents=[_DOC])]
    new = [_speech("A", documents=[])]
    assert carry_forward_documents(new, published) == 1
    assert new[0]["documents"] == [_DOC]


def test_carry_forward_documents_never_overwrites_existing():
    """Newer data with its own documents wins -- the parser may re-extract."""
    other = {"type": "officialDocument", "label": "Drucksache 21/2"}
    published = [_speech("A", documents=[_DOC])]
    new = [_speech("A", documents=[other])]
    assert carry_forward_documents(new, published) == 0
    assert new[0]["documents"] == [other]


def test_carry_forward_documents_matches_by_origin_text_id():
    published = [_speech("A", documents=[_DOC])]
    new = [_speech("B", documents=[])]
    assert carry_forward_documents(new, published) == 0
    assert new[0]["documents"] == []


# text_signature + content-aware demotion -- the --rebuild path. A *content*
# change to the transcript (e.g. new sentence-splitting) may replace stale
# timing/NER under rebuild, but the transcript can never be emptied.


def test_text_signature_ignores_enrichment_changes():
    """Adding timing/entities to the same sentences must not change the sig."""
    a = [_tspeech("A", ["Hallo Welt."])]
    b = [_tspeech("A", ["Hallo Welt."], debug={"alignDuration": 5.0})]
    b[0]["textContents"][0]["textBody"][0]["sentences"][0]["entities"] = [{"wid": "Q1"}]
    b[0]["textContents"][0]["textBody"][0]["sentences"][0]["timeStart"] = 1.0
    assert text_signature(a) == text_signature(b)


def test_text_signature_changes_when_splitting_changes():
    one = [_tspeech("A", ["Hallo Welt."])]
    two = [_tspeech("A", ["Hallo", "Welt."])]  # different sentence boundaries
    assert text_signature(one) != text_signature(two)


def test_rebuild_never_empties_a_published_transcript():
    """Present→absent text is always a demotion, even under --rebuild
    (the irreversible media-only / crash data-loss case)."""
    published = [_tspeech("A", ["Hallo Welt."])]
    bare = [_speech("A")]  # media-only re-merge: no textContents
    assert is_demotion(bare, published)
    assert is_demotion(bare, published, allow_text_replace=True)


def test_changed_text_blocked_normally_but_allowed_under_rebuild():
    published = [_tspeech("A", ["Hallo Welt."],
                          debug={"alignDuration": 5.0, "nerDuration": 1.0})]
    rebuilt = [_tspeech("A", ["Hallo", "Welt."])]  # new splitting, not yet re-derived
    assert is_demotion(rebuilt, published)                          # would drop timing/NER
    assert not is_demotion(rebuilt, published, allow_text_replace=True)  # rebuild replaces


def test_unchanged_text_missing_ner_blocked_even_under_rebuild():
    """Same transcript content → the enrichment guards still apply, so a
    bare re-derive that lost NER is refused regardless of the flag."""
    published = [_tspeech("A", ["Hallo Welt."], debug={"nerDuration": 1.0})]
    new = [_tspeech("A", ["Hallo Welt."])]  # identical text, no NER
    assert is_demotion(new, published)
    assert is_demotion(new, published, allow_text_replace=True)


def _aligned(origin, texts, video="https://cdn/a.m3u8"):
    """Published speech with sentence timings, entities and align/NER markers."""
    s = _tspeech(origin, texts, debug={"alignDuration": 5.0, "nerDuration": 1.0})
    for i, sent in enumerate(s["textContents"][0]["textBody"][0]["sentences"]):
        sent.update(timeStart=float(i), timeEnd=i + 0.5,
                    entities=[{"label": "Bayern", "wid": "Q980"}])
    s["media"] = {"videoFileURI": video, "aligned": True}
    return s


def test_text_enrichment_kept_for_unchanged_speech():
    published = [_aligned("A", ["Hallo Welt.", "Servus."])]
    new = [_tspeech("A", ["Hallo Welt.", "Servus."])]
    new[0]["media"] = {"videoFileURI": "https://cdn/a.m3u8"}
    assert carry_forward_text_enrichment(new, published) == 1
    sents = new[0]["textContents"][0]["textBody"][0]["sentences"]
    assert [(x["timeStart"], x["timeEnd"]) for x in sents] == [(0.0, 0.5), (1.0, 1.5)]
    assert sents[0]["entities"] == [{"label": "Bayern", "wid": "Q980"}]
    assert new[0]["debug"]["alignDuration"] == 5.0 and new[0]["debug"]["nerDuration"] == 1.0
    assert new[0]["media"]["aligned"] is True
    assert not is_demotion(new, published)


def test_text_enrichment_not_kept_when_text_or_media_changed():
    published = [_aligned("A", ["Hallo Welt."]), _aligned("B", ["Servus."])]
    new = [_tspeech("A", ["Hallo, Welt."]), _tspeech("B", ["Servus."])]
    new[0]["media"] = {"videoFileURI": "https://cdn/a.m3u8"}
    new[1]["media"] = {"videoFileURI": "https://cdn/other.m3u8"}  # another clip
    assert carry_forward_text_enrichment(new, published) == 0
    assert "timeStart" not in new[0]["textContents"][0]["textBody"][0]["sentences"][0]
    assert "alignDuration" not in new[1]["debug"]


def test_text_enrichment_not_kept_when_start_offset_moved():
    """A speech cut from a session-wide stream: same URL, same text, but a
    corrected start offset means the published timings describe other audio."""
    published = [_aligned("A", ["Hallo Welt."]), _aligned("B", ["Servus."])]
    published[0]["media"]["additionalInformation"] = {"startOffset": 893.15}
    published[1]["media"]["additionalInformation"] = {"startOffset": 900.0}
    new = [_tspeech("A", ["Hallo Welt."]), _tspeech("B", ["Servus."])]
    new[0]["media"] = {"videoFileURI": "https://cdn/a.m3u8",
                       "additionalInformation": {"startOffset": 891.05}}
    new[1]["media"] = {"videoFileURI": "https://cdn/a.m3u8",
                       "additionalInformation": {"startOffset": 900.0}}
    assert carry_forward_text_enrichment(new, published) == 1
    assert "timeStart" not in new[0]["textContents"][0]["textBody"][0]["sentences"][0]
    assert "alignDuration" not in (new[0].get("debug") or {})
    assert new[1]["textContents"][0]["textBody"][0]["sentences"][0]["timeStart"] == 0.0


def test_text_enrichment_never_overwrites_new_values():
    published = [_aligned("A", ["Hallo Welt."])]
    new = [_aligned("A", ["Hallo Welt."])]
    new[0]["textContents"][0]["textBody"][0]["sentences"][0]["timeStart"] = 7.0
    carry_forward_text_enrichment(new, published)
    assert new[0]["textContents"][0]["textBody"][0]["sentences"][0]["timeStart"] == 7.0


def test_text_enrichment_keeps_no_stale_align_error_on_a_realigned_speech():
    # The published run failed to align the speech; the new run aligned it.
    published = [_tspeech("A", ["Hallo Welt."])]
    published[0]["media"] = {"videoFileURI": "https://cdn/a.m3u8"}
    published[0].setdefault("debug", {})["alignError"] = "timeout"
    new = [_aligned("A", ["Hallo Welt."])]
    new[0]["debug"].pop("nerDuration")
    carry_forward_text_enrichment(new, published)
    assert "alignError" not in new[0]["debug"]
