"""Unit tests for cross-item semantic rules that aren't covered by the
example-validation suite. Currently focuses on the media.sourcePage
uniqueness rule (the platform keys speech identity on sourcePage)."""

from __future__ import annotations

from optv.shared.validators.semantic_validator import (
    _rule_media_source_page_unique,
    validate_semantic,
)


def _doc(pages):
    return {"meta": {"session": "x"},
            "data": [{"media": {"sourcePage": p}} for p in pages]}


def test_unique_source_pages_pass():
    doc = _doc(["https://x/?start=1", "https://x/?start=2", "https://x/?start=3"])
    assert _rule_media_source_page_unique(doc) == []


def test_duplicate_source_pages_flagged():
    doc = _doc(["https://x/sess", "https://x/sess", "https://x/sess"])
    findings = _rule_media_source_page_unique(doc)
    # Two duplicates of the first occurrence → two findings (data[1], data[2]).
    assert len(findings) == 2
    assert all(f["rule"] == "semantic.media.sourcePage.duplicate" for f in findings)
    assert all(f["severity"] == "warning" for f in findings)
    assert findings[0]["path"] == "data/1/media/sourcePage"


def test_empty_source_page_is_not_a_duplicate():
    # Missing/empty sourcePage is left to the schema minLength check, not here.
    doc = _doc(["", "", "https://x/?start=1"])
    assert _rule_media_source_page_unique(doc) == []


def test_rule_is_wired_into_validate_semantic():
    doc = _doc(["https://x/dup", "https://x/dup"])
    rules = {f["rule"] for f in validate_semantic(doc)}
    assert "semantic.media.sourcePage.duplicate" in rules


from optv.shared.validators.semantic_validator import (
    _rule_meta_no_per_speech_duplicates,
    _rule_text_speech_proceeding_index,
)


def test_meta_parliament_and_period_flagged_as_duplicates():
    doc = {"meta": {"session": "x", "parliament": "DE", "electoralPeriod": {"number": 17}},
           "data": [{"parliament": "DE", "electoralPeriod": {"number": 17}}]}
    findings = _rule_meta_no_per_speech_duplicates(doc)
    assert [f["rule"] for f in findings] == [
        "semantic.meta.duplicatesPerSpeech", "semantic.meta.duplicatesPerSpeech"]
    assert {f["path"] for f in findings} == {"meta/parliament", "meta/electoralPeriod"}


def test_meta_without_per_speech_keys_ok():
    doc = {"meta": {"session": "x", "schemaVersion": "1.0"},
           "data": [{"parliament": "DE", "electoralPeriod": {"number": 17}}]}
    assert _rule_meta_no_per_speech_duplicates(doc) == []


def test_proceeding_index_required_for_text_speech():
    doc = {"meta": {"session": "x"},
           "data": [{"textContents": [{"language": "de"}], "debug": {}}]}
    findings = _rule_text_speech_proceeding_index(doc)
    assert [f["rule"] for f in findings] == ["semantic.debug.proceedingIndex.missing"]


def test_proceeding_index_not_required_for_video_only():
    doc = {"meta": {"session": "x"}, "data": [{"textContents": [], "debug": {}}]}
    assert _rule_text_speech_proceeding_index(doc) == []


from optv.shared.validators.semantic_validator import _rule_origin_media_id_placement


def test_top_level_origin_media_id_flagged():
    doc = {"meta": {"session": "x"},
           "data": [{"originMediaID": "abc", "media": {"originMediaID": "abc"}}]}
    findings = _rule_origin_media_id_placement(doc)
    assert [f["rule"] for f in findings] == ["semantic.speech.originMediaID_misplaced"]


def test_media_only_origin_media_id_ok():
    doc = {"meta": {"session": "x"}, "data": [{"media": {"originMediaID": "abc"}}]}
    assert _rule_origin_media_id_placement(doc) == []
