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
