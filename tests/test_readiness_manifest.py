"""The ``readiness`` block every manifest carries, and how a malformed one is reported."""

import pytest

from optv.parliaments import list_parliaments
from optv.scripts.readiness.config import (DEFAULT_THRESHOLDS, load_declaration, parse_declaration)

PARLIAMENTS = list_parliaments()


@pytest.mark.parametrize("parliament", PARLIAMENTS)
def test_every_parliament_declares_its_sample_sessions(parliament):
    declaration = load_declaration(parliament)
    assert declaration.errors == []
    assert len(declaration.samples) >= DEFAULT_THRESHOLDS["min_samples"]
    assert all(isinstance(sample.id, str) and sample.id for sample in declaration.samples)


def test_the_reference_parliament_is_declared_live():
    declaration = load_declaration("DE")
    assert declaration.is_live
    assert declaration.attestations["backfilled"]["periods"]


def test_a_sample_can_be_declared_without_text():
    declaration = parse_declaration({"sample_sessions": [
        "19078", {"id": "18009", "expect_text": False, "note": "video only"}]})
    assert declaration.errors == []
    assert [(s.id, s.expect_text) for s in declaration.samples] == [("19078", True), ("18009", False)]


def test_thresholds_are_overridden_only_with_a_reason():
    declaration = parse_declaration({
        "sample_sessions": ["1"],
        "thresholds": {"speaker_wid_share": {"value": 0.8, "reason": "chair is not a person"},
                       "timing_share": {"value": 0.5}}})
    assert declaration.thresholds["speaker_wid_share"] == 0.8
    assert declaration.thresholds["timing_share"] == DEFAULT_THRESHOLDS["timing_share"]
    assert declaration.overrides == {"speaker_wid_share": {"value": 0.8, "reason": "chair is not a person"}}
    assert declaration.errors == ["threshold 'timing_share' needs a numeric value and a reason"]


@pytest.mark.parametrize("block, error", [
    (None, "no readiness block"),
    ({"sample_sessions": [18115]}, "sample 18115: the session key must be a quoted string"),
    ({"sample_sessions": ["1", "1"]}, "sample '1' is listed twice"),
    ({"sample_sessions": ["1"], "samples": []}, "unknown keys: samples"),
    ({"sample_sessions": ["1"], "attestations": {"approved": {}}}, "unknown attestation 'approved'"),
    ({"sample_sessions": ["1"], "attestations": {"live": True}}, "attestation 'live' is not a mapping"),
    ({"sample_sessions": ["1"], "thresholds": {"speed": {"value": 1, "reason": "x"}}},
     "unknown threshold 'speed'"),
    ({"sample_sessions": ["1"], "whisper": {"model": "tiny"}}, "whisper takes only a language"),
])
def test_a_malformed_block_is_reported_not_raised(block, error):
    assert error in parse_declaration(block).errors


def test_attestations_and_whisper_language_are_read():
    declaration = parse_declaration({"sample_sessions": ["1"], "whisper": {"language": "no"},
                                     "attestations": {"live": {"url": "https://example.org"}}})
    assert declaration.errors == []
    assert declaration.whisper_language == "no"
    assert declaration.is_live
