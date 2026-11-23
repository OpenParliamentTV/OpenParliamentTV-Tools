"""The ``readiness`` block every manifest carries, and how a malformed one is reported."""

import pytest

from optv.parliaments import list_parliaments
from optv.scripts.readiness.config import DEFAULT_THRESHOLDS, load_declaration, parse_declaration

PARLIAMENTS = list_parliaments()


@pytest.mark.parametrize("parliament", PARLIAMENTS)
def test_every_parliament_has_a_well_formed_readiness_block(parliament):
    """How many sample sessions a parliament has is the scorecard's business; a parliament under construction has none yet."""
    declaration = load_declaration(parliament)
    assert declaration.errors == []
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


SAMPLES = ["18115", "18116", "18117"]


def test_a_sample_run_names_its_sessions_and_the_arguments_that_confine_it():
    declaration = parse_declaration({"sample_sessions": SAMPLES, "sample_run": [
        {"sessions": ["18115", "18116"], "args": ["--period=18", "--limit-session", "^1811[56]$"]},
        {"sessions": ["18117"], "args": ["--session=https://example.org/18117"]}]})
    assert declaration.errors == []
    assert [(run.sessions, run.args) for run in declaration.sample_runs] == [
        (("18115", "18116"), ("--period=18", "--limit-session", "^1811[56]$")),
        (("18117",), ("--session=https://example.org/18117",))]


@pytest.mark.parametrize("entry, error", [
    ({"sessions": SAMPLES, "args": ["--period=18"]},
     "sample_run 1: no argument confines the run to sessions (--limit-session, --session, --sitting, --kid)"),
    ({"sessions": ["18115", "18001"], "args": ["--limit-session", "^18"]},
     "sample_run 1: 18001 is not a sample session"),
    ({"sessions": SAMPLES, "args": ["--limit-session", "^1811", "--download-original"]},
     "sample_run 1: --download-original is not for a manifest to set"),
    ({"sessions": SAMPLES, "args": ["--limit-session", "^1811", "--force", "--rebuild"]},
     "sample_run 1: --force, --rebuild is not for a manifest to set"),
    ({"sessions": SAMPLES, "args": ["--sitting", 144]}, "sample_run 1: args is a list of strings"),
    ({"sessions": [], "args": ["--limit-session", "^1811"]},
     "sample_run 1: sessions is a list of quoted session keys"),
    ({"args": ["--limit-session", "^1811"]}, "sample_run 1: takes sessions and args"),
])
def test_a_sample_run_that_could_escape_its_sessions_is_rejected(entry, error):
    declaration = parse_declaration({"sample_sessions": SAMPLES, "sample_run": [entry]})
    assert declaration.errors == [error]
    assert declaration.sample_runs == []
