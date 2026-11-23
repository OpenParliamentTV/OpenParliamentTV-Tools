"""The readiness scorecard on synthetic sample sessions: each criterion, the level they add up to, and what gets written."""

import json

import pytest

from optv.parliaments import list_parliaments
from optv.scripts.readiness import overview, scorecard
from optv.scripts.readiness.config import parse_declaration
from optv.scripts.readiness.criteria import CATALOGUE, own_test_modules
from optv.shared.config import BaseConfig
from optv.shared.whisper_diff import write_session_qc
from tests.readiness_fixtures import (ROOT, clean_session, make_session, make_speech, stage_clips,
                                      write_session, write_transcripts)

SESSIONS = {"19115": 115, "19116": 116, "19117": 117}


def _publish(data_dir, docs):
    for session, doc in docs.items():
        write_session(data_dir, session, doc)


def _clean_docs(**speech):
    return {session: clean_session(session, number, **speech) for session, number in SESSIONS.items()}


def _evaluate(data_dir, docs, *, previous=None, **block):
    declaration = parse_declaration({"sample_sessions": list(docs), **block})
    assert declaration.errors == []
    return scorecard.evaluate("DE", config=BaseConfig(data_dir), declaration=declaration,
                              previous=previous if previous is not None else {})


def _strip_speaker_ids(doc):
    """Leave only the first speech of a session with a Wikidata ID: the session still counts as linked."""
    for speech in doc["data"][1:]:
        speech["people"][0].pop("wid", None)


def _transcribe_all(data_dir, docs, **kwargs):
    for session, doc in docs.items():
        stage_clips(data_dir, doc)
        write_transcripts(data_dir, session, doc, **kwargs)
        write_session_qc(data_dir, session)


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path / "OpenParliamentTV-Data-XX"


def test_clean_samples_reach_level_two_and_wait_for_whisper(data_dir):
    docs = _clean_docs()
    _publish(data_dir, docs)
    card = _evaluate(data_dir, docs)
    failing = {cid: entry for cid, entry in card["criteria"].items()
               if entry["level"] == 2 and entry["status"] != "pass"}
    assert failing == {}
    assert (card["level"], card["level_name"], card["status"]) == (2, "samples pass", "in development")
    assert card["blocking"] == ["qc.whisper_coverage", "qc.fidelity", "qc.mismatch", "qc.reviewed",
                                "release.licence", "release.platform"]
    assert card["criteria"]["qc.whisper_coverage"] == {
        "level": 3, "status": "not_evaluated", "detail": "no Whisper transcripts on this machine"}
    assert card["samples"]["19115"]["speeches"] == 4
    assert card["samples"]["19115"]["audio_seconds"] == 240


def test_whisper_evidence_and_attestations_reach_level_three(data_dir):
    docs = _clean_docs()
    _publish(data_dir, docs)
    _transcribe_all(data_dir, docs)
    card = _evaluate(data_dir, docs)
    assert card["criteria"]["qc.whisper_coverage"]["status"] == "pass"
    assert card["criteria"]["qc.fidelity"]["metrics"]["median_similarity"] == 1.0
    assert card["criteria"]["qc.mismatch"]["metrics"]["share"] == 0.0
    assert card["blocking"] == ["qc.reviewed", "release.licence", "release.platform"]

    attested = {"qc_reviewed": {"evidence": card["qc_evidence"]}, "licence_cleared": {},
                "platform_ready": {}}
    card = _evaluate(data_dir, docs, attestations=attested)
    assert (card["level"], card["level_name"]) == (3, "QC cleared")
    assert card["blocking"] == ["release.backfilled"]
    assert card["regressions"] == card["unverified"] == []


def test_a_review_of_other_evidence_is_stale(data_dir):
    docs = _clean_docs()
    _publish(data_dir, docs)
    _transcribe_all(data_dir, docs)
    card = _evaluate(data_dir, docs, attestations={"qc_reviewed": {"evidence": "0123456789abcdef"}})
    assert card["criteria"]["qc.reviewed"]["status"] == "stale"


def test_missing_transcripts_fail_the_coverage(data_dir):
    docs = _clean_docs()
    _publish(data_dir, docs)
    for session, doc in docs.items():
        write_transcripts(data_dir, session, doc, skip=(2, 3) if session == "19116" else ())
    coverage = _evaluate(data_dir, docs)["criteria"]["qc.whisper_coverage"]
    assert coverage["status"] == "fail"
    assert coverage["metrics"] == {"share": 0.833, "transcribed": 10, "expected": 12, "threshold": 0.95}
    assert coverage["detail"] == "19116: 2 of 4"


def test_text_that_does_not_match_the_audio_fails_fidelity(data_dir):
    docs = _clean_docs()
    _publish(data_dir, docs)
    _transcribe_all(data_dir, docs, text="Etwas völlig anderes wurde in diesem Ausschnitt gesagt.")
    card = _evaluate(data_dir, docs)
    assert card["criteria"]["qc.fidelity"]["status"] == "fail"
    assert card["criteria"]["qc.mismatch"]["status"] == "fail"
    assert card["criteria"]["qc.mismatch"]["metrics"]["mismatched"] == 12


def test_figures_go_stale_when_the_text_changes(data_dir):
    docs = _clean_docs()
    _publish(data_dir, docs)
    _transcribe_all(data_dir, docs)
    docs["19117"]["data"][0]["textContents"][0]["textBody"][0]["sentences"][0]["text"] = "Neu gemerged."
    _publish(data_dir, docs)
    card = _evaluate(data_dir, docs)
    assert card["criteria"]["qc.fidelity"]["status"] == "stale"
    assert "19117" in card["criteria"]["qc.fidelity"]["detail"]
    assert card["criteria"]["qc.mismatch"]["status"] == "stale"


def test_transcripts_outdated_by_a_re_merge_count_as_missing(data_dir):
    docs = _clean_docs()
    _publish(data_dir, docs)
    _transcribe_all(data_dir, docs)
    # 19117 was transcribed when its speeches were cut differently.
    write_transcripts(data_dir, "19117", clean_session("19117", 117, duration=400.0))
    write_session_qc(data_dir, "19117")
    card = _evaluate(data_dir, docs)
    coverage = card["criteria"]["qc.whisper_coverage"]
    assert coverage["status"] == "fail"
    assert coverage["metrics"]["transcribed"] == 8
    assert coverage["detail"] == "19117: 0 of 4 (4 transcripts outdated by a re-merge)"
    assert card["criteria"]["qc.fidelity"]["status"] == "not_evaluated"
    assert card["criteria"]["qc.fidelity"]["detail"] == "no QC figures for 19117"


def test_a_session_whose_transcribed_speeches_are_all_gated_cannot_be_judged(data_dir):
    docs = _clean_docs()
    for speech in docs["19117"]["data"]:
        speech["debug"]["confidence"] = 0.5
    _publish(data_dir, docs)
    _transcribe_all(data_dir, docs)
    fidelity = _evaluate(data_dir, docs)["criteria"]["qc.fidelity"]
    assert fidelity["status"] == "not_evaluated"
    assert fidelity["detail"] == "no transcribed speech of 19117 is long enough and imported with text"
    assert sorted(fidelity["metrics"]["median_similarity"]) == ["19115", "19116"]


def test_sessions_without_figures_are_not_evaluated(data_dir):
    docs = _clean_docs()
    _publish(data_dir, docs)
    for session, doc in docs.items():
        write_transcripts(data_dir, session, doc)
    write_session_qc(data_dir, "19115")
    fidelity = _evaluate(data_dir, docs)["criteria"]["qc.fidelity"]
    assert fidelity["status"] == "not_evaluated"
    assert fidelity["detail"] == "no QC figures for 19116, 19117"
    assert fidelity["metrics"] == {"median_similarity": {"19115": 1.0}}


def test_low_confidence_text_leaks_without_linked_media_indexes(data_dir):
    docs = _clean_docs(linked=None)
    docs["19116"]["data"][1]["debug"]["confidence"] = 0.5
    docs["19116"]["data"][2]["debug"]["confidence"] = 0.5
    _publish(data_dir, docs)
    gate = _evaluate(data_dir, docs)["criteria"]["samples.import_gate"]
    assert gate["status"] == "fail"
    assert gate["metrics"] == {"mode": "confidence-only", "text_speeches": 12, "leaks": 2}
    assert "19116: 2" in gate["detail"]


def test_the_gate_holds_when_both_fields_are_present(data_dir):
    docs = _clean_docs()
    docs["19116"]["data"][1]["debug"]["confidence"] = 0.5
    _publish(data_dir, docs)
    gate = _evaluate(data_dir, docs)["criteria"]["samples.import_gate"]
    assert gate["status"] == "pass"
    assert gate["metrics"] == {"mode": "gated", "text_speeches": 12, "importable_share": 0.917}


def test_the_gate_does_not_apply_without_confidence(data_dir):
    docs = _clean_docs(confidence=None, linked=None)
    _publish(data_dir, docs)
    card = _evaluate(data_dir, docs)
    assert card["criteria"]["samples.import_gate"]["status"] == "not_applicable"
    assert card["level"] == 2


def test_speaker_ids_below_the_threshold_block_level_two(data_dir):
    docs = _clean_docs()
    _strip_speaker_ids(docs["19115"])
    _publish(data_dir, docs)
    card = _evaluate(data_dir, docs)
    assert card["level"] == 1
    assert card["blocking"] == ["samples.speaker_wid"]
    assert card["criteria"]["samples.speaker_wid"]["metrics"] == {
        "share": 0.75, "with_wid": 9, "speeches": 12, "threshold": 0.95}

    override = {"speaker_wid_share": {"value": 0.6, "reason": "the chair is not a person here"}}
    card = _evaluate(data_dir, docs, thresholds=override)
    assert card["level"] == 2
    assert card["thresholds"] == override


def test_a_stage_that_never_ran_is_named(data_dir):
    docs = _clean_docs()
    docs["19117"] = clean_session("19117", 117, ner=False)
    _publish(data_dir, docs)
    stages = _evaluate(data_dir, docs)["criteria"]["samples.stages"]
    assert stages["status"] == "fail"
    assert stages["detail"] == "19117: no ner"
    assert stages["metrics"] == {"complete": 2, "sessions": 3}


def test_a_sample_declared_without_text_needs_no_text_stages(data_dir):
    docs = _clean_docs()
    docs["19118"] = clean_session("19118", 118, sentences=0, ner=False)
    _publish(data_dir, docs)
    block = {"sample_sessions": ["19115", "19116", "19117", {"id": "19118", "expect_text": False}]}
    declaration = parse_declaration(block)
    card = scorecard.evaluate("DE", config=BaseConfig(data_dir), declaration=declaration, previous={})
    assert card["criteria"]["samples.stages"]["status"] == "pass"
    assert card["samples"]["19118"] == {"speeches": 4, "text_speeches": 0, "audio_seconds": 240,
                                        "text": card["samples"]["19118"]["text"], "expect_text": False}


def test_an_unpublished_sample_fails_the_declaration(data_dir):
    docs = _clean_docs()
    _publish(data_dir, docs)
    declaration = parse_declaration({"sample_sessions": [*docs, "19999"]})
    card = scorecard.evaluate("DE", config=BaseConfig(data_dir), declaration=declaration, previous={})
    assert card["criteria"]["samples.declared"]["status"] == "fail"
    assert card["criteria"]["samples.declared"]["detail"] == "not published: 19999"
    two = parse_declaration({"sample_sessions": ["19115", "19116"]})
    card = scorecard.evaluate("DE", config=BaseConfig(data_dir), declaration=two, previous={})
    assert card["criteria"]["samples.declared"]["detail"] == "2 declared, 3 needed"


def test_more_text_than_a_debate_clip_can_hold_is_implausible(data_dir):
    def session(agenda_type, confidence=1):
        speeches = [make_speech(i, session_number=115) for i in (1, 2, 3)]
        speeches.append(make_speech(4, session_number=115, sentences=40, duration=10.0,
                                    agenda_type=agenda_type, confidence=confidence))
        return make_session("19115", speeches)

    def result(doc):
        docs = _clean_docs()
        docs["19115"] = doc
        _publish(data_dir, docs)
        return _evaluate(data_dir, docs)["criteria"]["samples.text_plausible"]

    debate = result(session("regular"))
    assert debate["status"] == "fail"
    assert debate["metrics"]["implausible"] == 1
    assert debate["detail"].startswith("19115#4: ")
    # Chair text of an opening is long for its clip without being wrong.
    assert result(session("opening"))["status"] == "pass"
    # A speech the merger marked is the import gate's concern.
    assert result(session("regular", confidence=0.5))["status"] == "pass"


def test_a_speech_the_importer_would_reject_is_listed(data_dir):
    docs = _clean_docs()
    docs["19116"]["data"][2]["media"]["sourcePage"] = ""
    _publish(data_dir, docs)
    fields = _evaluate(data_dir, docs)["criteria"]["samples.import_fields"]
    assert fields["status"] == "fail"
    assert fields["detail"] == "19116#3: media.sourcePage"


def test_validator_errors_and_out_of_bounds_times_fail(data_dir):
    docs = _clean_docs()
    last = docs["19115"]["data"][0]["textContents"][0]["textBody"][0]["sentences"][-1]
    last["timeEnd"] = "500.000"
    del docs["19116"]["data"][0]["media"]["videoFileURI"]
    _publish(data_dir, docs)
    valid = _evaluate(data_dir, docs)["criteria"]["samples.valid"]
    assert valid["status"] == "fail"
    assert valid["metrics"]["time_bounds"] == 1
    assert valid["metrics"]["errors"] >= 1
    assert "19115: 1" in valid["detail"]


def test_untimed_sentences_and_missing_rights_fail(data_dir):
    docs = _clean_docs()
    docs["19117"] = clean_session("19117", 117, timed=False)
    docs["19116"]["data"][0]["media"]["license"] = ""
    _publish(data_dir, docs)
    card = _evaluate(data_dir, docs)
    assert card["criteria"]["samples.timing"]["metrics"] == {
        "share": 0.667, "timed": 48, "sentences": 72, "threshold": 0.95}
    assert card["criteria"]["samples.timing"]["status"] == "fail"
    assert card["criteria"]["samples.rights"]["detail"] == "media.license x1"


def test_a_declared_level_survives_a_failing_criterion(data_dir):
    docs = _clean_docs()
    _strip_speaker_ids(docs["19115"])
    _publish(data_dir, docs)
    card = _evaluate(data_dir, docs, attestations={"backfilled": {"periods": [19]},
                                                   "live": {"url": "https://example.org"}})
    assert (card["level"], card["level_name"], card["status"]) == (5, "live", "production")
    assert card["blocking"] == []
    assert card["regressions"] == ["samples.speaker_wid"]
    assert "qc.whisper_coverage" in card["unverified"]

    card = _evaluate(data_dir, docs, attestations={"backfilled": {"periods": [19]}})
    assert (card["level"], card["status"]) == (4, "in development")
    assert card["blocking"] == ["release.live"]
    assert card["regressions"] == ["samples.speaker_wid"]


def _criteria(**statuses):
    return {criterion.id: {"level": criterion.level, "status": statuses.get(criterion.id, "pass")}
            for criterion in CATALOGUE}


def test_levels_are_cumulative_up_to_three_and_declared_above():
    unattested = {"release.backfilled": "not_evaluated", "release.live": "not_evaluated"}
    assert scorecard.derive_level(_criteria(**unattested))["level"] == 3
    # A satisfied level three does not help while level two is open.
    assert scorecard.derive_level(_criteria(**unattested, **{"impl.tests": "fail"})) == {
        "level": 1, "level_name": "implemented", "blocking": ["impl.tests"], "regressions": [],
        "unverified": []}
    stale = scorecard.derive_level(_criteria(**unattested, **{"qc.fidelity": "stale"}))
    assert (stale["level"], stale["blocking"]) == (2, ["qc.fidelity"])
    not_applicable = scorecard.derive_level(_criteria(**unattested, **{"samples.import_gate": "not_applicable"}))
    assert not_applicable["level"] == 3
    live_only = scorecard.derive_level(_criteria(**{"release.backfilled": "not_evaluated"}))
    assert (live_only["level"], live_only["unverified"]) == (5, ["release.backfilled"])


def test_results_are_kept_when_the_data_is_not_on_this_machine(data_dir, tmp_path):
    docs = _clean_docs()
    _publish(data_dir, docs)
    _transcribe_all(data_dir, docs)
    recorded = _evaluate(data_dir, docs)
    declaration = parse_declaration({"sample_sessions": list(docs)})
    elsewhere = scorecard.evaluate("DE", data_dir=tmp_path / "absent", declaration=declaration,
                                   previous=recorded)
    assert elsewhere == recorded
    # With nothing recorded, the same criteria are simply not evaluated.
    fresh = scorecard.evaluate("DE", data_dir=tmp_path / "absent", declaration=declaration, previous={})
    assert fresh["criteria"]["samples.valid"] == {
        "level": 2, "status": "not_evaluated", "detail": "no data repository on this machine"}
    assert fresh["criteria"]["impl.files"]["status"] == "pass"
    assert fresh["level"] == 1


def test_a_scorecard_is_rewritten_only_when_a_result_changes(data_dir, tmp_path):
    docs = _clean_docs()
    _publish(data_dir, docs)
    card = _evaluate(data_dir, docs)
    path = scorecard.scorecard_path("DE", tmp_path)
    assert scorecard.write_if_changed(card, tools=tmp_path, today="2026-01-01", revision="aaaaaaa")
    first = path.read_bytes()
    assert json.loads(first)["evaluated"] == {"date": "2026-01-01", "tools": "aaaaaaa"}

    again = _evaluate(data_dir, docs)
    assert not scorecard.write_if_changed(again, tools=tmp_path, today="2026-02-02", revision="bbbbbbb")
    assert path.read_bytes() == first
    assert again["evaluated"] == {"date": "2026-01-01", "tools": "aaaaaaa"}

    _strip_speaker_ids(docs["19115"])
    _publish(data_dir, docs)
    changed = _evaluate(data_dir, docs)
    assert scorecard.write_if_changed(changed, tools=tmp_path, today="2026-02-02", revision="bbbbbbb")
    assert json.loads(path.read_text())["evaluated"] == {"date": "2026-02-02", "tools": "bbbbbbb"}


def test_the_overview_lists_every_scorecard(data_dir, tmp_path):
    docs = _clean_docs()
    _strip_speaker_ids(docs["19115"])
    _publish(data_dir, docs)
    card = _evaluate(data_dir, docs)
    scorecard.write_if_changed(card, tools=tmp_path, today="2026-01-01", revision="aaaaaaa")
    assert not overview.is_current(tmp_path)
    assert overview.write(tmp_path)
    assert not overview.write(tmp_path)
    assert overview.is_current(tmp_path)
    page = (tmp_path / "docs" / "READINESS.md").read_text()
    assert "| [`DE`](../optv/parliaments/DE/) | Deutscher Bundestag | 1 implemented | in development | `samples.speaker_wid` | – |" in page
    assert "| `samples.speaker_wid` | 2 | fail | share 0.75 against 0.95 |" in page
    assert "Evaluated 2026-01-01 at `aaaaaaa`." in page
    for criterion in CATALOGUE:
        assert f"| `{criterion.id}` | {criterion.level} | {criterion.kind} |" in page


def test_the_committed_overview_matches_the_committed_scorecards():
    assert overview.is_current(ROOT), "run `python -m optv.scripts.readiness check`"
    assert [card["parliament"] for card in overview.load_scorecards(ROOT)] == list_parliaments()


def test_a_parliaments_own_test_modules_exclude_those_of_its_regions():
    codes = list_parliaments()
    assert "test_de_golden.py" in own_test_modules(ROOT, "DE", codes)
    assert not [name for name in own_test_modules(ROOT, "DE", codes) if name.startswith("test_de_bw")]
    assert own_test_modules(ROOT, "DE-BW", codes) == ["test_de_bw_merger.py"]
    assert own_test_modules(ROOT, "FR", codes) == ["test_fr.py"]
