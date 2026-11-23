"""The Whisper QC command line, with the transcription itself replaced: resume and retry, language and time limit per speech, and the figures it writes."""

import json

import pytest

from optv.scripts import merge_audit
from optv.shared import whisper_diff, whisper_qc
from optv.shared.publish import text_signature
from tests.readiness_fixtures import (clean_session, make_session, make_speech, speech_text, stage_clips,
                                      write_session, write_transcripts)


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path / "data"


@pytest.fixture
def transcriber(monkeypatch):
    """Stands in for faster-whisper. ``fail`` holds the clip names that fail; ``calls`` records what was asked."""
    class Transcriber:
        fail: set = set()
        calls: list = []

        def __call__(self, audio_path, language="de", model_size="large-v3", timeout=0):
            self.calls.append({"clip": audio_path.name, "language": language, "timeout": timeout})
            if audio_path.name in self.fail:
                return None
            return {"ok": True, "text": f"Text von {audio_path.name}", "segments": [],
                    "language": language, "language_probability": 1.0, "duration": 60.0,
                    "model": model_size}

    stub = Transcriber()
    stub.fail, stub.calls = set(), []
    monkeypatch.setattr(whisper_qc, "transcribe_speech", stub)
    monkeypatch.setattr(whisper_diff, "speech_timeout", lambda audio: 777)
    # The staged clips are not audio; their length is whatever a test says, by default unknown.
    stub.clip_seconds = {}
    monkeypatch.setattr(whisper_diff, "clip_seconds", lambda audio: stub.clip_seconds.get(audio.name, 0.0))
    return stub


def _session(data_dir, session="19115", number=115, **speech):
    doc = clean_session(session, number, **speech)
    write_session(data_dir, session, doc)
    stage_clips(data_dir, doc)
    return doc


def _transcribe(data_dir, session="19115", *extra):
    return whisper_diff.main(["transcribe", "--data-dir", str(data_dir), "--session", session, *extra])


def _entries(data_dir, session="19115"):
    doc = json.loads(whisper_diff.whisper_path(data_dir, session).read_text())
    return {entry["speechIndex"]: entry for entry in doc["speeches"]}


def test_a_failed_speech_is_recorded_even_when_it_is_the_last(data_dir, transcriber):
    _session(data_dir)
    transcriber.fail = {"191154.mp3"}
    assert _transcribe(data_dir) == 0
    entries = _entries(data_dir)
    assert [entries[i]["ok"] for i in (1, 2, 3, 4)] == [True, True, True, False]
    assert entries[4] == {"speechIndex": 4, "ok": False, "audio_file": "191154.mp3", "attempts": 1}


def test_a_failed_speech_is_retried_once_and_then_left_alone(data_dir, transcriber):
    _session(data_dir)
    transcriber.fail = {"191152.mp3"}
    _transcribe(data_dir)
    assert len(transcriber.calls) == 4

    _transcribe(data_dir)
    assert [call["clip"] for call in transcriber.calls[4:]] == ["191152.mp3"]
    assert _entries(data_dir)[2]["attempts"] == 2

    _transcribe(data_dir)
    assert len(transcriber.calls) == 5

    transcriber.fail = set()
    _transcribe(data_dir, "19115", "--speech", "2", "--force")
    assert _entries(data_dir)[2]["ok"] is True


def test_a_retry_that_succeeds_replaces_the_failure(data_dir, transcriber):
    _session(data_dir)
    transcriber.fail = {"191153.mp3"}
    _transcribe(data_dir)
    transcriber.fail = set()
    _transcribe(data_dir)
    entry = _entries(data_dir)[3]
    assert entry["ok"] is True and "attempts" not in entry
    assert len(transcriber.calls) == 5


def test_a_failure_recorded_without_a_count_is_one_attempt(data_dir, transcriber):
    doc = _session(data_dir)
    path = write_transcripts(data_dir, "19115", doc, skip=(4,))
    cache = json.loads(path.read_text())
    cache["speeches"].append({"speechIndex": 4, "ok": False, "audio_file": "191154.mp3"})
    path.write_text(json.dumps(cache))
    transcriber.fail = {"191154.mp3"}
    _transcribe(data_dir)
    assert len(transcriber.calls) == 1
    assert _entries(data_dir)[4]["attempts"] == 2


def test_speeches_without_a_clip_are_skipped(data_dir, transcriber):
    doc = clean_session("19115", 115)
    write_session(data_dir, "19115", doc)
    stage_clips(data_dir, make_session("19115", doc["data"][:2]))
    _transcribe(data_dir)
    assert sorted(_entries(data_dir)) == [1, 2]


def test_each_speech_is_transcribed_in_its_own_language(data_dir, transcriber):
    doc = clean_session("19115", 115)
    for speech, language in zip(doc["data"], ["de", "nb", "ca", None]):
        speech["originalLanguage"] = language
    write_session(data_dir, "19115", doc)
    stage_clips(data_dir, doc)
    _transcribe(data_dir)
    assert [call["language"] for call in transcriber.calls] == ["de", "no", "ca", None]
    assert {call["timeout"] for call in transcriber.calls} == {777}

    transcriber.calls.clear()
    _transcribe(data_dir, "19115", "--force", "--language", "sv", "--timeout", "30")
    assert {(call["language"], call["timeout"]) for call in transcriber.calls} == {("sv", 30)}


@pytest.mark.parametrize("seconds, limit", [
    (0.0, whisper_qc.DEFAULT_TIMEOUT),
    (20.0, 600),
    (1000.0, 3000),
    (7200.0, whisper_qc.DEFAULT_TIMEOUT),
])
def test_the_time_limit_follows_the_clip_length(tmp_path, monkeypatch, seconds, limit):
    monkeypatch.setattr("optv.shared.audio_prep._duration_seconds", lambda path: seconds)
    assert whisper_diff.speech_timeout(tmp_path / "clip.mp3") == limit


def test_text_in_several_languages_is_compared_in_the_one_spoken():
    speech = make_speech(1)
    original = speech["textContents"][0]
    rendering = json.loads(json.dumps(original))
    rendering["language"] = "es"
    rendering["textBody"][0]["sentences"] = [{"text": "Una traducción mucho más larga que no se pronunció."}]
    original["language"] = "ca"
    speech["originalLanguage"] = "ca"
    speech["textContents"] = [original, rendering]
    assert whisper_diff.spoken_text_contents(speech) == [original]
    assert "traducción" not in whisper_diff.speech_text(speech)
    assert whisper_diff.speech_chars(speech) == len(speech_text({"textContents": [original]})) - 5

    speech["originalLanguage"] = "eu"
    assert whisper_diff.spoken_text_contents(speech) == [original, rendering]


def test_diff_writes_the_figures_next_to_the_report(data_dir, capsys):
    doc = _session(data_dir)
    write_transcripts(data_dir, "19115", doc, skip=(4,), generated_at="2026-03-03T03:03:03")
    assert whisper_diff.main(["diff", "--data-dir", str(data_dir), "--session", "19115", "--quiet"]) == 0
    assert capsys.readouterr().out == ""
    figures = json.loads(whisper_diff.qc_path(data_dir, "19115").read_text())
    assert figures["text_signature"] == text_signature(doc["data"])
    assert figures["whisper_generated_at"] == "2026-03-03T03:03:03"
    assert (figures["transcribed"], figures["failed"]) == (3, 0)
    assert [row["text_similarity"] for row in figures["rows"]] == [1.0, 1.0, 1.0, None]
    assert whisper_diff.report_path(data_dir, "19115").read_text().startswith("# QC report")


def test_diff_without_a_transcript_writes_nothing(data_dir):
    _session(data_dir)
    assert whisper_diff.write_session_qc(data_dir, "19115") is None
    assert whisper_diff.main(["diff", "--data-dir", str(data_dir), "--session", "19115"]) == 2
    assert not whisper_diff.qc_path(data_dir, "19115").exists()


@pytest.mark.parametrize("session", ["2025-122", "17-1-057", "22_11517", "19115"])
def test_rank_and_audit_keep_the_whole_session_key(data_dir, capsys, session):
    doc = _session(data_dir, session=session)
    (data_dir / "cache" / "merged").mkdir(parents=True)
    (data_dir / "cache" / "merged" / f"{session}-merged.json").write_text(json.dumps(doc))
    assert whisper_diff.main(["rank", "--data-dir", str(data_dir)]) == 0
    assert capsys.readouterr().out.splitlines()[1].split()[0] == session
    assert {found for found, _, _ in merge_audit.walk(data_dir, "both", "*")} == {session}


def _whisper_doc(doc, durations):
    return {"speeches": [{"speechIndex": speech["speechIndex"], "ok": True, "duration": duration}
                         for speech, duration in zip(doc["data"], durations)]}


def test_transcripts_of_a_session_merged_anew_are_outdated():
    """A speech was inserted at the front since the transcripts were made: every transcript now sits one index early."""
    before = make_session("19115", [make_speech(i, session_number=115, duration=d)
                                    for i, d in enumerate([100.0, 250.0, 40.0, 600.0, 75.0], start=1)])
    after = make_session("19115", [make_speech(i, session_number=115, duration=d)
                                   for i, d in enumerate([30.0, 100.0, 250.0, 40.0, 600.0, 75.0], start=1)])
    recorded = _whisper_doc(before, [100.0, 250.0, 40.0, 600.0, 75.0])
    assert whisper_diff.current_transcripts(before["data"], recorded) == (
        {entry["speechIndex"]: entry for entry in recorded["speeches"]}, 0)
    assert whisper_diff.current_transcripts(after["data"], recorded) == ({}, 5)


def test_a_stray_difference_in_length_keeps_the_transcripts():
    doc = clean_session("19115", 115, count=12, duration=120.0)
    recorded = _whisper_doc(doc, [120.0] * 10 + [126.0, 300.0])
    current, outdated = whisper_diff.current_transcripts(doc["data"], recorded)
    assert (len(current), outdated) == (12, 0)
    # Three of twelve is no longer stray.
    recorded = _whisper_doc(doc, [120.0] * 9 + [300.0, 300.0, 300.0])
    assert whisper_diff.current_transcripts(doc["data"], recorded) == ({}, 12)


def test_a_transcript_without_a_speech_is_outdated_and_failures_do_not_count():
    doc = clean_session("19115", 115, count=3)
    recorded = _whisper_doc(doc, [60.0, 60.0, 60.0])
    recorded["speeches"] += [{"speechIndex": 9, "ok": True, "duration": 60.0},
                             {"speechIndex": 2, "ok": False, "attempts": 2}]
    current, outdated = whisper_diff.current_transcripts(doc["data"], recorded)
    assert (sorted(current), outdated) == ([1, 2, 3], 1)


def test_speeches_without_a_known_length_cannot_be_checked():
    doc = clean_session("19115", 115, timed=False)
    for speech in doc["data"]:
        del speech["media"]["duration"]
    recorded = _whisper_doc(doc, [999.0] * 4)
    assert len(whisper_diff.current_transcripts(doc["data"], recorded)[0]) == 4


def test_the_length_comes_from_what_the_speech_states():
    assert whisper_diff.declared_duration({"media": {"duration": 219}}) == (219.0, "media.duration")
    assert whisper_diff.declared_duration(
        {"media": {"duration": 0, "videoFileURI": "https://example.org/a.m3u8#t=10,98"}}) == (88.0, "fragment")
    assert whisper_diff.declared_duration({"media": {"additionalInformation": {
        "clipStartInStreamSecs": 339, "stopInStreamSecs": 694}}}) == (355.0, "stream-offsets")
    assert whisper_diff.declared_duration({"media": {"videoFileURI": "https://example.org/a.mp4"}}) == (0.0, "")
    aligned = clean_session("19115", 115, count=1, duration=90.0)["data"][0]
    assert whisper_diff.last_sentence_end(aligned) == 90.0


def test_outdated_transcripts_are_made_again(data_dir, transcriber):
    doc = _session(data_dir, duration=60.0)
    write_transcripts(data_dir, "19115", clean_session("19115", 115, duration=400.0))
    _transcribe(data_dir)
    assert len(transcriber.calls) == 4
    assert {entry["text"] for entry in _entries(data_dir).values()} == {
        f"Text von 19115{i}.mp3" for i in (1, 2, 3, 4)}

    transcriber.calls.clear()
    _transcribe(data_dir)
    assert transcriber.calls == []


def test_outdated_transcripts_are_left_out_of_the_figures(data_dir):
    doc = _session(data_dir, duration=60.0)
    write_transcripts(data_dir, "19115", clean_session("19115", 115, duration=400.0))
    rows = whisper_diff.write_session_qc(data_dir, "19115")
    assert [row["text_similarity"] for row in rows] == [None] * 4
    figures = json.loads(whisper_diff.qc_path(data_dir, "19115").read_text())
    assert (figures["transcribed"], figures["outdated"], figures["failed"]) == (0, 4, 0)


def test_clips_cut_before_a_re_merge_are_not_transcribed(data_dir, transcriber):
    _session(data_dir, duration=60.0)
    transcriber.clip_seconds = {f"19115{i}.mp3": seconds for i, seconds in ((1, 60.0), (2, 300.0),
                                                                             (3, 12.0), (4, 500.0))}
    assert _transcribe(data_dir) == whisper_diff.EXIT_OUTDATED_CLIPS
    assert transcriber.calls == []
    assert not whisper_diff.whisper_path(data_dir, "19115").exists()

    assert _transcribe(data_dir, "19115", "--force") == 0
    assert len(transcriber.calls) == 4


def test_clips_of_the_right_length_are_transcribed(data_dir, transcriber):
    doc = _session(data_dir, duration=60.0)
    transcriber.clip_seconds = {f"19115{i}.mp3": 61.5 for i in (1, 2, 3, 4)}
    assert whisper_diff.outdated_clips(data_dir, doc["data"]) == (0, 4)
    assert _transcribe(data_dir) == 0
    assert len(transcriber.calls) == 4
