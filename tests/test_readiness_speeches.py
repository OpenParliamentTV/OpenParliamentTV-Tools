"""What the readiness criteria read off a single speech: the platform's import decisions and the audio duration."""

import pytest

from optv.scripts.readiness import speeches
from optv.scripts.readiness.speeches import (gate_state, missing_import_fields, speech_duration,
                                              would_import_text)
from tests.readiness_fixtures import make_speech

TEXT = [{"textBody": [{"sentences": [{"text": "Ein Satz."}]}]}]


# The platform (modules/import/media-writer.php) imports a speech's text unless the speech
# carries BOTH debug.confidence and debug.linkedMediaIndexes and fails the check on them.
@pytest.mark.parametrize("debug, state, imports", [
    ({}, "ungated", True),
    ({"linkedMediaIndexes": [1]}, "ungated", True),
    ({"confidence": None, "linkedMediaIndexes": [1]}, "ungated", True),
    ({"confidence": 1}, "confidence-only", True),
    ({"confidence": 0.5}, "confidence-only", True),
    ({"confidence": 1, "linkedMediaIndexes": [1]}, "gated", True),
    ({"confidence": 1.0, "linkedMediaIndexes": [7]}, "gated", True),
    ({"confidence": 0.5, "linkedMediaIndexes": [1]}, "gated", False),
    ({"confidence": 1, "linkedMediaIndexes": [1, 2]}, "gated", False),
    ({"confidence": 1, "linkedMediaIndexes": []}, "gated", False),
    ({"confidence": 1, "linkedMediaIndexes": None}, "gated", False),
])
def test_text_import_follows_the_platform_gate(debug, state, imports):
    speech = {"textContents": TEXT, "debug": debug}
    assert gate_state(speech) == state
    assert would_import_text(speech) is imports


def test_a_speech_without_text_contents_imports_no_text():
    assert would_import_text({"textContents": [], "debug": {}}) is False
    assert would_import_text({"debug": {"confidence": 1, "linkedMediaIndexes": [1]}}) is False


def test_a_complete_speech_passes_the_importer():
    assert missing_import_fields(make_speech(1)) == []


@pytest.mark.parametrize("damage, field", [
    (lambda s: s.pop("parliament"), "parliament"),
    (lambda s: s["electoralPeriod"].update(number="neunzehn"), "electoralPeriod.number"),
    (lambda s: s["session"].pop("number"), "session.number"),
    (lambda s: s["agendaItem"].update(title="", officialTitle=""), "agendaItem.title"),
    (lambda s: s["media"].update(videoFileURI=""), "media.videoFileURI"),
    (lambda s: s["media"].pop("sourcePage"), "media.sourcePage"),
    (lambda s: s.update(dateStart=None), "dateStart"),
])
def test_the_importer_rejects_a_speech_missing_a_required_field(damage, field):
    speech = make_speech(1)
    damage(speech)
    assert missing_import_fields(speech) == [field]


def test_one_agenda_title_is_enough_for_the_importer():
    speech = make_speech(1)
    speech["agendaItem"]["title"] = ""
    assert missing_import_fields(speech) == []


def _bare(**media) -> dict:
    return {"media": media, "textContents": []}


def test_duration_comes_from_the_media_entry_first():
    speech = _bare(duration=219, videoFileURI="https://example.org/v.mp4#t=32,300")
    assert speech_duration(speech) == (219.0, "media.duration")


def test_duration_falls_back_to_the_media_fragment():
    speech = _bare(duration=0, videoFileURI="https://example.org/playlist.m3u8#t=10,98")
    assert speech_duration(speech) == (88.0, "fragment")


def test_duration_from_stream_offsets():
    speech = _bare(videoFileURI="https://example.org/index.m3u8?start=339&end=694",
                   additionalInformation={"clipStartInStreamSecs": 339, "stopInStreamSecs": 694})
    assert speech_duration(speech) == (355.0, "stream-offsets")


def test_duration_from_the_cached_clip(tmp_path, monkeypatch):
    clip = tmp_path / "19521.mp3"
    clip.write_bytes(b"x")
    monkeypatch.setattr("optv.shared.audio_prep._duration_seconds", lambda path: 42.5)
    speech = _bare(videoFileURI="https://example.org/master.m3u8")
    assert speech_duration(speech, clip=clip) == (42.5, "clip")
    assert speech_duration(speech, clip=tmp_path / "missing.mp3") == (0.0, "unknown")


def test_duration_from_sentence_times_then_text_length():
    timed = {"media": {}, "textContents": [{"textBody": [{"sentences": [
        {"text": "Eins.", "timeStart": "0.0", "timeEnd": "4.5"},
        {"text": "Zwei.", "timeStart": "4.5", "timeEnd": "77.25"}]}]}]}
    assert speech_duration(timed) == (77.25, "sentence-times")
    untimed = {"media": {}, "originalLanguage": "de",
               "textContents": [{"textBody": [{"sentences": [{"text": "x" * 150}]}]}]}
    assert speech_duration(untimed) == (10.0, "text-length")
    assert speech_duration({"media": {}}) == (0.0, "unknown")


@pytest.mark.parametrize("period, session, index, name", [
    (19, 19, 3, "190193.mp3"),
    (2025, 122, 14, "202512214.mp3"),
    (17, 1057, 2, "1710572.mp3"),
])
def test_clip_path_matches_the_align_cache(tmp_path, period, session, index, name):
    speech = {"electoralPeriod": {"number": period}, "session": {"number": session}, "speechIndex": index}
    assert speeches.clip_path(tmp_path, speech) == tmp_path / "cache" / "audio" / name
