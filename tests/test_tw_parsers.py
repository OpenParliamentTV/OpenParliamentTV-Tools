"""TW parser unit tests (media + proceedings + transcript helpers).

The fixtures under ``tests/fixtures/TW/`` are trimmed copies of real
ly.govapi.tw responses for plenary 院會-11-5-11 (term 11 / session 5 /
meeting 11). Two per-speech clips + one ``Full`` session video that the
parser must filter out.
"""

from __future__ import annotations

import json
from pathlib import Path

from optv.parliaments.TW.common import decode_session, encode_session
from optv.parliaments.TW.parsers.media2json import (
    parse_duration_hhmmss,
    parse_ivod_list,
)
from optv.parliaments.TW.parsers.proceedings2json import parse_details
from optv.parliaments.TW.parsers.transcript import (
    whisperx_max_time,
    whisperx_to_sentences,
)

FIX = Path(__file__).resolve().parent / "fixtures" / "TW"


def _load(name):
    return json.loads((FIX / name).read_text())


# ---- session-key + duration helpers ----

def test_session_key_round_trip():
    assert encode_session(11, 5, 11) == "1105011"
    assert decode_session("1105011") == (11, 5, 11)


def test_parse_duration_handles_hhmmss_and_int():
    assert parse_duration_hhmmss("00:30:44") == 1844
    assert parse_duration_hhmmss("01:02:03") == 3723
    assert parse_duration_hhmmss(120) == 120
    assert parse_duration_hhmmss(None) is None
    assert parse_duration_hhmmss("") is None
    assert parse_duration_hhmmss("not-a-duration") is None


# ---- whisperx → sentences ----

def test_whisperx_to_sentences_formats_timings_as_numeric_strings():
    segments = [{"start": 1.5, "end": 2.5, "text": "你好"}]
    sentences = whisperx_to_sentences(segments)
    assert sentences == [{"text": "你好", "timeStart": "1.500", "timeEnd": "2.500"}]
    # Spec invariant from stage2-full.schema.json#/definitions/numericString
    # is "^\\d+(\\.\\d+)?$" — the formatter must always produce that shape.
    import re
    s = whisperx_to_sentences([{"start": 0, "end": 0.001, "text": "x"}])[0]
    assert re.fullmatch(r"\d+(\.\d+)?", s["timeStart"])
    assert re.fullmatch(r"\d+(\.\d+)?", s["timeEnd"])


def test_whisperx_to_sentences_drops_empty_text():
    segments = [
        {"start": 0.0, "end": 1.0, "text": "  "},
        {"start": 1.0, "end": 2.0, "text": "有字"},
    ]
    sentences = whisperx_to_sentences(segments)
    assert [s["text"] for s in sentences] == ["有字"]


def test_whisperx_max_time_returns_largest_end():
    segs = [
        {"start": 0.0, "end": 1.0, "text": "a"},
        {"start": 1.0, "end": 5.5, "text": "b"},
        {"start": 5.5, "end": 3.0, "text": "c"},
    ]
    assert whisperx_max_time(segs) == 5.5


# ---- media parser ----

def test_media_parser_filters_full_videos_and_extracts_video_url():
    raw = _load("sample-ivods.json")
    parsed = parse_ivod_list(raw)
    # The fixture has 2 Clip + 1 Full; only clips survive.
    assert len(parsed["data"]) == 2
    record = parsed["data"][0]
    assert record["parliament"] == "TW"
    assert record["electoralPeriod"]["number"] == 11
    # session_period=5, meeting_number=11 → encoded as 5011
    assert record["session"]["number"] == 5011
    assert record["media"]["videoFileURI"].startswith("https://ivod")
    assert record["media"]["videoFileURI"].endswith(".m3u8")
    assert record["media"]["sourcePage"].startswith("https://ivod.ly.gov.tw/Play/Clip/")
    assert isinstance(record["media"]["duration"], int)
    assert record["media"]["duration"] > 0


def test_media_parser_meta_carries_session_key():
    raw = _load("sample-ivods.json")
    parsed = parse_ivod_list(raw)
    assert parsed["meta"]["session"] == "1105011"
    assert parsed["meta"]["meetingCode"] == "院會-11-5-11"


# ---- proceedings parser ----

def test_proceedings_parser_emits_one_speech_per_detail_with_whisperx_text():
    raw = _load("sample-details.json")
    parsed = parse_details(raw, term=11, session_period=5, meeting_number=11)
    assert len(parsed["data"]) == 2
    sp = parsed["data"][0]
    assert sp["parliament"] == "TW"
    assert sp["electoralPeriod"]["number"] == 11
    assert sp["session"]["number"] == 5011
    assert sp["originID"]  # stringified IVOD_ID
    # Speaker carried from 委員名稱
    assert sp["people"][0]["label"]
    assert sp["people"][0]["context"] == "main-speaker"
    # whisperx → sentences inside one textBody block
    assert sp["textContents"]
    tb = sp["textContents"][0]["textBody"][0]
    assert tb["type"] == "speech"
    assert tb["sentences"]
    # Whisperx timings landed on every sentence
    for sent in tb["sentences"]:
        assert "text" in sent
        assert "timeStart" in sent
        assert "timeEnd" in sent
    # agendaItem carries the plenary title + nativeType marker
    assert sp["agendaItem"]["officialTitle"]
    assert sp["agendaItem"]["nativeType"] == "TW-plenary"
    assert sp["agendaItem"]["type"] == "regular"
