"""TW parser unit tests (media + proceedings + transcript helpers).

The fixtures under ``tests/fixtures/TW/`` are trimmed copies of real
responses for plenary 院會-11-5-11 (term 11 / session 5 / meeting 11):
ly.govapi.tw IVOD list + details (two per-speech clips + one ``Full``
session video that the parser must filter out), the dataset-221 speech-kind
rows of that sitting, and a few lines of the official text of IVOD 169638
from ``ivod.ly.gov.tw/Demand/Speech/169638``.
"""

from __future__ import annotations

import json
from pathlib import Path

from optv.parliaments import get_rights
from optv.parliaments.TW.common import decode_session, encode_session
from optv.parliaments.TW.parsers.media2json import (
    assign_speech_kinds,
    parse_duration_hhmmss,
    parse_ivod_list,
)
from optv.parliaments.TW.parsers.proceedings2json import parse_details
from optv.parliaments.TW.parsers.speech_record import (
    parse_speech_record,
    split_sentences,
)
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
    # No official text yet: the only entry is the WhisperX machine transcript.
    assert [tc["type"] for tc in sp["textContents"]] == ["generated"]
    tc = sp["textContents"][0]
    rights = get_rights("TW", stream="transcript")
    assert (tc["creator"], tc["license"]) == (rights["creator"], rights["license"])
    assert tc["sourceURI"] == f"https://ly.govapi.tw/v2/ivods/{sp['originID']}"
    assert tc["language"] == "zh"
    tb = tc["textBody"][0]
    assert tb["type"] == "speech"
    assert tb["sentences"]
    # Whisperx timings landed on every sentence
    for sent in tb["sentences"]:
        assert "text" in sent
        assert "timeStart" in sent
        assert "timeEnd" in sent
    # agendaItem is the merger's job (from the media record).
    assert "agendaItem" not in sp
    assert sp["debug"]["officialText"] is False


def test_proceedings_parser_puts_official_text_first():
    raw = _load("sample-details.json")
    html = (FIX / "sample-speech-record.html").read_text()
    parsed = parse_details(raw, term=11, session_period=5, meeting_number=11,
                           records={"169638": html})
    by_id = {sp["originID"]: sp for sp in parsed["data"]}
    sp = by_id["169638"]
    assert [tc["type"] for tc in sp["textContents"]] == ["proceedings", "generated"]
    official = sp["textContents"][0]
    rights = get_rights("TW", stream="proceedings")
    assert (official["creator"], official["license"]) == (rights["creator"], rights["license"])
    assert official["license"] == "https://data.ly.gov.tw/copyprotection.action"
    assert official["sourceURI"] == "https://ivod.ly.gov.tw/Demand/Speech/169638"
    assert official["originTextID"] == "169638"
    # Official text is untimed.
    assert all("timeStart" not in s
               for item in official["textBody"] for s in item["sentences"])
    assert sp["debug"]["officialText"] is True
    # The other clip has no official text yet.
    assert [tc["type"] for tc in by_id["169637"]["textContents"]] == ["generated"]


# ---- official per-clip text ----

def test_speech_record_splits_turns_and_labels_speakers():
    html = (FIX / "sample-speech-record.html").read_text()
    items = parse_speech_record(html, "陳昭姿")
    speakers = [(i["speaker"], i["speakerstatus"]) for i in items]
    assert speakers[:3] == [
        ("陳昭姿", "main-speaker"),   # 陳委員昭姿
        ("主席", "president"),
        ("卓榮泰", "speaker"),        # 卓院長榮泰
    ]
    # The clock note opening the first turn is not part of the text.
    assert items[0]["sentences"][0]["text"] == "謝謝主席，有請卓院長。"
    # Hard-wrapped lines are joined into the turn they continue, including a
    # line that starts with 「說：」 (a colon, but not a speaker label).
    long_turn = items[7]
    assert long_turn["speaker"] == "陳昭姿"
    texts = [s["text"] for s in long_turn["sentences"]]
    assert "賴總統兩次都回答我說：「我當然支持啊！」" in texts
    assert all(i["speaker"] != "說" for i in items)
    # The written-interpellation stub at the end is not spoken text.
    assert items[-1]["speaker"] == "主席"
    assert not any("書面質詢：" in s["text"] for i in items for s in i["sentences"])


def test_speech_record_label_variants():
    html = ("張廖次長萬堅：好。<br />杜副主任委員張梅莊：是。<br />"
            "郭副院長工賓：對。<br />")
    items = parse_speech_record(html, "")
    assert [i["speaker"] for i in items] == ["張廖萬堅", "杜張梅莊", "郭工賓"]


def test_speech_record_name_with_latin_part_and_hour_only_clock():
    # Real label shapes from 院會-11-5-11 (IVOD 169636 and 169566).
    html = ("伍麗華 Saidhai Tahovecahe 委員：（10 時 49 分）謝謝主席，有請卓院長。<br />"
            "林委員倩綺：（9 時）總統大談經濟。<br />")
    items = parse_speech_record(html, "伍麗華Saidhai Tahovecahe")
    assert [(i["speaker"], i["speakerstatus"]) for i in items] == [
        ("伍麗華 Saidhai Tahovecahe", "main-speaker"),
        ("林倩綺", "speaker"),
    ]
    assert [i["sentences"][0]["text"] for i in items] == ["謝謝主席，有請卓院長。", "總統大談經濟。"]


def test_speech_record_empty_page():
    assert parse_speech_record("", "陳昭姿") == []


def test_split_sentences_keeps_closing_quotes():
    assert split_sentences("他說：「好！」我們走吧。還有") == ["他說：「好！」", "我們走吧。", "還有"]


# ---- dataset-221 speech kinds ----

def test_media_parser_assigns_speech_kinds_from_dataset_221():
    raw = _load("sample-ivods.json")
    rows = _load("sample-speechkinds.json")["rows"]
    parsed = parse_ivod_list(raw, rows)
    assert {r["speakerLabel"]: r["speechKind"] for r in parsed["data"]} == {
        "陳瑩": "個人質詢", "陳昭姿": "個人質詢",
    }


def test_speech_kinds_pair_repeated_speakers_in_order():
    # Real 2026-10-30 data: 洪孟楷 spoke in the 國是論壇 at 09:03 and on a bill
    # at 12:49; dataset 221 lists the rows in that order.
    records = [
        {"dateStart": "2026-10-30T09:00:19+08:00", "speakerLabel": "林倩綺"},
        {"dateStart": "2026-10-30T09:03:33+08:00", "speakerLabel": "洪孟楷"},
        {"dateStart": "2026-10-30T12:49:55+08:00", "speakerLabel": "洪孟楷"},
        {"dateStart": "2026-10-30T13:00:00+08:00", "speakerLabel": "主席"},
    ]
    rows = [
        {"smeetingDate": "115/10/30", "speechKindName": "國是論壇", "legislatorNameList": "林倩綺"},
        {"smeetingDate": "115/10/30", "speechKindName": "國是論壇", "legislatorNameList": "洪孟楷"},
        {"smeetingDate": "115/10/30", "speechKindName": "法案發言", "legislatorNameList": "洪孟楷"},
    ]
    unmatched = assign_speech_kinds(records, rows)
    assert [r.get("speechKind") for r in records] == ["國是論壇", "國是論壇", "法案發言", None]
    assert unmatched == 1


def test_speech_kinds_joint_interpellation_matches_each_name():
    records = [{"dateStart": "2025-10-07T10:00:00+08:00", "speakerLabel": "翁曉玲"}]
    rows = [{"smeetingDate": "114/10/07", "speechKindName": "政黨質詢",
             "legislatorNameList": "傅崐萁、翁曉玲、林倩綺"}]
    assert assign_speech_kinds(records, rows) == 0
    assert records[0]["speechKind"] == "政黨質詢"
