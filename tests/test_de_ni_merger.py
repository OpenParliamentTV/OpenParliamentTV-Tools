"""DE-NI tests: scraper helpers, joint debates, parser flatten, VTT placement,
end-to-end Stage 2 shape.

No network: the scraper's API calls are monkeypatched, the parser runs against a
static items.json fixture (the scraper's output shape), the VTT parser reads a
cached subtitle file, and the merger consumes the intermediate media JSON. The
merged result is validated against the full Stage 2 schema. The DE-NI package
directory has a hyphen, so it is imported via ``importlib`` rather than a normal
import.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path

from optv.shared.agenda_types import classify_de_ni
from optv.shared.validators import validate_stage2

_scraper = importlib.import_module("optv.parliaments.DE-NI.scraper.common")
_fetch_media = importlib.import_module("optv.parliaments.DE-NI.scraper.fetch_media")
_parser = importlib.import_module("optv.parliaments.DE-NI.parsers.media2json")
_vtt = importlib.import_module("optv.parliaments.DE-NI.parsers.vtt2json")
_merger = importlib.import_module("optv.parliaments.DE-NI.merger.merge_session")
Config = importlib.import_module("optv.parliaments.DE-NI.common").Config

FIXTURES = Path(__file__).parent / "fixtures" / "DE-NI"


# --------------------------------------------------------------------------- #
# Agenda classification (title + subjectArt + consultationType)
# --------------------------------------------------------------------------- #

def test_classify_de_ni():
    assert classify_de_ni("Haushaltsgesetz 2026", "Gesetzentwurf", "Erste Beratung") == \
        ("DE-NI-budget", "budget")
    assert classify_de_ni("Aktuelle Stunde zur Lage", None, None) == \
        ("DE-NI-current_affairs", "current_affairs")
    assert classify_de_ni("Irgendein Thema", "Gesetzentwurf", "Erste Beratung") == \
        ("DE-NI-gesetzentwurf", "regular")
    assert classify_de_ni("Schulpolitik", "Antrag", "Abschließende Beratung") == \
        ("DE-NI-antrag", "regular")
    assert classify_de_ni("Mitteilungen der Präsidentin", "", "") == \
        ("DE-NI-announcements", "procedural")
    assert classify_de_ni("Mündliche Anfragen", "", "") == ("DE-NI-question_time", "qa")
    # nothing recognisable falls through to regular
    assert classify_de_ni("", None, None) == (None, "regular")


# --------------------------------------------------------------------------- #
# Scraper helpers: session key + clip URL construction
# --------------------------------------------------------------------------- #

def test_session_key_and_clip_uri():
    assert _scraper.session_key(19, 80) == "19080"
    assert _scraper.session_key(18, 9) == "18009"
    assert _scraper.video_clip_uri("session30_meeting80_85516", 382.0, 505.0) == \
        "https://vod.plenartv.de/stream/session30_meeting80_85516/index.m3u8?start=382&end=505"
    assert _scraper.video_clip_uri("", 1, 2) == ""
    assert _scraper.player_page_url("session37_meeting99_14259", 3517.0, 3901.0) == \
        "https://plenartv.de/player?file=session37_meeting99_14259&startPos=3517&endPos=3901"
    assert _scraper.player_page_url("", 1, 2) == ""


def test_clip_start_secs():
    # The VOD server starts a clip with segment ceil(start/d) - 1 (0-based).
    assert _scraper.clip_start_secs(3517.0, 10.0) == 3510.0
    assert _scraper.clip_start_secs(3510.0, 10.0) == 3500.0   # on a boundary
    assert _scraper.clip_start_secs(782.0, 1.0) == 781.0
    assert _scraper.clip_start_secs(0.0, 10.0) == 0.0
    assert _scraper.clip_start_secs(5.0, None) == 5.0          # unknown segment length
    assert _scraper.clip_start_secs(None, 10.0) is None


# --------------------------------------------------------------------------- #
# Scraper: agenda order, joint debates, VTT source, stream-name repair
# --------------------------------------------------------------------------- #

def _detail(sid, item, timings, *, subject=1, together=0, vtt_file=None, vtt_offset=0.0,
            stream="session30_meeting79_132650", start=691.0, stop=2689.0):
    return {
        "id": sid, "subjectNumber": subject, "title": f"Titel {item}",
        "item": {"itemNumber": item, "itemBeginning": "09:10:00", "togetherStatus": together},
        "itemStartTimeInStreamSecs": start, "itemStopTimeInStreamSecs": stop,
        "streamFileName": stream,
        "video": {"streamFileName": stream, "startTime": "2025-12-15T07:55:00", "offset": 0},
        "vttFile": vtt_file, "vttOffset": vtt_offset,
        "speakerTimings": timings,
    }


def _timing(tid, start, stop, function=None):
    return {"id": tid, "abg_id": 1, "surname": "X", "name": "Y", "fraktion": "SPD",
            "function": function, "speechType": "RZ",
            "startTimeInStreamSecs": start, "stopTimeInStreamSecs": stop}


def test_fetch_session_joint_debate(monkeypatch):
    shared = [_timing("t1", 700.0, 900.0), _timing("t2", 910.0, 1200.0)]
    details = {
        # listed out of agenda order: the last item of the joint debate first
        "s3": _detail("s3", 3.0, shared, together=3, vtt_file="TOP3-1.vtt", vtt_offset=4.0),
        "s2": _detail("s2", 2.0, shared, together=1),
        "s4": _detail("s4", 4.0, [_timing("t3", 3000.0, 3100.0, "Ministerin für Inneres")],
                      stream="session30_meeting79_132650_x", start=2690.0, stop=5690.0),
        "s5": _detail("s5", 5.0, [], stream="session30_meeting79_132650_x_std",
                      start=5691.0, stop=7158.0),
        "s6": _detail("s6", 6.0, [], stream="session30_meeting79_132650_x_std",
                      start=7159.0, stop=8018.0),
    }
    monkeypatch.setattr(_fetch_media, "get_subjects_by_date",
                        lambda date, retry_count=10: [{"id": k} for k in details])
    monkeypatch.setattr(_fetch_media, "get_subject",
                        lambda sid, retry_count=10: details[sid])
    monkeypatch.setattr(_fetch_media, "get_segment_secs",
                        lambda name, retry_count=10: 1.0)
    entry = {"session_id": "19079", "period": 19, "sitzung": 79,
             "tagungsabschnitt": 30, "date": "2025-12-15", "meeting_id": "m"}
    out = _fetch_media.fetch_session(entry)
    subj = {s["subject_id"]: s for s in out["subjects"]}

    # agenda order, speeches kept under the first item of the joint debate
    assert [s["subject_id"] for s in out["subjects"]] == ["s2", "s3", "s4", "s5", "s6"]
    assert [sp["timing_id"] for sp in subj["s2"]["speeches"]] == ["t1", "t2"]
    assert subj["s3"]["speeches"] == []
    assert subj["s3"]["joint_with"] == "s2"
    assert subj["s2"]["joint_subjects"] == [
        {"subject_id": "s3", "item_number": 3.0, "subject_number": 1, "title": "Titel 3.0"}]
    # TOP 2 has no subtitle file: the joint debate's file (TOP 3) is used
    assert subj["s2"]["vtt_subject_id"] == "s3"
    assert subj["s2"]["vtt_source_offset"] == 4.0
    assert subj["s2"]["vtt_item_start_secs"] == 691.0
    assert subj["s2"]["item_start_secs"] == 691.0
    assert subj["s2"]["segment_secs"] == 1.0
    # a stream name no sibling uses, with a "_std" sibling, is repaired
    assert subj["s4"]["stream_file_name"] == "session30_meeting79_132650_x_std"
    assert subj["s4"]["speeches"][0]["function"] == "Ministerin für Inneres"
    assert out["_counts"]["speeches"] == 3


# --------------------------------------------------------------------------- #
# Parser: flatten subjects × speakerTimings; context + wall-clock + ordering
# --------------------------------------------------------------------------- #

def test_parse_session():
    doc = _parser.parse_session(FIXTURES / "items-sample.json")
    data = doc["data"]
    assert len(data) == 5
    # global order is wall-clock: the afternoon stream counts seconds from its
    # own start, so its speeches come last despite their low stream seconds
    assert [s["timing_id"] for s in data] == \
        ["t-pres-1", "t-min-3", "t-mp-2", "t-vp-4", "t-mp-5"]
    assert [s["speech_index"] for s in data] == [1, 2, 3, 4, 5]
    assert [s["start_secs"] for s in data] == [382.0, 700.0, 1000.0, 335.0, 437.0]

    pres = data[0]
    assert pres["label"] == "Barbara Otte-Kinast"
    assert pres["context"] == "president"
    assert pres["role"] == "Landtagspräsidentin"
    assert pres["faction"] == "CDU"
    # wall-clock = stream startTime (UTC) + offset
    assert pres["start_datetime"] == "2025-12-16T08:01:38Z"
    assert pres["end_datetime"] == "2025-12-16T08:03:41Z"
    assert pres["stream_start_estimated"] is False
    # 1 s segments: the clip starts one second early
    assert pres["clip_start_secs"] == 381.0

    minister = data[1]
    assert minister["label"] == "Gerald Heere"
    assert minister["context"] == "main-speaker"   # speechType "Antwort"
    assert minister["role"] == "Finanzminister"
    assert minister["faction"] == ""               # government speaker, no party
    # the joint debate's subtitles and the other item travel with the speech
    assert minister["vtt_subject_id"] == "cccc3333-0000-0000-0000-000000000003"
    assert minister["vtt_offset"] == 3.0
    assert minister["joint_subjects"][0]["item_number"] == 20.0

    vice = data[3]
    assert vice["context"] == "vice-president"     # function wins over speechType
    assert vice["role"] == "Vizepräsidentin"
    assert vice["start_datetime"] == "2025-12-16T11:16:07Z"
    # 10 s segments: the clip starts at the segment boundary below
    assert vice["clip_start_secs"] == 330.0
    assert data[4]["clip_start_secs"] == 430.0

    mp = data[2]
    assert mp["context"] == "main-speaker"         # speechType "RZ"
    assert mp["role"] == ""
    assert mp["faction"] == "SPD"


def test_context_from_function():
    assert _parser._context("RB", "Vizepräsident") == "vice-president"
    assert _parser._context("Mitteilungen", "Landtagsvizepräsidentin") == "vice-president"
    assert _parser._context("KI", "Vizepräsident") == "vice-president"
    assert _parser._context("Mitteilungen", "Landtagspräsidentin") == "president"
    assert _parser._context("RB", "Alterspräsident") == "interim-president"
    # "Ministerpräsident" is not the chair
    assert _parser._context("RZ", "Ministerpräsident") == "main-speaker"
    assert _parser._context("Mitteilungen", "") == "president"
    assert _parser._context("KI", "") == "speaker"


def test_parse_session_estimates_wp18_stream_start(tmp_path):
    """WP 18 streams have no startTime: estimate it from itemBeginning."""
    manifest = json.loads((FIXTURES / "items-sample.json").read_text())
    manifest.update(session_id="18009", period=18, sitzung=9, date="2018-02-28")
    stream = "2018-02-28_1_std"
    subjects = []
    # itemBeginning is local time to the minute; stream start = 08:09:19 local
    for n, (begin, item_start, subject_no) in enumerate(
            [("09:01:00", 3101.0, 1), ("09:05:00", 3341.0, 1),
             ("09:05:00", 5258.0, 2), ("10:46:00", 9423.0, 1)]):
        subjects.append({
            "subject_id": f"s{n}", "subject_number": subject_no, "item_number": 15.0 + n,
            "item_beginning": begin, "item_start_secs": item_start, "item_stop_secs": item_start + 200,
            "title": f"TOP {n}", "stream_file_name": stream, "video_start_time": "",
            "segment_secs": 10.0, "vtt_subject_id": "", "vtt_source_offset": 0,
            "speeches": [{"timing_id": f"t{n}", "abg_id": 1, "surname": "Busemann",
                          "name": "Bernd", "fraktion": "CDU", "function": "Vizepräsident",
                          "speech_type": "RB", "start_secs": item_start + 10,
                          "stop_secs": item_start + 100}],
        })
    manifest["subjects"] = subjects
    path = tmp_path / "18009-items.json"
    path.write_text(json.dumps(manifest))
    data = _parser.parse_session(path)["data"]
    first = data[0]
    assert first["stream_start_estimated"] is True
    assert first["context"] == "vice-president"
    # median of 08:09:19 / 08:09:19 / 08:08:57 local (CET) → 07:09:19 UTC;
    # the subject-2 item (07:37:22) is ignored
    assert first["start_datetime"] == "2018-02-28T08:01:10Z"    # 07:09:19 + 3111 s
    assert [s["timing_id"] for s in data] == ["t0", "t1", "t2", "t3"]


# --------------------------------------------------------------------------- #
# VTT: anchor at the item start + vttOffset, partition, clip-relative timing
# --------------------------------------------------------------------------- #

_VTT = """WEBVTT

00:00:01.000 --> 00:00:04.000
Ich rufe auf: Tagesordnungspunkt 19.

00:00:49.000 --> 00:00:52.000
Herr Präsident! Meine Damen und Herren!

00:00:52.000 --> 00:00:56.000
Der Haushalt ist solide.

00:05:36.000 --> 00:05:38.000
Vielen Dank.

00:05:38.000 --> 00:05:40.000
Erika Mustermann hat das Wort.

00:05:58.000 --> 00:06:02.000
Danke, Herr Präsident! Dieser Haushalt ist gut.
"""


def test_vtt_anchor_uses_the_players_clip_start():
    # TOP 11 of 2026-10-07: item at 3465 on a 10 s grid, player clip from 3460
    assert _vtt.vtt_anchor(3465.0, 4.0, 10.0) == 3464.0
    # 1 s grid: the clip starts one second early
    assert _vtt.vtt_anchor(1919.0, 0.0, 1.0) == 1918.0
    # unknown segment length: the item start itself
    assert _vtt.vtt_anchor(500.0, 1.0) == 501.0


def test_assign_cues_partitions_overlaps_and_drops_gaps():
    recs = [{"start_secs": 100.0, "stop_secs": 200.0, "speech_index": 1},
            {"start_secs": 150.0, "stop_secs": 170.0, "speech_index": 2},
            {"start_secs": 210.0, "stop_secs": 300.0, "speech_index": 3}]
    cues = [(0, 4, "a"), (55, 57, "b"), (80, 84, "c"), (102, 104, "gap"), (160, 170, "d")]
    out = _vtt.assign_cues(recs, cues, 98.0)
    assert [t for (_, _, t) in out[1]] == ["a", "c"]       # after the intervention
    assert [t for (_, _, t) in out[2]] == ["b"]            # nested window wins
    assert [t for (_, _, t) in out[3]] == ["d"]
    assert all(t != "gap" for seg in out.values() for (_, _, t) in seg)


def _write_media_and_vtt(tmp_path):
    media_doc = _parser.parse_session(FIXTURES / "items-sample.json")
    config = Config(tmp_path)
    config.dir("media", create=True)
    config.file("19080", "media", create=True).write_text(json.dumps(media_doc))
    vtt_dir = config.dir("cache") / "vtt"
    vtt_dir.mkdir(parents=True, exist_ok=True)
    (vtt_dir / "cccc3333-0000-0000-0000-000000000003.vtt").write_text(_VTT)
    (vtt_dir / "aaaa1111-0000-0000-0000-000000000001.vtt").write_text("WEBVTT\n")
    return config


def test_vtt_build_places_cues_by_item_start(tmp_path):
    config = _write_media_and_vtt(tmp_path)
    doc = _vtt.build(config, "19080")
    # the player's item clip starts one 1-s segment early (649), + vttOffset
    anchor = 649.0 + 3.0
    assert doc["meta"]["vttAnchors"] == {"cccc3333-0000-0000-0000-000000000003": anchor}
    turns = {t["speechIndex"]: t for t in doc["data"]}
    # only the joint debate has subtitles; the chair turn has an empty VTT
    assert set(turns) == {2, 3}
    minister = [s["text"] for s in turns[2]["sentences"]]
    mp = [s["text"] for s in turns[3]["sentences"]]
    # the item's call-up lies before the first speech window and is dropped
    assert minister == ["Herr Präsident! Meine Damen und Herren!", "Der Haushalt ist solide.",
                        "Vielen Dank.", "Erika Mustermann hat das Wort."]
    assert mp == ["Danke, Herr Präsident! Dieser Haushalt ist gut."]
    # timings count from the clip start (start_secs - 1 on a 1 s segment grid)
    first = turns[2]["sentences"][0]
    assert float(first["timeStart"]) == round(anchor + 49.0 - 699.0, 3)


# --------------------------------------------------------------------------- #
# End-to-end merge → Stage 2 shape + schema validation
# --------------------------------------------------------------------------- #

def test_merge_session_end_to_end(tmp_path):
    config = _write_media_and_vtt(tmp_path)
    config.save_data(_vtt.build(config, "19080"), "19080", "proceedings")

    out = _merger.merge_session("19080", config, None)
    doc = json.loads(out.read_text())
    data = doc["data"]
    assert len(data) == 5
    assert "merge" in doc["meta"]["processing"]
    assert doc["meta"]["tagungsabschnitt"] == 30

    pres = data[0]
    assert pres["parliament"] == "DE-NI"
    assert pres["electoralPeriod"]["number"] == 19
    assert pres["session"]["number"] == 80
    assert pres["people"][0]["context"] == "president"
    assert pres["people"][0]["role"] == "Landtagspräsidentin"
    assert pres["people"][0]["faction"]["label"] == "CDU"
    assert pres["people"][0]["originPersonID"] == "202835"
    assert pres["agendaItem"]["type"] == "procedural"
    assert pres["textContents"] == []
    # per-speech server-side clip URL (no #t fragment) and the player page
    assert pres["media"]["videoFileURI"] == (
        "https://vod.plenartv.de/stream/session30_meeting80_85516/index.m3u8"
        "?start=382&end=505")
    assert pres["media"]["sourcePage"] == (
        "https://plenartv.de/player?file=session30_meeting80_85516&startPos=382&endPos=505")
    assert pres["media"]["additionalInformation"]["clipStartInStreamSecs"] == 381.0
    assert pres["debug"]["timesAreVideoRelative"] is False
    assert "jointAgendaItems" not in pres["debug"]

    minister = data[1]
    assert minister["agendaItem"]["type"] == "budget"
    assert minister["agendaItem"]["nativeType"] == "DE-NI-budget"
    assert minister["agendaItem"]["id"] == "TOP-19-1"
    assert minister["debug"]["jointAgendaItems"] == [
        {"id": "TOP-20-1", "title": "Entwurf eines Haushaltsbegleitgesetzes 2026"}]
    assert minister["debug"]["vttOffset"] == 3.0
    assert minister["media"]["additionalInformation"]["subtitleVttURI"] == \
        "https://api.plenartv.de/vtt/cccc3333-0000-0000-0000-000000000003"
    assert minister["textContents"][0]["textBody"]
    assert data[3]["people"][0]["context"] == "vice-president"

    # sourcePage must be unique per speech (platform keys speech identity on it)
    pages = [s["media"]["sourcePage"] for s in data]
    assert len(set(pages)) == len(pages)

    # downstream-required keys present on every speech
    required = {"speechIndex", "session", "electoralPeriod", "people",
                "textContents", "media", "agendaItem", "originID"}
    for sp in data:
        assert not (required - set(sp))

    # full Stage 2 schema: structural validation must be error-free
    findings = validate_stage2(doc, schema="full", semantic=True)
    errors = [f for f in findings if f["severity"] == "error"]
    assert errors == []
