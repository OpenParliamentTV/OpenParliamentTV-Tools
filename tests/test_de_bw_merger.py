"""DE-BW tests: chapter-list parsing, name/meta split, end-to-end Stage 2 shape.

No network: the scraper parser runs against a static HTML fixture, and the
merger consumes intermediate media JSON directly. The merged result is
validated against the full Stage 2 schema. The DE-BW package directory has a
hyphen, so it is imported via ``importlib`` rather than a normal import.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path

from optv.shared.validators import validate_stage2

_scraper = importlib.import_module("optv.parliaments.DE-BW.scraper.common")
_parser = importlib.import_module("optv.parliaments.DE-BW.parsers.media2json")
_merger = importlib.import_module("optv.parliaments.DE-BW.merger.merge_session")
_archive = importlib.import_module("optv.parliaments.DE-BW.scraper.fetch_archive")
Config = importlib.import_module("optv.parliaments.DE-BW.common").Config

FIXTURES = Path(__file__).parent / "fixtures" / "DE-BW"


# --------------------------------------------------------------------------- #
# Scraper: slug parsing + filterlist link extraction + WP date scoping
# --------------------------------------------------------------------------- #

def test_slug_helpers():
    u = "https://www.landtag-bw.de/de/mediathek/videos/118-sitzung-vom-13-maerz-2025-563198"
    assert _scraper.slug_sitzung(u) == 118
    assert _scraper.slug_date(u) == "2025-03-13"
    # umlaut month spelling + non-session slug
    assert _scraper.slug_date("/de/mediathek/videos/5-sitzung-vom-1-märz-2026-9") == "2026-03-01"
    assert _scraper.slug_sitzung(".../videos/oea-wirta-vom-24-februar-2026-619566") is None


def test_parse_filterlist_keeps_only_sessions():
    html = ('<a href="/de/mediathek/videos/118-sitzung-vom-13-maerz-2025-563198">x</a>'
            '<a href="/de/mediathek/videos/118-sitzung-vom-13-maerz-2025-563198">dup</a>'
            '<a href="/de/mediathek/videos/oea-wirta-vom-24-februar-2026-619566">skip</a>'
            '<a href="/de/mediathek/videos/videos-508226">skip</a>')
    urls = _scraper.parse_filterlist(html)
    assert urls == ["https://www.landtag-bw.de/de/mediathek/videos/118-sitzung-vom-13-maerz-2025-563198"]


def test_in_period_scoping():
    # WP17 = [2021-05-12, 2026-05-12); WP18 from 2026-05-12
    assert _archive._in_period("2025-03-13", 17) is True
    assert _archive._in_period("2026-05-12", 17) is False   # first WP18 day
    assert _archive._in_period("2026-05-12", 18) is True
    assert _archive._in_period("2021-01-01", 17) is False
    # unparseable date is kept (fetch_media derives the real WP)
    assert _archive._in_period(None, 17) is True


# --------------------------------------------------------------------------- #
# Scraper: parse the static chapter list
# --------------------------------------------------------------------------- #

def test_parse_video_page():
    html = (FIXTURES / "chapterlist-sample.html").read_text()
    doc = _scraper.parse_video_page(html)
    assert doc is not None
    assert doc["wp"] == 17 and doc["sitzung"] == 118 and doc["part"] == 1
    assert doc["date"] == "2025-03-13"
    assert doc["mp4_url"].endswith("Aufzeichnung_118_1.mp4")
    assert len(doc["tops"]) == 2
    assert doc["tops"][0]["title"] == "Beginn der Sitzung"
    assert doc["tops"][1]["title"] == "TOP 1 Aktuelle Debatte"
    speeches = doc["tops"][1]["speeches"]
    assert speeches[0] == {
        "name_raw": "Rülke Hans-Ulrich",
        "meta_raw": "| FDP/DVP",
        "start_offset": 483,
        "clock": "00:08:03",
    }
    assert doc["tops"][0]["speeches"][0]["meta_raw"] == "| Präsidentin | GRÜNE"


# --------------------------------------------------------------------------- #
# Parser: name reorder + meta split + end-offset computation
# --------------------------------------------------------------------------- #

def test_reorder_name():
    assert _parser._reorder_name("Aras Muhterem") == ("Muhterem Aras", "Muhterem", "Aras")
    assert _parser._reorder_name("Rülke Hans-Ulrich") == ("Hans-Ulrich Rülke", "Hans-Ulrich", "Rülke")
    assert _parser._reorder_name("Dr. Schmidt Hans") == ("Hans Schmidt", "Hans", "Schmidt")
    assert _parser._reorder_name("") == ("", "", "")


def test_split_meta():
    assert _parser._split_meta("| Präsidentin | GRÜNE") == ("Präsidentin", "GRÜNE")
    assert _parser._split_meta(" | FDP/DVP") == ("", "FDP/DVP")
    assert _parser._split_meta("| Ministerin | GRÜNE") == ("Ministerin", "GRÜNE")
    # role-only (a minister with no party segment)
    assert _parser._split_meta("| Minister für Verkehr") == ("Minister für Verkehr", "")
    assert _parser._split_meta("") == ("", "")


def test_parse_session_end_offsets(tmp_path):
    # Build a tops.json from the fixture and parse it.
    html = (FIXTURES / "chapterlist-sample.html").read_text()
    page = _scraper.parse_video_page(html)
    tops_path = tmp_path / "17118-tops.json"
    tops_path.write_text(json.dumps({
        "session_id": "17118", "wp": 17, "sitzung": 118, "part": 1,
        "date": "2025-03-13", "mp4_url": page["mp4_url"],
        "video_page_url": "https://www.landtag-bw.de/de/mediathek/videos/118-x-1",
        "tops": page["tops"],
    }))
    doc = _parser.parse_session(tops_path)
    data = doc["data"]
    assert len(data) == 3
    # sorted by offset: 1, 483, 936
    assert [s["start_offset"] for s in data] == [1, 483, 936]
    assert [s["speech_index"] for s in data] == [1, 2, 3]
    # end offset = next speech start; last is open
    assert data[0]["end_offset"] == 483
    assert data[1]["end_offset"] == 936
    assert data[2]["end_offset"] is None
    assert data[0]["label"] == "Muhterem Aras" and data[0]["role"] == "Präsidentin"
    assert data[1]["gruppe"] == "FDP/DVP"


# --------------------------------------------------------------------------- #
# End-to-end merge → Stage 2 shape + schema validation
# --------------------------------------------------------------------------- #

def _media_doc():
    mp4 = ("https://ltbw-stream.babiel.com/wahlperiode17/2025/"
           "sitzung118_20250313/Aufzeichnung_118_1.mp4")
    page = "https://www.landtag-bw.de/de/mediathek/videos/118-sitzung-vom-13-maerz-2025-563198"
    return {
        "meta": {
            "session": "17118", "wp": 17, "date": "2025-03-13", "sitzung": 118,
            "processing": {"parse_media": "2025-03-13T00:00:00"},
        },
        "data": [
            {"date": "2025-03-13", "wp": 17, "sitzung_no": 118,
             "top_index": 0, "top_title": "Beginn der Sitzung",
             "top_description": "Verabschiedung eines Abgeordneten",
             "name_raw": "Aras Muhterem", "label": "Muhterem Aras",
             "firstname": "Muhterem", "lastname": "Aras",
             "role": "Präsidentin", "gruppe": "GRÜNE",
             "start_offset": 1, "end_offset": 483, "start_clock": "0:00:01",
             "mp4_url": mp4, "video_page_url": page, "speech_index": 1,
             "speech_id": "17118_1",
             "start_datetime": "2025-03-13T00:00:01", "end_datetime": "2025-03-13T00:08:03"},
            {"date": "2025-03-13", "wp": 17, "sitzung_no": 118,
             "top_index": 1, "top_title": "TOP 1 Aktuelle Debatte",
             "top_description": "Eine wichtige aktuelle Frage",
             "name_raw": "Rülke Hans-Ulrich", "label": "Hans-Ulrich Rülke",
             "firstname": "Hans-Ulrich", "lastname": "Rülke",
             "role": "", "gruppe": "FDP/DVP",
             "start_offset": 483, "end_offset": None, "start_clock": "0:08:03",
             "mp4_url": mp4, "video_page_url": page, "speech_index": 2,
             "speech_id": "17118_483",
             "start_datetime": "2025-03-13T00:08:03", "end_datetime": "2025-03-13T00:08:03"},
        ],
    }


def test_merge_session_end_to_end(tmp_path):
    config = Config(tmp_path)
    config.dir("media", create=True)
    config.file("17118", "media", create=True).write_text(json.dumps(_media_doc()))

    out = _merger.merge_session("17118", config, None)
    doc = json.loads(out.read_text())
    data = doc["data"]
    assert len(data) == 2
    assert "merge" in doc["meta"]["processing"]

    assert data[0]["electoralPeriod"]["number"] == 17
    assert data[0]["session"]["number"] == 118
    assert data[0]["parliament"] == "DE-BW"

    # speech 1: president context, opening agenda type, faction present
    pres = data[0]
    assert pres["people"][0]["context"] == "president"
    assert pres["people"][0]["faction"]["label"] == "GRÜNE"
    assert pres["agendaItem"]["type"] == "opening"
    assert pres["agendaItem"]["nativeType"] == "DE-BW-opening"
    assert pres["textContents"] == []

    # speech 2: main-speaker, current_affairs, #t=start,end media + offsets
    rulke = data[1]
    assert rulke["people"][0]["context"] == "main-speaker"
    assert rulke["agendaItem"]["type"] == "current_affairs"
    assert rulke["media"]["videoFileURI"].endswith("Aufzeichnung_118_1.mp4#t=483")
    assert pres["media"]["videoFileURI"].endswith("#t=1,483")
    assert rulke["media"]["additionalInformation"]["startOffset"] == 483.0

    # downstream-required keys present on every speech
    required = {"speechIndex", "session", "electoralPeriod", "people",
                "textContents", "media", "agendaItem", "originID"}
    for sp in data:
        assert not (required - set(sp))

    # sourcePage unique per speech, monotonic dates
    assert len({sp["media"]["sourcePage"] for sp in data}) == len(data)
    assert data[0]["dateStart"] < data[1]["dateStart"]

    # full Stage 2 semantic validation: no errors
    findings = validate_stage2(doc, schema="full", semantic=True)
    errors = [f for f in findings if f["severity"] == "error"]
    assert errors == [], errors


# --------------------------------------------------------------------------- #
# Multi-part session (one calendar day split into sequential video files)
# --------------------------------------------------------------------------- #

def _speech(name, meta, off, clock):
    return {"name_raw": name, "meta_raw": meta, "start_offset": off, "clock": clock}


def _multipart_tops():
    mp4 = "https://ltbw-stream.babiel.com/wahlperiode17/2026/sitzung140_20260204/Aufzeichnung_140_{}.mp4"
    page = "https://www.landtag-bw.de/de/mediathek/videos/140-sitzung-vom-4-februar-2026-{}"
    return {
        "session_id": "17140", "wp": 17, "sitzung": 140, "date": "2026-02-04",
        "video_page_url": page.format("615632"),
        "parts": [
            {"part": 1, "mp4_url": mp4.format(1), "video_page_url": page.format("615632"),
             "tops": [
                 {"index": 0, "title": "TOP 3 Zweite Beratung", "description": "Gesetz A",
                  "speeches": [_speech("Aras Muhterem", "| Präsidentin | GRÜNE", 100, "0:01:40"),
                               _speech("Rülke Hans-Ulrich", "| FDP/DVP", 300, "0:05:00")]},
                 {"index": 1, "title": "TOP 4 Zweite Beratung", "description": "Gesetz B",
                  "speeches": [_speech("Evers Daniela", "| Ministerin | GRÜNE", 900, "0:15:00")]},
             ]},
            {"part": 2, "mp4_url": mp4.format(2), "video_page_url": page.format("615636"),
             "tops": [
                 {"index": 0, "title": "Fortsetzung TOP 4 Zweite Beratung", "description": "Gesetz B",
                  "speeches": [_speech("Miller Matthias", "| CDU", 50, "0:00:50")]},
                 {"index": 1, "title": "TOP 5 Regierungsbefragung", "description": "Fragen",
                  "speeches": [_speech("Aras Muhterem", "| Präsidentin | GRÜNE", 400, "0:06:40")]},
             ]},
        ],
    }


def test_multipart_parse_and_merge(tmp_path):
    config = Config(tmp_path)
    media_dir = config.dir("media", create=True)
    (media_dir / "17140-tops.json").write_text(json.dumps(_multipart_tops()))

    _parser.parse_media_directory(media_dir)
    media = json.loads((media_dir / "17140-media.json").read_text())["data"]

    # 5 speeches across 2 parts, global speechIndex 1..5 ordered by (part, offset)
    assert len(media) == 5
    assert [s["speech_index"] for s in media] == [1, 2, 3, 4, 5]
    assert [s["part"] for s in media] == [1, 1, 1, 2, 2]
    # per-part end offset: last speech of part 1 has no successor in its part
    p1_last = [s for s in media if s["part"] == 1][-1]
    assert p1_last["end_offset"] is None          # not chained into part 2
    assert media[0]["end_offset"] == 300          # next within part 1
    assert media[3]["end_offset"] == 400          # part-2 first → part-2 second
    # synthetic start times are globally monotonic across the part boundary
    assert [s["start_datetime"] for s in media] == sorted(s["start_datetime"] for s in media)

    out = _merger.merge_session("17140", config, None)
    data = json.loads(out.read_text())["data"]

    # part-2 speeches reference the part-2 MP4 with their own offset
    p2 = [s for s in data if s["debug"]["part"] == 2]
    assert all("Aufzeichnung_140_2.mp4#t=" in s["media"]["videoFileURI"] for s in p2)
    assert any(s["media"]["videoFileURI"].endswith("_140_2.mp4#t=50,400") for s in data)
    # the split debate collapses: TOP 4 (part 1) and Fortsetzung TOP 4 (part 2)
    # share one agendaItem id
    top4 = {s["agendaItem"]["id"] for s in data
            if "TOP 4" in s["agendaItem"]["title"]}
    assert top4 == {"TOP-4"}
    # sourcePage unique across all parts (per-part page URL + offset)
    assert len({s["media"]["sourcePage"] for s in data}) == len(data)

    findings = validate_stage2({"meta": json.loads(out.read_text())["meta"], "data": data},
                               schema="full", semantic=True)
    assert [f for f in findings if f["severity"] == "error"] == []
