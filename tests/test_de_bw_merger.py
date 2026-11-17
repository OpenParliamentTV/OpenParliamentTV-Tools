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
_fetch_media = importlib.import_module("optv.parliaments.DE-BW.scraper.fetch_media")
Config = importlib.import_module("optv.parliaments.DE-BW.common").Config

from optv.shared.pdf2tei.sitting_times import parse_sitting_times

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
    # WP 15–17 form: "Lastname Firstname", first token = surname
    assert _parser._reorder_name("Aras Muhterem") == ("Muhterem Aras", "Muhterem", "Aras")
    assert _parser._reorder_name("Rülke Hans-Ulrich") == ("Hans-Ulrich Rülke", "Hans-Ulrich", "Rülke")
    assert _parser._reorder_name("Dr. Schmidt Hans") == ("Hans Schmidt", "Hans", "Schmidt")
    assert _parser._reorder_name("Eyb Arnulf Freiherr von") == (
        "Arnulf Freiherr von Eyb", "Arnulf Freiherr", "von Eyb")
    assert _parser._reorder_name("") == ("", "", "")


def test_reorder_name_comma_form():
    # WP 18 form: "Lastname, Firstname" — split at the comma
    assert _parser._reorder_name("Strobl, Thomas") == ("Thomas Strobl", "Thomas", "Strobl")
    assert _parser._reorder_name("Lede Abal, Daniel") == (
        "Daniel Lede Abal", "Daniel", "Lede Abal")
    assert _parser._reorder_name("Kliche-Behnke, Dorothea") == (
        "Dorothea Kliche-Behnke", "Dorothea", "Kliche-Behnke")
    # trailing particle moves to the surname
    assert _parser._reorder_name("Loga, Cornelia von") == (
        "Cornelia von Loga", "Cornelia", "von Loga")
    # honorific on either side of the comma
    assert _parser._reorder_name("Rülke, Dr. Hans-Ulrich") == (
        "Hans-Ulrich Rülke", "Hans-Ulrich", "Rülke")
    assert _parser._reorder_name("Dr. Rülke, Hans-Ulrich") == (
        "Hans-Ulrich Rülke", "Hans-Ulrich", "Rülke")
    assert _parser._reorder_name("Strobl,") == ("Strobl", "", "Strobl")


def test_split_name_role():
    # an office written into the name span (non-member, empty meta span)
    assert _parser._split_name_role("Böhlen, Beate | Bürgerbeauftragte", "") == (
        "Böhlen, Beate", "| Bürgerbeauftragte")
    assert _parser._split_name_role("Strobl, Thomas", "| Präsident | CDU") == (
        "Strobl, Thomas", "| Präsident | CDU")


def test_parse_p18_chapter_list(tmp_path):
    html = (FIXTURES / "chapterlist-p18-sample.html").read_text()
    page = _scraper.parse_video_page(html)
    assert page["wp"] == 18 and page["sitzung"] == 14 and page["date"] == "2026-10-21"
    assert page["tops"][0]["speeches"][0]["name_raw"] == "Strobl, Thomas"
    tops_path = tmp_path / "18014-tops.json"
    tops_path.write_text(json.dumps({
        "session_id": "18014", "wp": 18, "sitzung": 14, "date": "2026-10-21",
        "parts": [{"part": 1, "mp4_url": page["mp4_url"],
                   "video_page_url": "https://www.landtag-bw.de/de/mediathek/videos/14-x-659748",
                   "tops": page["tops"]}],
    }))
    data = _parser.parse_session(tops_path)["data"]
    assert [(s["label"], s["firstname"], s["lastname"]) for s in data] == [
        ("Thomas Strobl", "Thomas", "Strobl"),
        ("Daniel Lede Abal", "Daniel", "Lede Abal"),
        ("Cornelia von Loga", "Cornelia", "von Loga"),
    ]
    assert data[0]["role"] == "Präsident" and data[0]["gruppe"] == "CDU"


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

    # sourcePage: the session page + the ?t= start the page's player reads
    assert pres["media"]["sourcePage"].endswith("-563198?t=1")
    assert rulke["media"]["sourcePage"].endswith("-563198?t=483")
    assert len({sp["media"]["sourcePage"] for sp in data}) == len(data)
    # no protocol → synthetic video-relative times, flagged
    assert data[0]["dateStart"] < data[1]["dateStart"]
    assert pres["debug"]["timesAreVideoRelative"] is True
    assert pres["debug"]["timeSource"] == "video"

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


# --------------------------------------------------------------------------- #
# Wall-clock times from the protocol's sitting times
# --------------------------------------------------------------------------- #

def test_parse_sitting_times():
    # 17118 shape: cover summary + body lines; a short vote-count break in the
    # afternoon besides the lunch break; two-column text interleaves lines.
    text = ("Beginn: 9:33 Uhr  Mittagspause: 12:45 bis 14:00 Uhr  Schluss: 16:12 Uhr "
            "… Beginn: 9:33 Uhr … (Unterbrechung der Sitzung: 12:45 Uhr) Text der "
            "anderen Spalte (Wiederaufnahme der Sitzung: 14:00 Uhr) … "
            "(Unterbrechung der Sitzung: 16:06 Uhr) … (Wiederauf- nahme der "
            "Sitzung: 16:11 Uhr) … Schluss: 16:12 Uhr")
    assert parse_sitting_times(text) == {
        "begin": "09:33", "end": "16:12",
        "breaks": [["12:45", "14:00"], ["16:06", "16:11"]],
    }
    assert parse_sitting_times("Beginn: 9.03 Uhr … Schluss: 12.39 Uhr") == {
        "begin": "09:03", "end": "12:39", "breaks": []}
    assert parse_sitting_times("Beginn: 9:03 Uhr") is None
    assert parse_sitting_times("") is None


_SITTING_TIMES = {
    "begin": "10:03", "end": "18:21",
    # a short break inside part 1 (recording continues) + the lunch break
    "breaks": [["10:10", "10:12"], ["10:30", "14:46"]],
}


def _write_proceedings(config, session, times):
    config.dir("proceedings", create=True)
    config.file(session, "proceedings", create=True).write_text(
        json.dumps({"meta": {"session": session, "sittingTimes": times}, "data": []}))


def test_multipart_wall_clock_from_protocol(tmp_path):
    config = Config(tmp_path)
    media_dir = config.dir("media", create=True)
    (media_dir / "17140-tops.json").write_text(json.dumps(_multipart_tops()))
    _parser.parse_media_directory(media_dir)
    _write_proceedings(config, "17140", _SITTING_TIMES)

    doc = json.loads(_merger.merge_session("17140", config, None).read_text())
    data = doc["data"]
    # part 1 (offsets 100, 300, 900) starts at Beginn 10:03; the 10:10 break
    # starts before part 1's last entry (10:18), so it does not split the video
    # and part 2 (offsets 50, 400) starts at the 14:46 resumption
    assert [(s["dateStart"], s["dateEnd"]) for s in data] == [
        ("2026-02-04T10:04:40+01:00", "2026-02-04T10:08:00+01:00"),
        ("2026-02-04T10:08:00+01:00", "2026-02-04T10:18:00+01:00"),
        ("2026-02-04T10:18:00+01:00", "2026-02-04T10:30:00+01:00"),  # → lunch break
        ("2026-02-04T14:46:50+01:00", "2026-02-04T14:52:40+01:00"),
        ("2026-02-04T14:52:40+01:00", "2026-02-04T18:21:00+01:00"),  # → Schluss
    ]
    assert data[0]["session"]["dateStart"] == "2026-02-04T10:03:00+01:00"
    assert data[0]["session"]["dateEnd"] == "2026-02-04T18:21:00+01:00"
    assert doc["meta"]["dateStart"] == "2026-02-04T10:03:00+01:00"
    assert all(s["debug"]["timesAreVideoRelative"] is False for s in data)
    assert all(s["debug"]["timeSource"] == "protocol" for s in data)
    assert data[3]["debug"]["partStart"] == "2026-02-04T14:46:00+01:00"
    findings = validate_stage2(doc, schema="full", semantic=True)
    assert [f for f in findings if f["severity"] == "error"] == []


def test_wall_clock_summer_offset():
    speeches = [{"part": 1, "start_offset": 1, "end_offset": None}]
    clock = _merger._wall_clock(speeches, {"begin": "09:33", "end": "16:44",
                                           "breaks": []}, "2026-10-08")
    per_speech, start, end, _ = clock
    assert per_speech == [("2026-10-08T09:33:01+02:00", "2026-10-08T16:44:00+02:00")]
    assert (start, end) == ("2026-10-08T09:33:00+02:00", "2026-10-08T16:44:00+02:00")


def test_wall_clock_falls_back_when_times_do_not_fit(tmp_path):
    config = Config(tmp_path)
    media_dir = config.dir("media", create=True)
    (media_dir / "17140-tops.json").write_text(json.dumps(_multipart_tops()))
    _parser.parse_media_directory(media_dir)
    # no break after part 1's last entry → part 2 cannot be anchored
    _write_proceedings(config, "17140", {"begin": "10:03", "end": "18:21",
                                         "breaks": [["10:10", "10:12"]]})
    data = json.loads(_merger.merge_session("17140", config, None).read_text())["data"]
    assert all(s["debug"]["timesAreVideoRelative"] is True for s in data)
    assert data[0]["dateStart"] == "2026-02-04T00:01:40"   # synthetic, naive
    # parts not numbered 1..n, or a last part overrunning Schluss
    sp = [{"part": 2, "start_offset": 5, "end_offset": None}]
    assert _merger._part_anchors(sp, _SITTING_TIMES, "2026-02-04") is None
    sp = [{"part": 1, "start_offset": 9 * 3600, "end_offset": None}]
    assert _merger._part_anchors(sp, _SITTING_TIMES, "2026-02-04") is None


# --------------------------------------------------------------------------- #
# Session discovery: incremental archive refresh + late part cards
# --------------------------------------------------------------------------- #

_V = "/de/mediathek/videos/"


def _card(slug):
    return f'<a href="{_V}{slug}">x</a>'


def _fake_filterlist(monkeypatch, pages):
    requested = []

    def fake(url, **kw):
        off = int(url.split("offset=")[1].split("&")[0])
        requested.append(off)
        return pages.get(off, "")
    monkeypatch.setattr(_scraper, "fetch_text", fake)
    return requested


def test_fetch_session_urls_stops_at_known(monkeypatch):
    known_url = f"{_scraper.BASE}{_V}12-sitzung-vom-1-oktober-2026-3"
    pages = {
        0: '"numberOfResults": 100 ' + _card("14-sitzung-vom-21-oktober-2026-5"),
        12: _card("oea-anhoerung-vom-15-oktober-2026-9"),          # no sitting: go on
        24: _card("13-sitzung-vom-8-oktober-2026-4") + _card(
            "12-sitzung-vom-1-oktober-2026-3"),                        # new + known: go on
        36: _card("12-sitzung-vom-1-oktober-2026-3"),                 # known only: stop
        48: _card("11-sitzung-vom-24-september-2026-2"),
    }
    requested = _fake_filterlist(monkeypatch, pages)
    urls = _scraper.fetch_session_urls(known={known_url}, period_start="2026-05-12")
    assert requested == [0, 12, 24, 36]
    assert [u.rsplit("/", 1)[1] for u in urls] == [
        "14-sitzung-vom-21-oktober-2026-5", "13-sitzung-vom-8-oktober-2026-4",
        "12-sitzung-vom-1-oktober-2026-3"]


def test_fetch_session_urls_stops_before_period(monkeypatch):
    pages = {
        0: '"numberOfResults": 100 ' + _card("1-sitzung-vom-12-mai-2026-1"),
        12: _card("150-sitzung-vom-29-april-2026-7"),                  # all older: stop
        24: _card("149-sitzung-vom-28-april-2026-6"),
    }
    requested = _fake_filterlist(monkeypatch, pages)
    _scraper.fetch_session_urls(period_start="2026-05-12")
    assert requested == [0, 12]


def test_fetch_media_refetches_when_a_part_is_new(tmp_path, monkeypatch):
    page1 = "https://www.landtag-bw.de/de/mediathek/videos/140-sitzung-vom-4-februar-2026-615632"
    page2 = "https://www.landtag-bw.de/de/mediathek/videos/140-sitzung-vom-4-februar-2026-615636"
    (tmp_path / "17140-tops.json").write_text(json.dumps({
        "session_id": "17140", "video_page_url": page1,
        "parts": [{"part": 1, "video_page_url": page1, "tops": []}]}))
    fetched = []

    def fake(url, **kw):
        fetched.append(url)
        return (FIXTURES / "chapterlist-sample.html").read_text()
    monkeypatch.setattr(_fetch_media, "fetch_text", fake)

    entry = {"sitzung": 140, "date": "2026-02-04", "session_id": "17140"}
    archive = {"sessions": [dict(entry, video_page_url=page1)]}
    _fetch_media.fetch_media_for_archive(archive=archive, media_dir=tmp_path)
    assert fetched == []                                   # all parts covered
    archive["sessions"].append(dict(entry, video_page_url=page2))
    _fetch_media.fetch_media_for_archive(archive=archive, media_dir=tmp_path)
    assert fetched == [page1, page2]                       # late part 2 → refetch
