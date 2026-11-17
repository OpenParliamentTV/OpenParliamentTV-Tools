"""DE-HH tests: mediathek page parsing, per-speech clips, subtitles, merge.

No network: the scraper parser runs against a trimmed copy of a live session
page, the subtitle helpers against two overlapping HLS WebVTT segments, and the
merger against the intermediate media JSON built from them. The DE-HH package
directory has a hyphen, so it is imported via ``importlib``.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

from optv.shared.lang.de import match_key_surname, spacy_sentencize
from optv.shared.validators import validate_stage2
from optv.shared.vtt import cues_to_sentences, merge_segments, parse_vtt, write_vtt

_scraper = importlib.import_module("optv.parliaments.DE-HH.scraper.common")
_parser = importlib.import_module("optv.parliaments.DE-HH.parsers.media2json")
_merger = importlib.import_module("optv.parliaments.DE-HH.merger.merge_session")
_workflow = importlib.import_module("optv.parliaments.DE-HH.workflow")
_proceedings = importlib.import_module("optv.parliaments.DE-HH.scraper.fetch_proceedings")
Config = importlib.import_module("optv.parliaments.DE-HH.common").Config

FIXTURES = Path(__file__).parent / "fixtures" / "DE-HH"
QUAST = "d43ac9e5-464d-452c-87aa-7bf44e591093"


def _page() -> str:
    return (FIXTURES / "sitzung-23-18-sample.html").read_text()


def _segments() -> list[str]:
    return [(FIXTURES / f"subtitles-segment-{i}.vtt").read_text() for i in (1, 2)]


# --------------------------------------------------------------------------- #
# Session page parsing
# --------------------------------------------------------------------------- #

def test_parse_session_page():
    doc = _scraper.parse_session_page(_page())
    assert doc["date"] == "2026-02-11"
    assert doc["session_uuid"] == "c528c03e-0182-4467-825c-cab6adef066f"
    items = doc["items"]
    # the whole-sitting <video id="session-…"> is not an agenda item
    assert [it["sid"] for it in items] == ["bba988fc-9e83-4a27-a1eb-0bc0d144c023",
                                          "2dc869f9-b8cc-434d-8d7c-b95877f352bc"]
    aktuelle, top38 = items
    assert aktuelle["top_number"] is None
    assert aktuelle["title"].startswith("AKTUELLE STUNDE Schreckliche Gewaltspirale")
    assert top38["top_number"] == "38"
    assert top38["clean_hls"].startswith(
        "https://mediathek.buergerschaft-hh.de/hls/clipFrom/6644000/clipTo/8147000/")
    assert "/sign_" in top38["sign_hls"]
    quast = top38["speeches"][0]
    assert quast["speech_pk"] == QUAST
    assert quast["legacy_id"] is None
    assert (quast["start_offset"], quast["duration"]) == (30.0, 282.0)
    assert (quast["download_start"], quast["download_stop"]) == (1770819440, 1770819722)


def test_parse_session_page_legacy_id_and_date_fallback():
    # WP 22 pages carry a numeric data-speechLegacyId.
    page = _page().replace('data-speechLegacyId="None"', 'data-speechLegacyId="22547"', 1)
    assert _scraper.parse_session_page(page)["items"][0]["speeches"][0]["legacy_id"] == 22547
    # Without a dated clip URL, the date is the local day of the first speech.
    undated = _page().replace("/2026-02-11/", "/")
    assert _scraper.parse_session_page(undated)["date"] == "2026-02-11"


def test_speech_clip_url():
    item = ("https://mediathek.buergerschaft-hh.de/hls/clipFrom/4223000/clipTo/9999000/"
            "2026-05-06/clean_x/,a.mp4,lang/deu/2026-05-06_clean.vtt,.urlset/master.m3u8")
    # Thering, 23/23: item clipFrom 4223000, data-start 1291, data-duration 760.
    url = _scraper.speech_clip_url(item, 1291, 1291 + 760)
    assert "/clipFrom/5514000/clipTo/6274000/2026-05-06/clean_x/" in url
    assert url.endswith(".urlset/master.m3u8")
    # A speech starting before its TOP's clip is cut from the day's recording.
    assert "/clipFrom/4218000/clipTo/4614000/" in _scraper.speech_clip_url(item, -5, 391)
    assert _scraper.speech_clip_url("https://x/no-range/master.m3u8", 1, 2) == ""


def test_has_subtitles():
    top38 = _scraper.parse_session_page(_page())["items"][1]
    assert _scraper.has_subtitles(top38["clean_hls"])
    # WP 22 before 2022-06-15: no WebVTT file in the urlset.
    assert not _scraper.has_subtitles(
        "/hls/clipFrom/869000/clipTo/885000/2022-06-01/clean_cc/2022-06-01_,0446kbps.mp4,"
        "0846kbps.mp4,.urlset/master.m3u8")


def test_max_session_in_plenary_list():
    html = (FIXTURES / "plenar-sample.html").read_text()
    assert _scraper.max_session_in(html, 23) == 32
    assert _scraper.max_session_in(html, 22) is None


# --------------------------------------------------------------------------- #
# Subtitles: segment merge + timed sentences
# --------------------------------------------------------------------------- #

def test_merge_segments_drops_repeated_cues():
    cues = merge_segments(_segments())
    assert [c.id for c in cues] == ["1280", "1281", "1282", "1283", "1284"]
    assert cues[0].start == 1.44 and cues[0].end == 5.08
    # multi-line cue text is joined
    assert cues[0].text == ("Ja, Herr Präsident, meine Damen und Herren, "
                            "mir obliegt jetzt die große Aufgabe,")
    # round trip
    assert [c.id for c in parse_vtt(write_vtt(cues))] == [c.id for c in cues]


def test_cues_to_sentences_timing():
    sents = cues_to_sentences(merge_segments(_segments()), spacy_sentencize, duration=20.0)
    assert [s["text"] for s in sents] == [
        "Ja, Herr Präsident, meine Damen und Herren, mir obliegt jetzt die große "
        "Aufgabe, nach dieser hochemotionalen Debatte über Bürokratieabbau zu "
        "sprechen, aber wir werden das gemeinsam hinbekommen.",
        "Gut gemeint ist leider nicht immer gut gemacht oder aber beim Elterngeld "
        "haben wir ein Paradebeispiel von Bürokratisierung sowie der Notwendigkeit, "
        "wieder gegenzusteuern.",
    ]
    first, second = sents
    assert first["timeStart"] == "1.440"
    # the sentence boundary falls inside cue 1282 (9.24–14.28): interpolated
    assert 9.24 < float(first["timeEnd"]) < 14.28
    assert float(second["timeStart"]) >= float(first["timeEnd"]) - 0.1
    # clamped to the clip duration (cue 1284 ends at 21.2)
    assert second["timeEnd"] == "20.000"


# --------------------------------------------------------------------------- #
# Parser + merger end to end
# --------------------------------------------------------------------------- #

def _write_session(tmp_path, *, subtitles: bool) -> Config:
    config = Config(tmp_path)
    page = _scraper.parse_session_page(_page())
    items = {"session_id": "23018", "wp": 23, "sitzung": 18, "date": page["date"],
             "session_uuid": page["session_uuid"],
             "video_page_url": "https://mediathek.buergerschaft-hh.de/sitzung/23/18/",
             "items": page["items"]}
    items_path = config.dir("media", create=True) / "23018-items.json"
    items_path.write_text(json.dumps(items))
    if subtitles:
        sub_dir = config.dir("subtitles") / "23018"
        sub_dir.mkdir(parents=True)
        (sub_dir / f"{QUAST}.vtt").write_text(write_vtt(merge_segments(_segments())))
    _parser.parse_media_directory(config.dir("media"), config.dir("subtitles"))
    return config


def _merge(config) -> dict:
    return json.loads(_merger.merge_session("23018", config, None).read_text())


def test_parse_session_per_speech_clips(tmp_path):
    config = _write_session(tmp_path, subtitles=True)
    data = json.loads(config.file("23018", "media").read_text())["data"]
    assert [s["speech_index"] for s in data] == [1, 2, 3, 4]
    quast = next(s for s in data if s["speech_pk"] == QUAST)
    assert "/clipFrom/6674000/clipTo/6956000/" in quast["video_uri"]
    # sign clip: same offsets on the sign stream's own range (6640000)
    assert "/clipFrom/6670000/clipTo/6952000/" in quast["sign_video_uri"]
    assert quast["subtitle_sentences"][0]["timeStart"] == "1.440"
    assert all("subtitle_sentences" not in s for s in data if s["speech_pk"] != QUAST)


def test_merge_per_speech_media(tmp_path):
    doc = _merge(_write_session(tmp_path, subtitles=False))
    data = doc["data"]
    assert len(data) == 4
    for sp in data:
        media = sp["media"]
        assert "#t=" not in media["videoFileURI"]
        assert "/clipFrom/" in media["videoFileURI"]
        assert media["additionalInformation"]["startOffset"] == 0.0
        assert media["additionalInformation"]["endOffset"] == media["duration"]
        assert "originID" not in sp
        assert len(media["originMediaID"]) == 36          # the source's speech UUID
        assert sp["debug"]["timesAreVideoRelative"] is False
        assert sp["textContents"] == []
    quast = next(s for s in data if s["media"]["originMediaID"] == QUAST)
    assert quast["media"]["duration"] == 282.0
    assert quast["debug"]["startOffset"] == 30.0            # TOP-relative, debug only
    assert quast["debug"]["speechLegacyId"] is None
    assert quast["dateStart"] == "2026-02-11T14:17:20Z"
    pages = [s["media"]["sourcePage"] for s in data]
    assert len(set(pages)) == len(pages)
    errors = [f for f in validate_stage2(doc, schema="full", semantic=True)
              if f["severity"] == "error"]
    assert errors == []


def test_merge_subtitle_text(tmp_path):
    config = _write_session(tmp_path, subtitles=True)
    doc = _merge(config)
    quast = next(s for s in doc["data"] if s["media"]["originMediaID"] == QUAST)
    (tc,) = quast["textContents"]
    assert tc["type"] == "generated"
    assert tc["creator"] == "Hamburgische Bürgerschaft"
    body = tc["textBody"][0]
    assert body["speaker"] == "Anja Quast"
    assert body["sentences"][0]["timeStart"] == "1.440"
    assert quast["debug"]["textSource"] == "subtitles"
    assert quast["debug"]["confidence"] == 1.0
    # subtitle-only speeches count as text for the session status
    assert quast["debug"]["proceedingIndex"] == quast["speechIndex"]
    assert config.has_merged_text(quast)
    errors = [f for f in validate_stage2(doc, schema="full", semantic=True)
              if f["severity"] == "error"]
    assert errors == []


def _write_turns(config, turns: list[dict]) -> None:
    config.dir("proceedings", create=True)
    config.file("23018", "proceedings").write_text(json.dumps({"meta": {}, "data": turns}))


def test_merge_protocol_wins_over_subtitles(tmp_path):
    config = _write_session(tmp_path, subtitles=True)
    _write_turns(config, [{"matchKey": match_key_surname("Anja Quast"), "index": 7,
                           "originTextID": "u23018_7",
                           "sentences": [{"text": "Sehr geehrter Herr Präsident!"}]}])
    quast = next(s for s in _merge(config)["data"] if s["media"]["originMediaID"] == QUAST)
    (tc,) = quast["textContents"]
    assert tc["type"] == "proceedings"
    assert tc["textBody"][0]["sentences"][0]["text"] == "Sehr geehrter Herr Präsident!"
    assert "textSource" not in quast["debug"]


def test_merge_cps_cap_still_gates_protocol_join(tmp_path):
    config = _write_session(tmp_path, subtitles=False)
    # 30,000 characters for a 282 s clip is > 100 chars/s: an implausible join.
    _write_turns(config, [{"matchKey": match_key_surname("Anja Quast"), "index": 7,
                           "sentences": [{"text": "x" * 30000}]}])
    quast = next(s for s in _merge(config)["data"] if s["media"]["originMediaID"] == QUAST)
    assert quast["debug"]["confidence"] == 0.5
    assert quast["debug"]["confidenceReason"] == "cps-cap"


# --------------------------------------------------------------------------- #
# Align hook: subtitle timing is stamped, not re-aligned
# --------------------------------------------------------------------------- #

def test_stamp_subtitle_timing(tmp_path):
    doc = _merge(_write_session(tmp_path, subtitles=True))
    assert _workflow.stamp_subtitle_timing(doc["data"]) == 1
    quast = next(s for s in doc["data"] if s["media"]["originMediaID"] == QUAST)
    assert quast["debug"]["alignSource"] == "subtitles"
    assert quast["debug"]["alignDuration"] > 0
    assert quast["media"]["aligned"] is True
    others = [s for s in doc["data"] if s is not quast]
    assert all("alignDuration" not in s["debug"] for s in others)


def test_align_hook_writes_aligned_file_without_aeneas(tmp_path, monkeypatch):
    config = _write_session(tmp_path, subtitles=True)
    _merge(config)

    def _no_aeneas(*a, **k):
        raise AssertionError("aeneas must not run for subtitle text")
    monkeypatch.setattr(_workflow, "_aeneas_align", _no_aeneas)
    out = _workflow._align(config, "23018", None)
    doc = json.loads(out.read_text())
    assert out == config.file("23018", "aligned")
    assert "align" in doc["meta"]["processing"]
    assert sum(1 for s in doc["data"] if s["debug"].get("alignSource") == "subtitles") == 1


# --------------------------------------------------------------------------- #
# Protocol arriving for a subtitle-text sitting → rebuild queue
# --------------------------------------------------------------------------- #

def test_queue_protocol_upgrades(tmp_path):
    config = _write_session(tmp_path, subtitles=True)
    merged = _merger.merge_session("23018", config, None)
    processed = config.file("23018", "processed", create=True)
    processed.write_text(merged.read_text())
    assert _proceedings.queue_protocol_upgrades(config, ["23018", "23019"]) == ["23018"]
    # idempotent: a second arrival does not duplicate the entry
    _proceedings.queue_protocol_upgrades(config, ["23018"])
    queue = config.dir("cache") / _proceedings.UPGRADES_FILE
    assert queue.read_text().split() == ["23018"]


# --------------------------------------------------------------------------- #
# Subtitle fetch: master → subtitle playlist → segments (HTTP stubbed)
# --------------------------------------------------------------------------- #

_fetch_subs = importlib.import_module("optv.parliaments.DE-HH.scraper.fetch_subtitles")

_MASTER = """#EXTM3U

#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs0",NAME="Deutsch",LANGUAGE="de",AUTOSELECT=YES,DEFAULT=YES,URI="index-f5.m3u8"

#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=447101,RESOLUTION=426x240,SUBTITLES="subs0"
index-f1-v1-a1.m3u8
"""
_PLAYLIST = """#EXTM3U
#EXT-X-TARGETDURATION:9
#EXT-X-MEDIA-SEQUENCE:1
#EXTINF:9.000,
segment-1-f5.vtt
#EXTINF:9.000,
segment-2-f5.vtt
#EXT-X-ENDLIST
"""


def _stub_http(monkeypatch, *, missing: str | None = None):
    base = "https://m/hls/clipFrom/6674000/clipTo/6956000/x/.urlset/"
    pages = {base + "master.m3u8": _MASTER, base + "index-f5.m3u8": _PLAYLIST,
             base + "segment-1-f5.vtt": _segments()[0],
             base + "segment-2-f5.vtt": _segments()[1]}
    if missing:
        pages[base + missing] = None
    monkeypatch.setattr(_fetch_subs, "fetch_text", lambda url, **kw: pages[url])
    return base + "master.m3u8"


def test_fetch_speech_subtitles(monkeypatch):
    vtt = _fetch_subs.fetch_speech_subtitles(_stub_http(monkeypatch))
    assert [c.id for c in parse_vtt(vtt)] == ["1280", "1281", "1282", "1283", "1284"]


def test_fetch_speech_subtitles_missing_segment_raises(monkeypatch):
    url = _stub_http(monkeypatch, missing="segment-2-f5.vtt")
    with pytest.raises(RuntimeError):
        _fetch_subs.fetch_speech_subtitles(url)
