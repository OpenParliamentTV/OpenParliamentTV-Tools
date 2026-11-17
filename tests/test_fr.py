"""FR (Assemblée nationale) adapter tests.

Covers the clock-time model (recording start from the HLS file name plus the
compte rendu's ``stime``, in Europe/Paris with the right DST offset), the
HTML-compte-rendu media resolver and its gates, the content-compare refresh of
republished comptes rendus, and the workflow's media scope gate. Synthetic
fixtures in ``tests/fixtures/FR/`` describe one CET sitting (2026-11-12 15:00,
recording ``hemi_20261112144538``); no network is touched.
"""

from __future__ import annotations

import datetime
import io
import json
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from optv.parliaments.FR import common as C
from optv.parliaments.FR import workflow as W
from optv.parliaments.FR.merger import merge_session as MS
from optv.parliaments.FR.parsers import media2json, proceedings2json
from optv.parliaments.FR.scraper import fetch_media as FM
from optv.parliaments.FR.scraper import fetch_proceedings as FP

FIXTURES = Path(__file__).parent / "fixtures" / "FR"
SESSION = "2027O1N061"
HLS = ("https://videos-diffusion.assemblee-nationale.fr/videos/definst/mp4/ida/"
       "domain1/2026/11/hemi_20261112144538.smil/master.m3u8")


def _cr_xml() -> str:
    return (FIXTURES / "cr-sample.xml").read_text(encoding="utf-8")


def _cr_html() -> str:
    return (FIXTURES / "cr-sample.html").read_text(encoding="utf-8")


def _provisional(xml: str) -> str:
    import re
    xml = xml.replace("<etat>complet</etat>", "<etat>provisoire</etat>")
    return re.sub(r'\s+stime="[^"]*"', "", xml)


def _args(**kw):
    base = dict(period=17, retry_count=1, retry_delay_max=0.0, force=False,
                fr_session=[], limit_session="")
    base.update(kw)
    return SimpleNamespace(**base)


def _config(tmp_path, xml: str | None = None) -> C.Config:
    config = C.Config(tmp_path)
    if xml is not None:
        cr = config.raw_cr(SESSION)
        cr.parent.mkdir(parents=True, exist_ok=True)
        cr.write_text(xml, encoding="utf-8")
    return config


# ---- clock helpers ----

def test_recording_start_is_paris_local_time_in_both_seasons():
    cet = C.recording_start(HLS)
    assert cet.isoformat() == "2026-11-12T14:45:38+01:00"
    cest = C.recording_start(".../2026/05/hemi_20260529211831.smil/master.m3u8")
    assert cest.isoformat() == "2026-05-29T21:18:31+02:00"
    assert C.recording_id(HLS) == "hemi_20261112144538"
    assert C.recording_start("https://example.org/other.m3u8") is None
    assert C.recording_id("") is None


def test_seance_start_uses_paris_offset():
    assert C.seance_start("20261112150000000").isoformat() == "2026-11-12T15:00:00+01:00"
    assert C.seance_start("20260529213000000").isoformat() == "2026-05-29T21:30:00+02:00"
    assert C.seance_start("") is None


def test_clock_crosses_dst_change():
    # On 2026-10-25 the clocks go back from 03:00 CEST to 02:00 CET: one
    # elapsed hour after 02:30 CEST is 02:30 CET (wall-clock arithmetic would
    # give 03:30).
    anchor = datetime.datetime(2026, 10, 25, 2, 30, tzinfo=C.PARIS)
    assert anchor.utcoffset() == datetime.timedelta(hours=2)
    assert MS._clock(anchor, 3600) == "2026-10-25T02:30:00+01:00"


def test_cr_page_url_uses_uid():
    assert C.cr_page_url(SESSION) == (
        "https://www.assemblee-nationale.fr/dyn/17/comptes-rendus/seance/"
        "CRSANR5L17S2027O1N061")


# ---- proceedings parser ----

def test_parser_emits_offsets_but_no_clock_times():
    doc = proceedings2json.parse_cr(_cr_xml().encode(), SESSION, {}, None)
    meta = doc["meta"]
    assert meta["dateStart"] == "2026-11-12T15:00:00+01:00"   # scheduled start
    assert meta["crState"] == "complet"
    assert "dateEnd" not in meta
    speeches = doc["data"]
    assert [s["debug"]["stime"] for s in speeches] == [877.79, 900.0, 1000.0]
    assert all("dateStart" not in s for s in speeches)
    assert [s["originID"] for s in speeches] == ["4100001", "4100004", "4100006"]


# ---- media resolver ----

def test_parse_cr_page_reads_seance_url_and_offsets():
    seance_url, offsets = FM.parse_cr_page(_cr_html())
    assert seance_url == HLS
    assert set(offsets) == {"4100001", "4100004", "4100005", "4100006"}
    assert offsets["4100001"] == (HLS, 877.79)
    stimes = {"4100001": 877.79, "4100004": 900.0, "4100005": 930.5, "4100006": 1000.0}
    assert FM.offset_mismatches(offsets, stimes) == 0
    # HTML rounds to 0.1 s above 10000 s: within tolerance.
    assert FM.offset_mismatches({"x": (HLS, 10001.2)}, {"x": 10001.17}) == 0
    stale = dict(stimes, **{"4100004": 902.1})
    assert FM.offset_mismatches(offsets, stale) == 1


def _patch_network(monkeypatch, page: str, status: int = 200):
    calls = {"get": 0, "status": 0}

    def fake_get(url, **_kw):
        calls["get"] += 1
        return page

    def fake_status(url, **_kw):
        calls["status"] += 1
        return status

    monkeypatch.setattr(FM, "http_get", fake_get)
    monkeypatch.setattr(FM, "http_status", fake_status)
    return calls


def test_resolve_writes_event_with_recording_start(tmp_path, monkeypatch):
    config = _config(tmp_path, _cr_xml())
    _patch_network(monkeypatch, _cr_html())
    assert FM._resolve_one(config, SESSION, _args(), {}, explicit=True)
    event = json.loads(config.raw_event(SESSION).read_text())
    assert event["hlsUrl"] == HLS
    assert event["recordingId"] == "hemi_20261112144538"
    assert event["recordingStart"] == "2026-11-12T14:45:38+01:00"
    assert event["crPage"] == C.cr_page_url(SESSION)
    assert event["seanceRef"] == "RUANR5L17S2027IDS30999"
    assert event["offsetMismatches"] == 0


def test_resolve_skips_compte_rendu_without_offsets(tmp_path, monkeypatch):
    config = _config(tmp_path, _provisional(_cr_xml()))
    calls = _patch_network(monkeypatch, _cr_html())
    assert not FM._resolve_one(config, SESSION, _args(), {}, explicit=True)
    assert calls == {"get": 0, "status": 0}
    assert not config.raw_event(SESSION).exists()


def test_resolve_records_offline_recording(tmp_path, monkeypatch):
    config = _config(tmp_path, _cr_xml())
    _patch_network(monkeypatch, _cr_html(), status=404)
    unavailable: dict = {}
    assert not FM._resolve_one(config, SESSION, _args(), unavailable, explicit=True)
    assert not config.raw_event(SESSION).exists()
    saved = json.loads((config.dir("media") / "_unavailable.json").read_text())
    assert saved[SESSION]["hlsUrl"] == HLS
    # The next run makes no request for it.
    calls = _patch_network(monkeypatch, _cr_html())
    assert not FM._resolve_one(config, SESSION, _args(), saved, explicit=True)
    assert calls == {"get": 0, "status": 0}


def test_resolve_skips_multiple_recordings(tmp_path, monkeypatch):
    config = _config(tmp_path, _cr_xml())
    # The last intervention points at a second recording.
    page = _cr_html()
    i = page.rfind("hemi_20261112144538")
    page = page[:i] + "hemi_20261112170000" + page[i + len("hemi_20261112144538"):]
    calls = _patch_network(monkeypatch, page)
    assert not FM._resolve_one(config, SESSION, _args(), {}, explicit=True)
    assert calls["status"] == 0
    assert not config.raw_event(SESSION).exists()


def test_bulk_mode_skips_sittings_past_retention(tmp_path, monkeypatch):
    old = _cr_xml().replace("20261112150000000", "20200101150000000")
    config = _config(tmp_path, old)
    calls = _patch_network(monkeypatch, _cr_html())
    assert not FM._resolve_one(config, SESSION, _args(), {}, explicit=False)
    assert calls == {"get": 0, "status": 0}


# ---- merger ----

def _merged(tmp_path, event_overrides: dict | None = None) -> dict:
    config = _config(tmp_path, _cr_xml())
    proc = proceedings2json.parse_cr(_cr_xml().encode(), SESSION, {}, None)
    config.file(SESSION, "proceedings", create=True).write_text(json.dumps(proc))
    event = {"session": SESSION, "seanceRef": "RU", "hlsUrl": HLS,
             "recordingId": C.recording_id(HLS),
             "recordingStart": C.recording_start(HLS).isoformat(),
             "crPage": C.cr_page_url(SESSION), "offsetMismatches": 0}
    event.update(event_overrides or {})
    config.raw_event(SESSION).parent.mkdir(parents=True, exist_ok=True)
    config.raw_event(SESSION).write_text(json.dumps(event))
    media = media2json.parse_media_for_session(config, SESSION)
    config.file(SESSION, "media", create=True).write_text(json.dumps(media))
    out = MS.merge_session(SESSION, config, SimpleNamespace(period=17))
    return json.loads(out.read_text())


def test_merge_clock_times_from_recording_start(tmp_path):
    doc = _merged(tmp_path)
    speeches = doc["data"]
    assert [s["dateStart"] for s in speeches] == [
        "2026-11-12T15:00:15.790000+01:00",   # 14:45:38 + 877.79 s: the opening
        "2026-11-12T15:00:38+01:00",
        "2026-11-12T15:02:18+01:00",
    ]
    assert speeches[0]["dateEnd"] == speeches[1]["dateStart"]
    # Last speech: median-based duration fallback (median of 22.21 and 100).
    assert speeches[-1]["media"]["duration"] == pytest.approx(61.1, abs=0.01)
    assert speeches[-1]["dateEnd"] == "2026-11-12T15:03:19.105000+01:00"
    assert speeches[0]["session"]["dateStart"] == speeches[0]["dateStart"]
    assert speeches[0]["session"]["dateEnd"] == speeches[-1]["dateEnd"]
    assert doc["meta"]["dateStart"] == speeches[0]["dateStart"]


def test_merge_media_block(tmp_path):
    speeches = _merged(tmp_path)["data"]
    page = C.cr_page_url(SESSION)
    assert [s["media"]["sourcePage"] for s in speeches] == [
        f"{page}#4100001", f"{page}#4100004", f"{page}#4100006"]
    m = speeches[1]["media"]
    assert m["videoFileURI"] == f"{HLS}#t=900.00,1000.00"
    assert m["originMediaID"] == "hemi_20261112144538"
    assert m["additionalInformation"] == {"startOffset": 900.0}
    assert m["license"] == "https://www.assemblee-nationale.fr/dyn/info-site"


def test_merge_without_recording_start_anchors_scheduled_opening(tmp_path):
    doc = _merged(tmp_path, {"hlsUrl": "https://example.org/seance/master.m3u8",
                             "recordingId": None, "recordingStart": None})
    starts = [s["dateStart"] for s in doc["data"]]
    assert starts[0] == "2026-11-12T15:00:00+01:00"
    assert starts[1] == "2026-11-12T15:00:22.210000+01:00"


# ---- proceedings refresh ----

def _zip_bytes(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def test_bulk_download_rewrites_only_changed_files(tmp_path, monkeypatch):
    config = C.Config(tmp_path)
    same, changed, fresh = "2027O1N001", "2027O1N002", "2027O1N003"
    for key, body in ((same, b"<a/>"), (changed, b"<old/>")):
        p = config.raw_cr(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(body)
        os.utime(p, (1_000_000, 1_000_000))
    blob = _zip_bytes({
        f"xml/compteRendu/CRSANR5L17S{same}.xml": b"<a/>",
        f"xml/compteRendu/CRSANR5L17S{changed}.xml": b"<new/>",
        f"xml/compteRendu/CRSANR5L17S{fresh}.xml": b"<b/>",
    })
    monkeypatch.setattr(FP, "http_get", lambda url, **kw: blob)
    counts = FP._download_bulk(config, _args())
    assert (counts["new"], counts["updated"], counts["unchanged"]) == (1, 1, 1)
    assert config.raw_cr(same).stat().st_mtime == 1_000_000      # untouched
    assert config.raw_cr(changed).read_bytes() == b"<new/>"
    assert config.raw_cr(fresh).read_bytes() == b"<b/>"


def test_targeted_download_refetches_and_compares(tmp_path, monkeypatch):
    config = _config(tmp_path, _cr_xml())
    os.utime(config.raw_cr(SESSION), (1_000_000, 1_000_000))
    monkeypatch.setattr(FP, "http_get", lambda url, **kw: _cr_xml().encode())
    assert FP._fetch_one(config, SESSION, _args()) == "unchanged"
    assert config.raw_cr(SESSION).stat().st_mtime == 1_000_000
    monkeypatch.setattr(FP, "http_get", lambda url, **kw: _provisional(_cr_xml()).encode())
    assert FP._fetch_one(config, SESSION, _args()) == "updated"


# ---- workflow scope ----

def test_session_in_scope_requires_resolved_media(tmp_path):
    config = _config(tmp_path, _cr_xml())
    args = SimpleNamespace(limit_session="", data_dir=tmp_path)
    assert not W._session_in_scope(args, SESSION)
    config.file(SESSION, "media", create=True).write_text("{}")
    assert W._session_in_scope(args, SESSION)
    assert not W._session_in_scope(SimpleNamespace(limit_session="2026", data_dir=tmp_path),
                                   SESSION)
    assert not W._session_in_scope(args, "not-a-key")
