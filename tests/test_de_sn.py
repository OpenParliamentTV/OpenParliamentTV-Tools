"""DE-SN tests: list-page parsing, Wahlperiode from the date (the constituent
sitting the list files under the previous WP), Europe/Berlin offsets, and the
end-to-end Stage 2 shape.

No network: list pages are generated in the mediathek's markup and served by a
fake ``fetch_text``. The DE-SN package directory has a hyphen, so it is imported
via ``importlib``.
"""

from __future__ import annotations

import importlib
import json
import re

import pytest

from optv.shared.validators import validate_stage2

_scraper = importlib.import_module("optv.parliaments.DE-SN.scraper.common")
_fetch = importlib.import_module("optv.parliaments.DE-SN.scraper.fetch_media")
_parser = importlib.import_module("optv.parliaments.DE-SN.parsers.media2json")
_merger = importlib.import_module("optv.parliaments.DE-SN.merger.merge_session")
_proceedings = importlib.import_module("optv.parliaments.DE-SN.scraper.fetch_proceedings")
Config = importlib.import_module("optv.parliaments.DE-SN.common").Config


# --------------------------------------------------------------------------- #
# Synthetic list pages (the markup parse_list_page reads)
# --------------------------------------------------------------------------- #

def _article(*, sitzung, date, time, start, end=None, speaker="Dirk Panter",
             faction="SPD", speech_type="Debatte", top="2", sitzung_page=2363):
    d, m, y = date[8:10], date[5:7], date[0:4]
    if faction:
        title = (f'{speaker} <span class="badge badge-party-5">{faction}</span>'
                 f' - {speech_type}')
    else:
        title = f"{speaker} - {speech_type}" if speech_type else speaker
    sid = f"{y}{m}{d}{start - 10}"
    opts = {"startPosition": start, "endPosition": end if end is not None else start + 60}
    return f"""
<article class="xm_teaser xm_teaser--compact">
  <h2 class="xm_teaser__title">{title}</h2>
  <div class="xm_teaser__subtitle">TOP {top} · Sitzungsnummer {sitzung} ·
    Sitzung des Plenums vom {d}.{m}.{y} {time} UHR</div>
  <div class="xm_teaser__text"><p>Thema zu TOP {top}</p></div>
  <a href="/de/aktuelles/sitzungskalender/sitzung/{sitzung_page}">Tagesordnung</a>
  <div id="modal-content-{sid}">
    <video data-component-options='{json.dumps(opts)}'>
      <source src="https://stream-o01.envia-tel.net/vod/smil:{y}{m}{d}.smil/playlist.m3u8" type="application/x-mpegURL" />
    </video>
  </div>
</article>"""


def _page(articles, *, wp=8, last_start=None):
    pager = (f'<a href="?electoral_term_id={wp}&amp;start={last_start}">Letzte</a>'
             if last_start is not None else "")
    return "<html><body>" + "".join(articles) + pager + "</body></html>"


# --------------------------------------------------------------------------- #
# Scraper helpers
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("date, listed, expected", [
    (None, 8, 8),
    ("2019-10-01", 7, 7),
    ("2024-09-30", 7, 7),
    ("2024-10-01", 7, 8),        # constituent sitting filed under WP 7
    ("2026-02-05", 8, 8),
    ("2031-01-01", 9, 9),        # WP missing from the table keeps the filter's WP
])
def test_derive_wp(date, listed, expected):
    assert _scraper.derive_wp(date, listed) == expected


def test_split_speaker_type():
    assert _scraper.split_speaker_type("Unbekannter Redner - Standard") == \
        ("Unbekannter Redner", "Standard")
    assert _scraper.split_speaker_type("Jens Hentschel-Thöricht") == \
        ("Jens Hentschel-Thöricht", "")
    assert _scraper.split_speaker_type("ENDE der Sitzung - Debatte") == \
        ("ENDE der Sitzung", "Debatte")


def test_last_page_start():
    assert _scraper.last_page_start(_page([], wp=7, last_start=11080)) == 11080
    assert _scraper.last_page_start("<html></html>") is None


def test_parse_list_page_fields_and_badgeless_titles():
    html = _page([
        _article(sitzung=1, date="2024-10-01", time="12:39:43", start=6256, end=6412,
                 speaker="Jens Hentschel-Thöricht", faction="BSW", speech_type="Debatte"),
        _article(sitzung=1, date="2024-10-01", time="12:59:18", start=7431,
                 speaker="Unbekannter Redner", faction="", speech_type="Standard"),
    ])
    a, b = _scraper.parse_list_page(html, wp=7)
    assert a["wp"] == 8                                  # derived from the date
    assert a["id"] == "202410016246"                     # date + (start − 10)
    assert (a["speaker_raw"], a["faction_raw"], a["speech_type"]) == \
        ("Jens Hentschel-Thöricht", "BSW", "Debatte")
    assert (a["start_offset"], a["end_offset"]) == (6256, 6412)
    assert a["smil_url"].endswith("smil:20241001.smil/playlist.m3u8")
    assert (b["speaker_raw"], b["faction_raw"], b["speech_type"]) == \
        ("Unbekannter Redner", "", "Standard")


# --------------------------------------------------------------------------- #
# fetch_media: constituent sitting from the previous WP's list tail
# --------------------------------------------------------------------------- #

def _serve(monkeypatch, pages):
    """Serve ``pages[(wp, start)]``; unknown offsets return an empty page."""
    calls = []

    def fake_fetch_text(url, **kw):
        wp = int(re.search(r"electoral_term_id=(\d+)", url).group(1))
        start = int(re.search(r"start=(\d+)", url).group(1))
        calls.append((wp, start))
        return pages.get((wp, start), _page([]))

    monkeypatch.setattr(_fetch, "fetch_text", fake_fetch_text)
    return calls


def _lists():
    wp8 = [_article(sitzung=2, date="2024-10-25", time="11:52:07", start=6857 + i * 30)
           for i in range(2)]
    wp7_s92 = [_article(sitzung=92, date="2024-09-26", time="10:11:00", start=7860)]
    wp7_s1_2019 = [_article(sitzung=1, date="2019-10-01", time="11:44:24", start=13414,
                            sitzung_page=1419)]
    wp7_s1_2024 = [_article(sitzung=1, date="2024-10-01", time="11:30:31", start=2104 + i * 400)
                   for i in range(3)]
    return {
        (8, 0): _page(wp8, wp=8, last_start=0),
        (7, 0): _page(wp7_s92, wp=7, last_start=40),
        # sitting 1 sorts last: WP 7's own sitting 1, then WP 8's constituent one
        (7, 20): _page(wp7_s1_2019 + wp7_s1_2024[:1], wp=7, last_start=40),
        (7, 40): _page(wp7_s1_2024[1:], wp=7, last_start=40),
    }


def test_fetch_media_picks_up_constituent_sitting(tmp_path, monkeypatch):
    calls = _serve(monkeypatch, _lists())
    written = _fetch.fetch_media(media_dir=tmp_path, period=8)
    assert written == ["08001", "08002"]
    raw = json.loads((tmp_path / "08001-raw.json").read_text())
    assert raw["wp"] == 8 and raw["sitzung"] == 1
    assert raw["dates"] == ["2024-10-01"] and len(raw["speeches"]) == 3
    # tail walk: page 0 for the pager, then backwards from the last page until
    # a page reaches back before 2024-10-01
    assert calls[:3] == [(7, 0), (7, 40), (7, 20)]


def test_fetch_media_previous_wp_excludes_constituent_sitting(tmp_path, monkeypatch):
    _serve(monkeypatch, _lists())
    written = _fetch.fetch_media(media_dir=tmp_path, period=7)
    assert written == ["07001", "07092"]
    raw = json.loads((tmp_path / "07001-raw.json").read_text())
    assert raw["dates"] == ["2019-10-01"] and len(raw["speeches"]) == 1


# --------------------------------------------------------------------------- #
# media2json: Europe/Berlin offsets, placeholder rows, unknown speakers
# --------------------------------------------------------------------------- #

def test_local_iso_is_dst_aware():
    assert _parser.local_iso("2026-02-05", "10:04:09") == "2026-02-05T10:04:09+01:00"
    assert _parser.local_iso("2026-09-17", "09:04:07") == "2026-09-17T09:04:07+02:00"


def _raw_manifest(tmp_path, speeches, sid="08025"):
    path = tmp_path / f"{sid}-raw.json"
    path.write_text(json.dumps({"session_id": sid, "wp": 8, "sitzung": int(sid[2:]),
                                "date": "2026-02-05", "speeches": speeches}))
    return path


def _raw(id_, time, start, end, speaker, faction="", speech_type="", top="1"):
    return {"id": id_, "date": "2026-02-05", "time": time, "top_no": top,
            "thema": f"Thema {top}", "speaker_raw": speaker, "faction_raw": faction,
            "speech_type": speech_type, "start_offset": start, "end_offset": end,
            "smil_url": "https://stream-o01.envia-tel.net/vod/smil:20260205.smil/playlist.m3u8",
            "source_page": f"https://example.org/videoeinzelbeitrag/{id_}"}


def test_parse_session_offsets_markers_and_unknown_speakers(tmp_path):
    path = _raw_manifest(tmp_path, [
        _raw("202602051129", "10:04:09", 1139, 1440, "Nam Duy Nguyen", "Die Linke",
             "Aktuelle Debatte"),
        # raw manifests written before the scraper split badge-less titles
        _raw("202602051400", "10:08:30", 1410, 1500, "Unbekannter Redner - Zwischenfrage"),
        _raw("202602051500", "10:10:00", 1510, 1600, "Unbekannter Redner - Auszählung der Stimmen"),
        _raw("202602051600", "10:12:00", 1610, 1700, "Unbekannter Redner", "", "Standard"),
    ])
    doc = _parser.parse_session(path)
    data = doc["data"]
    assert len(data) == 3                                # vote count dropped
    nguyen, zwischen, unknown = data
    assert nguyen["start_datetime"] == "2026-02-05T10:04:09+01:00"
    # end clamped to the next speech's start (source end overruns it)
    assert nguyen["end_offset"] == 1410
    assert nguyen["end_datetime"] == "2026-02-05T10:08:40+01:00"
    assert (zwischen["label"], zwischen["speech_type"], zwischen["context"]) == \
        ("Unbekannter Redner", "Zwischenfrage", "speaker")
    assert (zwischen["firstname"], zwischen["lastname"]) == ("", "")
    assert (unknown["label"], unknown["speech_type"]) == ("Unbekannter Redner", "Standard")


def test_merge_session_stage2_with_offsets(tmp_path):
    config = Config(tmp_path)
    media_dir = config.dir("media", create=True)
    path = _raw_manifest(media_dir, [
        _raw("202602051129", "10:04:09", 1139, 1440, "Nam Duy Nguyen", "Die Linke",
             "Aktuelle Debatte"),
        _raw("202602051400", "10:08:30", 1410, 1500, "Armin Schuster", "Staatsregierung",
             "Debatte", top="2"),
    ])
    media_doc = _parser.parse_session(path)
    config.file("08025", "media", create=True).write_text(json.dumps(media_doc))

    doc = json.loads(_merger.merge_session("08025", config, None).read_text())
    first, second = doc["data"]
    assert doc["meta"]["dateStart"] == "2026-02-05T10:04:09+01:00"
    assert first["session"]["dateEnd"] == "2026-02-05T10:10:00+01:00"
    assert first["dateStart"] == "2026-02-05T10:04:09+01:00"
    assert first["electoralPeriod"]["number"] == 8 and first["session"]["number"] == 25
    assert second["people"][0]["role"] == "Staatsregierung"
    errors = [f for f in validate_stage2(doc, schema="full", semantic=True)
              if f["severity"] == "error"]
    assert errors == []


def test_fetch_proceedings_takes_sessions_from_raw_manifests(tmp_path, monkeypatch):
    # the -media.json files only follow in the parse stage; the protocol of a
    # sitting fetched in this download run must not wait for the next run
    config = Config(tmp_path)
    media_dir = config.dir("media", create=True)
    _raw_manifest(media_dir, [], sid="08001")
    asked = []
    monkeypatch.setattr(_proceedings, "url_for",
                        lambda sid, date: asked.append(sid) or None)
    _proceedings.fetch_proceedings(config, None)
    assert _proceedings.manifest_sessions(config) == [("08001", "2026-02-05")]
    assert asked == ["08001"]
