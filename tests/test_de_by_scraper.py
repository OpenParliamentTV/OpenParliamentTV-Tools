"""DE-BY tests: Plenum Online parsing, incremental session index, Stage 2 shape.

No network: the JSF parsers run against fixtures trimmed from real
``plon2-webanzeige`` responses (sitting 87 with ``meta_vod`` playlists and two
urgent-motion sub-items, sitting 90 with the ``streaming?topId=`` endpoint),
the index builder runs against a fake JSF session, and the parser + merger
consume a tiny synthetic manifest. The DE-BY package directory has a hyphen, so
it is imported via ``importlib``.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace

from optv.shared.validators import validate_stage2

_common = importlib.import_module("optv.parliaments.DE-BY.scraper.common")
_archive = importlib.import_module("optv.parliaments.DE-BY.scraper.fetch_archive")
_media = importlib.import_module("optv.parliaments.DE-BY.parsers.media2json")
_merger = importlib.import_module("optv.parliaments.DE-BY.merger.merge_session")
Config = importlib.import_module("optv.parliaments.DE-BY.common").Config

FIXTURES = Path(__file__).parent / "fixtures" / "DE-BY"
METAFILE = "https://www.bayern.landtag.de/streamingservice/jsonmetafiles/wp19/19_659/meta_vod_{}.json"
STREAMING = "https://www.bayern.landtag.de/plon2-webanzeige/streaming?topId={}"
PLAYER = "https://www.bayern.landtag.de/plon2-webanzeige/views/ondemand/ondemand-playlist-param.html"


# --------------------------------------------------------------------------- #
# Scraper: session heading, TOP headers, playlists + speech rows
# --------------------------------------------------------------------------- #

def test_sitzungsnr_from_heading_not_agenda_link():
    html = (FIXTURES / "session_s87.xml").read_text()
    # 23.07.2026 is day 3 of a plenary week: the link names the week's first day.
    assert "sitzungsnr=85" in html
    assert _common.parse_sitzungsnr(html) == 87
    assert _common.parse_sitzungsnr("<p>no heading here</p>") is None


def test_tab_headers():
    html = (FIXTURES / "session_s87.xml").read_text()
    tabs = _common.parse_tab_headers(html)
    assert [t["index"] for t in tabs] == [0, 1]
    assert tabs[1]["title"].startswith("TOP 36 Beratung der zum Plenum eingereichten")
    assert tabs[1]["component_id"].startswith("accordion:1:")


def test_playlist_id():
    assert _common.playlist_id(METAFILE.format(53240)) == "53240"
    assert _common.playlist_id(STREAMING.format(53471)) == "53471"
    assert _common.playlist_id("https://example.org/other.json") is None


def test_metafile_panel_one_playlist_per_motion():
    pls = _common.parse_tab_playlists((FIXTURES / "panel_metafile_s87.xml").read_text())
    assert [p["tid"] for p in pls] == ["53240", "53241"]
    assert [p["url"] for p in pls] == [METAFILE.format(53240), METAFILE.format(53241)]
    assert all(p["player_url"] == PLAYER for p in pls)

    rows = pls[0]["rows"]
    assert [r["start_id"] for r in rows] == [1, 13]
    # Motion authors are linked in the sub-item header; the row keeps its own speaker.
    assert rows[0] == {"start_id": 1, "label": "Richard Graupner (AfD)",
                       "profile_id": "5240", "aktion_id": "73242_0_0"}

    minister = pls[1]["rows"][-1]
    assert minister == {"start_id": 10, "label": "Staatsministerin Judith Gerlach",
                        "profile_id": None, "aktion_id": "73311_0_0"}


def test_database_panel_skips_whole_top_stream():
    html = (FIXTURES / "panel_database_s90.xml").read_text()
    assert "gesamt=true" in html
    pls = _common.parse_tab_playlists(html)
    assert [p["url"] for p in pls] == [STREAMING.format(53471)]
    assert [(r["start_id"], r["profile_id"], r["aktion_id"]) for r in pls[0]["rows"]] == [
        (1, "5194", "73785_0_0"), (2, "5339", "73786_0_0")]


# --------------------------------------------------------------------------- #
# Session index: incremental + collision guard
# --------------------------------------------------------------------------- #

class _FakeSession:
    """Dropdown + per-gremium heading, counting the valueChange loads."""

    def __init__(self, options, numbers):
        self.options = options
        self.numbers = numbers
        self.loaded = []

    def start(self):
        return "".join(f'<option value="{g}" data-escape="true">{d}</option>'
                       for g, d in self.options)

    def load_session(self, gid):
        self.loaded.append(gid)
        return f"<h2>Sitzungsablauf für die {self.numbers[gid]}. Plenarsitzung</h2>"


def _entry(sid, iso, gid):
    return {"session_id": sid, "date": iso, "gremium_id": gid,
            "sitzungsnr": int(sid[2:]), "tops": []}


def test_archive_reloads_only_new_and_colliding(tmp_path, monkeypatch):
    meta = tmp_path / "metadata"
    meta.mkdir()
    # Cache as written with the link-derived numbers: a three-day week on 85.
    cached = [_entry("19084", "2026-07-15", 657),
              _entry("19085", "2026-07-21", 657 + 1),
              _entry("19085", "2026-07-22", 657 + 2),
              _entry("19085", "2026-07-23", 657 + 3)]
    (meta / "archive-wp19.json").write_text(json.dumps({"wp": 19, "sessions": cached}))
    options = [(661, "30.09.2026"), (660, "23.07.2026"), (659, "22.07.2026"),
               (658, "21.07.2026"), (657, "15.07.2026"), (1, "01.01.2020")]
    fake = _FakeSession(options, {658: 85, 659: 86, 660: 87, 661: 88})
    monkeypatch.setattr(_archive, "PlonSession", lambda **kw: fake)

    archive = _archive.fetch_archive(period=19, media_dir=tmp_path / "media", metadata_dir=meta)

    assert sorted(fake.loaded) == [658, 659, 660, 661]  # 657 kept from cache, 2020 out of range
    assert [(s["session_id"], s["date"]) for s in archive["sessions"]] == [
        ("19084", "2026-07-15"), ("19085", "2026-07-21"), ("19086", "2026-07-22"),
        ("19087", "2026-07-23"), ("19088", "2026-09-30")]

    # A second run loads nothing; --force scoped by the session filter loads one.
    fake.loaded.clear()
    _archive.fetch_archive(period=19, media_dir=tmp_path / "media", metadata_dir=meta)
    assert fake.loaded == []
    _archive.fetch_archive(period=19, media_dir=tmp_path / "media", metadata_dir=meta,
                           force=True, session_filter=r"^19087$")
    assert fake.loaded == [660]


def test_archive_takes_gremium_id_from_live_dropdown(tmp_path, monkeypatch):
    """The dropdown ids were renumbered (2025-07-02: 614 → 625); the cache keys on the date."""
    meta = tmp_path / "metadata"
    meta.mkdir()
    (meta / "archive-wp19.json").write_text(json.dumps(
        {"wp": 19, "sessions": [_entry("19053", "2025-06-24", 613), _entry("19054", "2025-07-02", 614)]}))
    fake = _FakeSession([(625, "02.07.2025"), (624, "24.06.2025"), (614, "19.03.2025")],
                        {614: 45})
    monkeypatch.setattr(_archive, "PlonSession", lambda **kw: fake)

    archive = _archive.fetch_archive(period=19, media_dir=tmp_path / "media", metadata_dir=meta)

    assert fake.loaded == [614]  # only the date not yet cached
    assert [(s["session_id"], s["date"], s["gremium_id"]) for s in archive["sessions"]] == [
        ("19045", "2025-03-19", 614), ("19053", "2025-06-24", 624), ("19054", "2025-07-02", 625)]


def test_archive_drops_unresolved_collision(tmp_path, monkeypatch):
    fake = _FakeSession([(2, "02.07.2025"), (1, "01.07.2025")], {1: 54, 2: 54})
    monkeypatch.setattr(_archive, "PlonSession", lambda **kw: fake)
    archive = _archive.fetch_archive(period=19, media_dir=tmp_path / "media",
                                     metadata_dir=tmp_path / "metadata")
    assert archive["sessions"] == []


# --------------------------------------------------------------------------- #
# media2json + merger
# --------------------------------------------------------------------------- #

def test_hls_datetime_is_utc_converted_to_berlin():
    assert _media._hls_datetime("…/vcd/20260210130002_1_,1,2,3,4,_00001.mp4.csmil/master.m3u8")[:2] == (
        "2026-02-10T14:00:02+01:00", "14:00:02")
    assert _media._hls_datetime("…/vcd/20261020120101_1_,1,2,3,4,_00001.mp4.csmil/master.m3u8") == (
        "2026-10-20T14:01:01+02:00", "14:01:01", "2026-10-20T12:01:01+00:00")
    assert _media._hls_datetime("no-timestamp") == (None, None, None)


def test_source_page_matches_site_player_link():
    assert _media.source_page(PLAYER, STREAMING.format(53271), 1) == (
        PLAYER + "?playlist=https%3A%2F%2Fwww.bayern.landtag.de%2Fplon2-webanzeige"
        "%2Fstreaming%3FtopId%3D53271&startId=1")


def _hls(ts):
    return f"https://cldf-wzw-od.r53.cdn.tv1.eu/5112ltbyod/i/ondem/vcd/{ts}_1_,1,2,3,4,_00001.mp4.csmil/master.m3u8"


def _write_session(media_dir: Path):
    """19087: TOP 1 with one playlist, TOP 36 with two (urgent motions)."""
    raw = media_dir / "19087"
    raw.mkdir(parents=True)
    playlists = {
        "100": {"title": "Aktuelle Stunde", "items": [
            {"id": 1, "title": "Katharina Schulze (BÜNDNIS 90/DIE GRÜNEN)", "hls": _hls("20260723070105")},
            {"id": 2, "title": "Staatsminister Albert Füracker", "hls": _hls("20260723071010")}]},
        "53240": {"title": "Dringlichkeitsantrag Messer, Schüsse, tödliche Gewalt", "items": [
            {"id": 1, "title": "Richard Graupner (AfD)", "hls": _hls("20260723080000")}]},
        "53241": {"title": "Dringlichkeitsantrag Pflege stärken", "items": [
            {"id": 1, "title": "Anna Rasehorn (SPD)", "hls": _hls("20260723090000")}]},
    }
    for tid, body in playlists.items():
        (raw / f"playlist_{tid}.json").write_text(json.dumps(body, ensure_ascii=False))

    def pl(tid, url, rows):
        return {"tid": tid, "url": url, "player_url": PLAYER, "file": f"playlist_{tid}.json",
                "rows": rows}

    manifest = {
        "format": 2, "session_id": "19087", "date": "2026-07-23", "sitzungsnr": 87,
        "gremium_id": 660,
        "tops": [
            {"index": 0, "title": "Eröffnung 09:00 Uhr", "playlists": []},
            {"index": 1, "title": "TOP 1 Aktuelle Stunde", "playlists": [
                pl("100", STREAMING.format(100), [
                    {"start_id": 1, "label": "Katharina Schulze (BÜNDNIS 90/DIE GRÜNEN)",
                     "profile_id": "5194", "aktion_id": "73785_0_0"},
                    {"start_id": 2, "label": "Staatsminister Albert Füracker",
                     "profile_id": None, "aktion_id": "73786_0_0"}])]},
            {"index": 2, "title": "TOP 36 Beratung der zum Plenum eingereichten Dringlichkeitsanträge",
             "playlists": [
                 pl("53240", METAFILE.format(53240), [
                     {"start_id": 1, "label": "Richard Graupner (AfD)",
                      "profile_id": "5240", "aktion_id": "73242_0_0"}]),
                 pl("53241", METAFILE.format(53241), [
                     {"start_id": 1, "label": "Anna Rasehorn (SPD)",
                      "profile_id": "5356", "aktion_id": "73250_0_0"}])]},
        ],
    }
    (media_dir / "19087-tops.json").write_text(json.dumps(manifest, ensure_ascii=False))


def test_old_manifest_format_is_not_parsed(tmp_path):
    old = {"session_id": "19054", "date": "2025-07-02", "sitzungsnr": 54, "gremium_id": 614,
           "tops": [{"index": 1, "title": "TOP 1", "playlist": "meta_vod_50234.json"}]}
    path = tmp_path / "19054-tops.json"
    path.write_text(json.dumps(old))
    assert _media.parse_session(path) is None


def test_parse_and_merge_end_to_end(tmp_path):
    config = Config(tmp_path)
    media_dir = config.dir("media", create=True)
    _write_session(media_dir)
    _media.parse_media_directory(media_dir)

    out = _merger.merge_session("19087", config, None)
    doc = json.loads(out.read_text())
    data = doc["data"]
    assert [sp["originID"] for sp in data] == ["100_1", "100_2", "53240_1", "53241_1"]
    assert [sp["speechIndex"] for sp in data] == [1, 2, 3, 4]
    assert data[0]["session"]["number"] == 87
    assert data[0]["dateStart"] == "2026-07-23T09:01:05+02:00"
    assert data[0]["session"]["dateStart"] == "2026-07-23T09:01:05+02:00"
    assert data[-1]["session"]["dateEnd"] == "2026-07-23T11:00:00+02:00"

    # Single-playlist TOP keeps TOP-{n}; the urgent-motions TOP splits per motion.
    assert data[0]["agendaItem"]["id"] == "TOP-1"
    motion = data[2]["agendaItem"]
    assert motion["id"] == "TOP-2-1"
    assert motion["title"] == "Dringlichkeitsantrag Messer, Schüsse, tödliche Gewalt"
    assert motion["officialTitle"] == ("TOP 36 Beratung der zum Plenum eingereichten "
                                       "Dringlichkeitsanträge: Dringlichkeitsantrag Messer, "
                                       "Schüsse, tödliche Gewalt")
    assert data[3]["agendaItem"]["id"] == "TOP-2-2"
    assert motion["type"] == data[3]["agendaItem"]["type"]

    # Member id from the biography link; none for the state minister.
    assert data[0]["people"][0]["originPersonID"] == "5194"
    assert "originPersonID" not in data[1]["people"][0]
    assert data[1]["people"][0]["role"] == "Staatsminister"

    media = data[0]["media"]
    assert media["sourcePage"] == _media.source_page(PLAYER, STREAMING.format(100), 1)
    assert media["additionalInformation"]["aktionId"] == "73785_0_0"
    assert media["additionalInformation"]["clockStart"] == "09:01:05"
    assert data[0]["debug"]["timesAreVideoRelative"] is False

    findings = validate_stage2(doc, schema="full", semantic=True)
    assert [f for f in findings if f["severity"] == "error"] == []


# --------------------------------------------------------------------------- #
# Proceedings fetch: same run as the media fetch
# --------------------------------------------------------------------------- #

def test_protocol_fetched_from_manifest_before_media_file(tmp_path, monkeypatch):
    """The TOP manifest exists right after the media fetch; the -media.json only
    after the parse stage. The protocol URL is built from the manifest."""
    fetch = importlib.import_module("optv.shared.pdf2tei.fetch")
    proceedings = importlib.import_module("optv.parliaments.DE-BY.scraper.fetch_proceedings")
    config = Config(tmp_path)
    media_dir = config.dir("media", create=True)
    (media_dir / "19087-tops.json").write_text(json.dumps(
        {"format": 2, "session_id": "19087", "date": "2026-07-23", "sitzungsnr": 87, "tops": []}))
    calls = []
    monkeypatch.setattr(fetch, "download_pdf", lambda url, dest, **kw: calls.append((url, dest.name)))

    proceedings.fetch_proceedings(config, SimpleNamespace(force=False, retry_count=1,
                                                          limit_session=None))

    assert calls == [(proceedings._BASE + "?date=230726&sitznr=087&wp=19", "19087.pdf")]
