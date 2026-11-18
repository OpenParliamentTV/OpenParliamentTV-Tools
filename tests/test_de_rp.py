"""DE-RP tests: OPAL row parsing (suffixed clips, electoral period from the
sitting header, WP 19 row shapes), raw-file merging, video fingerprints, the
media parser's faction/office routing and replacement flag, the headless
OPAL helper and the merger's role precedence.

No network and no browser: the HTML is a static fixture, HEAD requests and
the page fetch are stubbed. The DE-RP package directory has a hyphen, so it
is imported via ``importlib``.
"""

from __future__ import annotations

from datetime import date
import importlib
import json
import logging
from pathlib import Path
from types import SimpleNamespace

from lxml import html as lxml_html

_fetch = importlib.import_module("optv.parliaments.DE-RP.scraper.fetch_media")
_opal = importlib.import_module("optv.parliaments.DE-RP.scraper.fetch_opal")
_media = importlib.import_module("optv.parliaments.DE-RP.parsers.media2json")
_common = importlib.import_module("optv.parliaments.DE-RP.parsers.common")
_merger = importlib.import_module("optv.parliaments.DE-RP.merger.merge_session")

FIXTURE = Path(__file__).parent / "fixtures" / "DE-RP" / "opal-sample.html"
VIDEOS = "https://dokumente.landtag.rlp.de/landtag/opal-videos/"


def _rows():
    return {r["origin_media_id"]: r for r in _fetch.parse_html_file(FIXTURE)}


def test_rows_keep_suffixed_clips_and_take_period_from_header():
    rows = _rows()
    assert set(rows) == {
        "18-81-Sit6", "18-81-Sit6a", "18-81-Sit6b", "18-81-Sit6c", "18-81-Sit7",
        "18-81-Sit8", "18-81-Sit9a", "18-81-Sit71a", "18-81-Sit71b",
        "18-1-Sit1", "19-1-Sit1",
        "19-7-Sit10", "19-7-Sit19a", "19-7-Sit20", "19-7-Sit22", "19-7-Sit29b",
        "19-7-Sit56", "19-7-Sit69a",
    }
    assert rows["18-81-Sit6b"]["speech_suffix"] == "b"
    assert rows["18-81-Sit6b"]["speech_index_in_session"] == 6
    assert rows["19-7-Sit20"]["session_period"] == "19"
    # One file name, two electoral periods: two rows, same URL.
    assert rows["18-1-Sit1"]["video_url"] == rows["19-1-Sit1"]["video_url"]
    assert rows["18-1-Sit1"]["speaker_label"] == "Hendrik Hering"
    assert rows["19-1-Sit1"]["speaker_label"] == "Matthias Lammert"


def test_rows_skip_headerless_mismatched_foreign_and_unparseable(caplog):
    with caplog.at_level(logging.WARNING):
        rows = _rows()
    assert not any(r["video_url"].endswith(("99-Sit1.mp4", "8-Sit1.mp4", "7-Sit91.mp4"))
                   for r in rows.values())
    assert not any("example.org" in r["video_url"] for r in rows.values())
    text = caplog.text
    assert "before any 'Plenarsitzung' header" in text
    assert "listed under 'Plenarsitzung 19/7'" in text
    assert "non-OPAL video URL" in text
    assert "unparseable row 19-7-Sit91" in text


def test_row_shapes():
    rows = _rows()
    r = rows["19-7-Sit69a"]
    assert (r["faction"], r["function"], r["page_range"]) == (
        "Landtagsvizepräsidentin", "Bekanntgabe des Abstimmungsergebnisses", "154")
    r = rows["19-7-Sit29b"]
    assert (r["function"], r["page_range"]) == ("Bekanntgabe des Abstimmungsergebnisses", "")
    r = rows["19-7-Sit20"]
    assert (r["faction"], r["function"], r["page_range"]) == (
        "SPD", "Bekanntgabe der Ausschussüberweisung", "75")
    r = rows["19-7-Sit56"]
    assert (r["speaker_firstname"], r["speaker_lastname"]) == ("Roland", "Weimer")
    assert rows["19-7-Sit19a"]["function"] == "Kurzintervention"
    assert rows["19-7-Sit19a"]["page_range"] == "52-53"
    assert rows["19-7-Sit10"]["function"] == "Bericht"
    assert rows["19-7-Sit22"]["page_range"] == ""
    assert rows["18-81-Sit8"]["speaker_label"] == "Dr. Stefanie Hubig"


def test_raw_files_order_suffixes_after_their_number(tmp_path):
    counts = _fetch.merge_into_raw_files(list(_rows().values()), tmp_path)
    assert counts == {"18081": 9, "18001": 1, "19001": 1, "19007": 7}
    raw = json.loads((tmp_path / "raw-18081-media.json").read_text())
    assert [r["origin_media_id"] for r in raw["rows"]] == [
        "18-81-Sit6", "18-81-Sit6a", "18-81-Sit6b", "18-81-Sit6c", "18-81-Sit7",
        "18-81-Sit8", "18-81-Sit9a", "18-81-Sit71a", "18-81-Sit71b"]


def test_legacy_raw_rows_are_rekeyed_and_keep_their_fingerprint(tmp_path):
    legacy = {
        "parliament": "DE-RP", "session_period": "18", "session_number": "81",
        "speech_index_in_session": 6, "speaker_label": "Helge Schwab",
        "faction": "FREIE WÄHLER", "video_url": f"{VIDEOS}81-Sit6.mp4",
        "origin_media_id": "81-Sit6",
        "video_last_modified": "2025-01-30T10:00:00+00:00",
        "video_content_length": 1234,
    }
    (tmp_path / "raw-18081-media.json").write_text(
        json.dumps({"meta": {"session": "18081"}, "rows": [legacy]}))
    _fetch.merge_into_raw_files(list(_rows().values()), tmp_path)
    rows = json.loads((tmp_path / "raw-18081-media.json").read_text())["rows"]
    ids = [r["origin_media_id"] for r in rows]
    assert ids.count("18-81-Sit6") == 1 and "81-Sit6" not in ids
    sit6 = rows[ids.index("18-81-Sit6")]
    assert sit6["video_content_length"] == 1234
    assert sit6["page_range"] == "21-23"     # the fresh row wins otherwise


class _Head:
    """Stub HEAD: url -> (status, headers), counting requests."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def __call__(self, url):
        self.calls.append(url)
        return self.responses[url]


def _fp_rows():
    return [
        {"origin_media_id": "18-81-Sit6", "video_url": f"{VIDEOS}81-Sit6.mp4"},
        {"origin_media_id": "18-81-Sit6a", "video_url": f"{VIDEOS}81-Sit6a.mp4"},
    ]


def test_fingerprint_records_headers_once_and_marks_missing_files():
    head = _Head({
        f"{VIDEOS}81-Sit6.mp4": (200, {"Last-Modified": "Thu, 30 Jan 2025 10:08:22 GMT",
                                       "Content-Length": "143439030"}),
        f"{VIDEOS}81-Sit6a.mp4": (302, {"Location": "https://www.landtag.rlp.de/"}),
    })
    rows = _fp_rows()
    throttle = _fetch.Throttle(0)
    assert _fetch.fingerprint_rows(rows, head=head, throttle=throttle) == 2
    assert rows[0]["video_last_modified"] == "2025-01-30T10:08:22+00:00"
    assert rows[0]["video_content_length"] == 143439030
    assert rows[1]["video_missing"] is True
    # Fingerprinted and missing rows are not requested again.
    assert _fetch.fingerprint_rows(rows, head=head, throttle=throttle) == 0
    assert len(head.calls) == 2


def test_recheck_flags_a_replaced_file():
    rows = _fp_rows()[:1]
    rows[0].update(video_last_modified="2025-01-30T10:08:22+00:00",
                   video_content_length=143439030)
    head = _Head({f"{VIDEOS}81-Sit6.mp4": (200, {"Last-Modified": "Wed, 28 Oct 2026 13:24:31 GMT",
                                                 "Content-Length": "101558037"})})
    assert _fetch.fingerprint_rows(rows, head=head, throttle=_fetch.Throttle(0)) == 0
    assert _fetch.fingerprint_rows(rows, recheck=True, head=head,
                                   throttle=_fetch.Throttle(0)) == 1
    assert rows[0]["video_replaced"] is True
    # The recorded fingerprint is the one of the speech's own file.
    assert rows[0]["video_content_length"] == 143439030


def _media_session(tmp_path, rows, proceedings_date="2025-01-30T09:30:00"):
    media_dir = tmp_path / "original" / "media"
    proceedings_dir = tmp_path / "original" / "proceedings"
    media_dir.mkdir(parents=True)
    proceedings_dir.mkdir(parents=True)
    _fetch.merge_into_raw_files(rows, media_dir)
    sid = _fetch._raw_session_id(rows[0])
    if proceedings_date:
        (proceedings_dir / f"{sid}-proceedings.json").write_text(json.dumps(
            {"meta": {"dateStart": proceedings_date, "dateEnd": proceedings_date}}))
    return _media.parse_media_session(media_dir / f"raw-{sid}-media.json")


def test_media_order_faction_office_and_fingerprint(tmp_path):
    rows = [r for r in _rows().values() if r["origin_media_id"].startswith("18-81-")]
    rows[0]["video_last_modified"] = "2025-01-30T10:08:22+00:00"   # 18-81-Sit6
    rows[0]["video_content_length"] = 143439030
    doc = _media_session(tmp_path, rows)
    by_id = {s["media"]["originMediaID"]: s for s in doc["data"]}
    assert [s["media"]["originMediaID"] for s in doc["data"]][:5] == [
        "18-81-Sit6", "18-81-Sit6a", "18-81-Sit6b", "18-81-Sit6c", "18-81-Sit7"]
    assert [s["speechIndex"] for s in doc["data"]] == list(range(1, 10))

    paul = by_id["18-81-Sit6a"]["people"][0]
    assert paul["faction"] == {"label": "AfD"} and paul["role"] == "Kurzintervention"
    hubig = by_id["18-81-Sit8"]["people"][0]
    assert "faction" not in hubig and hubig["role"] == "BM"
    hering = by_id["18-81-Sit9a"]["people"][0]
    assert "faction" not in hering and hering["role"] == "Präsident"
    assert hering["context"] == "main-speaker"

    assert by_id["18-81-Sit6"]["media"]["additionalInformation"] == {
        "lastModified": "2025-01-30T10:08:22+00:00", "contentLength": 143439030}
    assert "additionalInformation" not in by_id["18-81-Sit7"]["media"]
    assert not any(s["debug"].get("mediaReplaced") for s in doc["data"])


def test_media_flags_a_clip_modified_long_after_the_sitting(tmp_path, caplog):
    rows = [r for r in _rows().values() if r["origin_media_id"] in ("18-81-Sit6", "18-81-Sit7")]
    rows[0]["video_last_modified"] = "2026-10-28T13:24:31+00:00"
    rows[1]["video_last_modified"] = "2025-01-30T10:00:00+00:00"
    with caplog.at_level(logging.ERROR):
        doc = _media_session(tmp_path, rows)
    flags = {s["media"]["originMediaID"]: s["debug"].get("mediaReplaced") for s in doc["data"]}
    assert flags == {"18-81-Sit6": True, "18-81-Sit7": None}
    assert "no longer plays this speech" in caplog.text


def test_media_flags_replaced_and_missing_rows_without_a_sitting_date(tmp_path):
    rows = [r for r in _rows().values() if r["origin_media_id"] in ("18-81-Sit6", "18-81-Sit7")]
    rows[0]["video_replaced"] = True
    rows[1]["video_missing"] = True
    doc = _media_session(tmp_path, rows, proceedings_date=None)
    assert all(s["debug"]["mediaReplaced"] for s in doc["data"])


def test_office_detection():
    for office in ("Präsident", "Vizepräsidentin", "Landtagsvizepräsidentin", "MdI", "BM",
                   "MKUEM", "Ministerpräsident", "Chef der Staatskanzlei",
                   "Bevollmächtigte des Landes beim Bund"):
        assert _common.is_office(office), office
    for faction in ("CDU", "SPD", "AfD", "FDP", "BÜNDNIS 90/DIE GRÜNEN", "FREIE WÄHLER",
                    "Gruppe FREIE WÄHLER", "fraktionslos", "DIE LINKE", ""):
        assert not _common.is_office(faction), faction
    assert _common.STATUS_TRANSLATION["Landtagsvizepräsidentin"] == "vice-president"


def test_opal_search_url_and_saved_page(tmp_path, caplog):
    url = _opal.opal_search_url(19, date(2026, 9, 16), date(2026, 9, 17))
    assert url == ("https://opal.rlp.de/portal/browse.tt.html?type=generic4&action=link"
                   "&searchgeneric4-from=16.09.2026&searchgeneric4-to=17.09.2026"
                   "&searchgeneric4-wp=19")
    assert _opal.hit_counter("Treffer: 1 bis 50 von 149") == (50, 149)

    fetched = []

    def fetch(u):
        fetched.append(u)
        return '<h2 id="efxSegmentCounter">Treffer: 1 bis 50 von 149</h2>'

    with caplog.at_level(logging.WARNING):
        path = _opal.fetch_speech_search(19, date(2026, 9, 16), date(2026, 9, 17),
                                         tmp_path, fetch=fetch)
    assert fetched == [url]
    assert path == tmp_path / "opal-wp19-2026-09-16-2026-09-17.html"
    assert "Treffer" in path.read_text()
    assert "Only 50 of 149 hits rendered" in caplog.text


def _speech(people):
    return {
        "session": {"number": 81, "dateStart": "", "dateEnd": ""},
        "speechIndex": 1, "originTextID": "18081-001",
        "agendaItem": {"officialTitle": "", "title": ""},
        "people": people, "textContents": [], "documents": [], "debug": {},
    }


def test_merger_keeps_the_proceedings_role():
    media = _speech([{"label": "Michael Ebling", "context": "main-speaker", "role": "MdI"}])
    proceedings = _speech([{"label": "Michael Ebling", "context": "main-speaker",
                            "role": "Minister"}])
    merged = _merger.merge_item(media, [proceedings])
    assert merged["people"][0]["role"] == "Minister"

    media = _speech([{"label": "Joachim Paul", "context": "main-speaker",
                      "role": "Kurzintervention", "faction": {"label": "AfD"}}])
    proceedings = _speech([{"label": "Joachim Paul", "context": "main-speaker",
                            "faction": "AfD"}])
    merged = _merger.merge_item(media, [proceedings])
    assert merged["people"][0]["role"] == "Kurzintervention"


def test_fixture_rows_are_detail_rows():
    # Guard the fixture against drifting from the markup extract_rows targets.
    root = lxml_html.parse(str(FIXTURE)).getroot()
    rows = [p for p in root.iter("p") if _fetch._is_detail_row(p)]
    assert len(rows) == 23


def test_download_ingests_the_data_dir_inbox_unless_told_otherwise(tmp_path, monkeypatch):
    workflow = importlib.import_module("optv.parliaments.DE-RP.workflow")
    seen = []
    monkeypatch.setattr(workflow, "ingest_inbox", lambda inbox, proceedings: seen.append(inbox))
    monkeypatch.setattr(workflow, "ingest_html_inbox", lambda *a, **k: None)
    config = workflow.Config(tmp_path)
    args = SimpleNamespace(inbox_dir=None, media_inbox_dir=None, fetch_opal=None,
                           fingerprint_media=False, recheck_media=False, period=18)

    workflow._download(config, args)
    args.inbox_dir = str(tmp_path / "elsewhere")
    workflow._download(config, args)

    assert seen == [tmp_path / "inbox", tmp_path / "elsewhere"]
