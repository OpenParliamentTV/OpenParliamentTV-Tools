"""DE-NW tests: archive discovery, sitting time zone, speech windows, HTML
protocol parsing and the per-agenda-item text join.

No network: the scrapers run against static HTML fixtures (``fetch_text`` is
monkeypatched for the archive walk), and the merger consumes intermediate JSON
built in a temp dir. The DE-NW package directory has a hyphen, so it is
imported via ``importlib``.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path

from optv.shared.lang.de import regex_sentencize
from optv.shared.validators import validate_stage2

_scraper = importlib.import_module("optv.parliaments.DE-NW.scraper.common")
_archive = importlib.import_module("optv.parliaments.DE-NW.scraper.fetch_archive")
_media = importlib.import_module("optv.parliaments.DE-NW.parsers.media2json")
_protocol = importlib.import_module("optv.parliaments.DE-NW.parsers.proceedings2json")
_merger = importlib.import_module("optv.parliaments.DE-NW.merger.merge_session")
Config = importlib.import_module("optv.parliaments.DE-NW.common").Config

FIXTURES = Path(__file__).parent / "fixtures" / "DE-NW"
KID_133 = "f70d51d8-6544-402c-b72f-61d6ed7aa637"
KID_67 = "5d350784-266a-4569-97c6-2f63de542e0f"
KID_66 = "12616f98-296d-4b29-a810-ef96fe3b3a6f"


# --------------------------------------------------------------------------- #
# Archive discovery
# --------------------------------------------------------------------------- #

def test_archive_page_parsing():
    html = (FIXTURES / "archive-page.html").read_text()
    # Pagination links are written "&amp;page=N".
    assert _scraper.archive_max_page(html) == 32
    cards = _scraper.parse_archive_page(html)
    assert cards == [
        {"kid": KID_133, "sitzung": 133, "date": "2026-10-15"},
        {"kid": KID_67, "sitzung": 67, "date": "2024-06-12"},
        {"kid": KID_66, "sitzung": 66, "date": "2024-06-03"},
    ]


def test_scope_start_is_speech_list_start_for_wp18():
    assert _archive.scope_start(18) == "2024-06-12"


def _card(kid, sitzung, date):
    return (f'<article class="e-multiple-teaser"><a href="/v?kid={kid}">Video</a>'
            f'<time datetime="{date} 10:00" class="e-multiple-teaser__date"></time>'
            f'<a class="e-multiple-teaser__link" href="/v?kid={kid}">{sitzung}. Plenarsitzung</a>'
            f'</article>')


def _uuid(n):
    return f"00000000-0000-0000-0000-{n:012d}"


def test_archive_walk_merges_cache_and_stops_before_scope(tmp_path, monkeypatch):
    pages = {
        1: '<a data-page="9">9</a>' + _card(_uuid(134), 134, "2026-11-12"),
        2: _card(_uuid(67), 67, "2024-06-12") + _card(_uuid(66), 66, "2024-06-03"),
        3: _card(_uuid(65), 65, "2024-05-17"),
        4: _card(_uuid(64), 64, "2024-05-16"),
    }
    requested = []

    def fake_fetch(url, retry_count=0):
        page = int(url.rsplit("=", 1)[1])
        requested.append(page)
        return pages.get(page, "")

    monkeypatch.setattr(_archive, "fetch_text", fake_fetch)
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    # A cached entry from an earlier run, without sitzung/date.
    cached = {"wp": 18, "sessions": [_archive._entry(18, _uuid(67))]}
    (metadata / "archive-wp18.json").write_text(json.dumps(cached))

    archive = _archive.fetch_archive(period=18, media_dir=tmp_path / "media",
                                     metadata_dir=metadata)
    # Page 3 holds only cards before the scope start, so the walk stops there.
    assert requested == [1, 2, 3]
    by_kid = {s["kid"]: s for s in archive["sessions"]}
    assert set(by_kid) == {_uuid(134), _uuid(67)}
    assert by_kid[_uuid(67)]["session_id"] == "18067"
    assert by_kid[_uuid(134)]["date"] == "2026-11-12"


def test_seeded_run_returns_seeds_but_keeps_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(_archive, "fetch_text",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("no paging")))
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    cached = {"wp": 18, "sessions": [_archive._entry(18, _uuid(134), 134, "2026-11-12")]}
    (metadata / "archive-wp18.json").write_text(json.dumps(cached))

    archive = _archive.fetch_archive(period=18, media_dir=tmp_path / "media",
                                     metadata_dir=metadata, seed_urls=[KID_133])
    assert [s["kid"] for s in archive["sessions"]] == [KID_133]
    written = json.loads((metadata / "archive-wp18.json").read_text())
    assert {s["kid"] for s in written["sessions"]} == {_uuid(134), KID_133}


# --------------------------------------------------------------------------- #
# Sitting time zone + speech windows
# --------------------------------------------------------------------------- #

def test_session_start_is_localised_to_berlin():
    # The page stamps the server's offset on the day of the request.
    assert _media.berlin_session_start("2026-01-30T10:00:00+02:00") == "2026-01-30T10:00:00+01:00"
    assert _media.berlin_session_start("2026-07-15T10:00:00+01:00") == "2026-07-15T10:00:00+02:00"
    assert _media.berlin_session_start("") == ""
    # Offsets are added in absolute time (across the spring DST switch).
    assert _media._iso_offset("2026-03-29T01:30:00+02:00", 3600) == "2026-03-29T03:30:00+02:00"


def test_end_offset_rule():
    end = _media._end_offset
    assert end(100, 180, 190, False) == 190              # next start
    assert end(100, 180, 600, False) == 180              # unlisted item in between
    assert end(0, 30, 400, False, is_chair=True) == 400  # chair runs to next speech
    assert end(100, 180, None, True) == 180              # last speech
    assert end(100, None, 900, False) == 900             # no rendered end


def _speech(rid, name, start, end, top_nr="", top_title="", funktion="", fraktion=""):
    return {"index": rid, "top_redner_id": str(rid), "mdl_id": "", "funktion_id": "",
            "name": name, "fraktion": fraktion, "funktion": funktion, "top_nr": top_nr,
            "speech_nr": "", "parent_title": "", "top_title": top_title,
            "display_time": None, "start_offset": start, "rendered_end": end}


def _write_items(config) -> None:
    items = {
        "session_id": "18117", "wp": 18, "sitzung": 117, "date": "2026-01-30",
        "session_start_iso": "2026-01-30T10:00:00+02:00",
        "kid": "16904f0f-9e3d-4b7c-a9a2-ad09bd26ac69",
        "video_page_url": _scraper.video_page_url("16904f0f-9e3d-4b7c-a9a2-ad09bd26ac69"),
        "speeches": [
            _speech(1, "André Kuper", 0, 30, top_title="Sitzungseröffnung", funktion="Präsident"),
            _speech(2, "Klaus Esser", 400, 550, "1", "Erstes Thema", fraktion="AfD"),
            _speech(3, "Verena Schäffer", 560, 800, "1", "Erstes Thema", funktion="Ministerin"),
            _speech(4, "Dr. Dennis Maelzer", 1300, 1500, "2", "Zweites Thema", fraktion="SPD"),
        ],
    }
    media_dir = config.dir("media", create=True)
    (media_dir / "18117-items.json").write_text(json.dumps(items))


def test_parse_session_windows_and_wall_clock(tmp_path):
    config = Config(tmp_path)
    _write_items(config)
    doc = _media.parse_session(config.dir("media") / "18117-items.json")
    assert doc["meta"]["session_start_iso"] == "2026-01-30T10:00:00+01:00"
    ends = [s["end_offset"] for s in doc["data"]]
    # Chair opening → next start; 560 follows 550 closely; 800 → 1300 is a gap.
    assert ends == [400, 560, 800, 1500]
    assert doc["data"][1]["start_datetime"] == "2026-01-30T10:06:40+01:00"


# --------------------------------------------------------------------------- #
# HTML protocol
# --------------------------------------------------------------------------- #

def _turns():
    html = (FIXTURES / "protocol.html").read_bytes()
    return _protocol.protocol_to_turns(html, "18117", sentencize=regex_sentencize)


def test_speaker_head_classification():
    head = _protocol.parse_speaker_head
    assert head("Präsident André Kuper")["kind"] == "chair"
    vp = head("Vizepräsidentin Berivan Aymaz")
    assert (vp["kind"], vp["key"], vp["role"]) == ("chair", "aymaz", "Vizepräsidentin")
    mp = head("Klaus Esser*) (AfD)")
    assert (mp["kind"], mp["name"], mp["party"], mp["key"]) == ("member", "Klaus Esser", "AfD", "esser")
    gov = head("Verena Schäffer, Ministerin für Kinder, Jugend, Familie, Gleichstellung")
    assert (gov["kind"], gov["name"], gov["key"]) == ("government", "Verena Schäffer", "schaffer")


def test_protocol_turns_group_one_per_spine_speech():
    turns = _turns()
    assert [t["matchKey"] for t in turns] == ["kuper", "esser", "schaffer", "maelzer"]
    assert [t["topNumber"] for t in turns] == ["", "1", "1", "2"]
    texts = [" ".join(s["text"] for s in t["sentences"]) for t in turns]
    # The opening stays with the chair, not the first speech.
    assert "Ich eröffne die Sitzung." in texts[0]
    assert texts[1].startswith("Sehr geehrter Herr Präsident!")
    # A speech interrupted by the chair is one turn.
    assert "Ich komme zum Schluss." in texts[1]
    # Interposed question, answer and a continued short intervention stay in
    # the minister's turn.
    for part in ("Meine Frage lautet so.", "Die Antwort lautet so.",
                 "Letzter Satz der Kurzintervention.", "Die Erwiderung."):
        assert part in texts[2]
    # Chair text between an agenda heading and the first speaker is dropped;
    # the appendix after "Schluss" is not parsed.
    assert all("Ich eröffne die Aussprache." not in t for t in texts)
    assert all("Nicht gehaltene Rede." not in t for t in texts)
    assert turns[3]["speaker"] == "Dennis Maelzer (SPD)"


def test_protocol_bodies_carry_their_speaker():
    minister = _turns()[2]
    speech = [b for b in minister["bodies"] if b["type"] == "speech"]
    who = {(b["speakerName"], b.get("chairContext")) for b in speech}
    assert ("Christof Rasche", "vice-president") in who
    assert ("Thorsten Klute", None) in who
    assert minister["chairs"] == [{"name": "Christof Rasche", "context": "vice-president",
                                   "firstname": "Christof", "lastname": "Rasche"}]
    comments = [b for b in _turns()[1]["bodies"] if b["type"] == "comment"]
    assert comments[0]["sentences"][0]["text"] == "(Beifall von der AfD)"


# --------------------------------------------------------------------------- #
# Merger end-to-end
# --------------------------------------------------------------------------- #

def test_merge_session_joins_per_agenda_item(tmp_path):
    config = Config(tmp_path)
    _write_items(config)
    _media.parse_media_directory(config.dir("media"))
    html = (FIXTURES / "protocol.html").read_bytes()
    doc = _protocol.protocol_to_proceedings_doc(html, "18117", sentencize=regex_sentencize)
    config.save_data(doc, "18117", "proceedings")

    out = _merger.merge_session("18117", config)
    merged = json.loads(out.read_text())
    findings = validate_stage2(merged, schema="full", semantic=True)
    assert not [f for f in findings if f["severity"] == "error"], findings

    data = merged["data"]
    assert data[0]["dateStart"] == "2026-01-30T10:00:00+01:00"
    assert data[0]["media"]["videoFileURI"].endswith("#t=0,400")

    def body_text(rec):
        return " ".join(s["text"] for tc in rec["textContents"]
                        for b in tc["textBody"] for s in b["sentences"])

    assert "Ich eröffne die Sitzung." in body_text(data[0])
    assert body_text(data[1]).startswith("Sehr geehrter Herr Präsident!")
    assert "Rede zu Thema zwei." in body_text(data[3])

    minister = data[2]
    labels = [(p["label"], p["context"]) for p in minister["people"]]
    assert labels[0][0] == "Verena Schäffer"
    assert ("Christof Rasche", "vice-president") in labels
    statuses = {(b["speaker"], b.get("speakerstatus"))
                for b in minister["textContents"][0]["textBody"] if b["type"] == "speech"}
    assert ("Thorsten Klute", "speaker") in statuses
    assert ("Verena Schäffer", "main-speaker") in statuses
