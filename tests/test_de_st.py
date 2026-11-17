"""DE-ST tests: archive discovery, Sitzung numbering, slot parsing, speaker
turns, sitting times and the end-to-end merge.

No network: the scrapers run against trimmed copies of the live pages
(``fetch_text`` is monkeypatched for the archive walk), the protocol times come
from a text excerpt of protocol 9/2, and the merger reads transcripts from the
cache directory. The DE-ST package directory has a hyphen, so it is imported
via ``importlib``.
"""

from __future__ import annotations

import importlib
import json
import shutil
from pathlib import Path
from urllib.error import HTTPError

import pytest

from optv.shared.pdf2tei.sitting_times import parse_sitting_times
from optv.shared.validators import validate_stage2

_common = importlib.import_module("optv.parliaments.DE-ST.scraper.common")
_archive = importlib.import_module("optv.parliaments.DE-ST.scraper.fetch_archive")
_pcommon = importlib.import_module("optv.parliaments.DE-ST.parsers.common")
_proc = importlib.import_module("optv.parliaments.DE-ST.parsers.proceedings2json")
_media = importlib.import_module("optv.parliaments.DE-ST.parsers.media2json")
_merger = importlib.import_module("optv.parliaments.DE-ST.merger.merge_session")
Config = importlib.import_module("optv.parliaments.DE-ST.common").Config

FIXTURES = Path(__file__).parent / "fixtures" / "DE-ST"
BASE = "https://www.landtag.sachsen-anhalt.de"
SP2_URL = f"{BASE}/2-sitzungsperiode-1"


def _sp2_html() -> str:
    return (FIXTURES / "sp-09-002.html").read_text(encoding="utf-8")


def _protocol_text() -> str:
    return (FIXTURES / "protocol-9-2-excerpt.txt").read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Archive + Sitzung map
# --------------------------------------------------------------------------- #

def test_parse_archive_keeps_hrefs_and_ignores_header_link():
    listed = _archive.parse_archive((FIXTURES / "archive.html").read_text(encoding="utf-8"))
    # The header's "3. Sitzungsperiode" link (/3-sitzungsperide, no videos yet)
    # is not under the period heading and must not be picked up.
    assert listed == {9: [(1, f"{BASE}/1-sitzungsperiode"), (2, SP2_URL)]}


def test_section_protocols_give_sitzung_numbers():
    assert _archive.extract_section_dates(_sp2_html()) == [(0, "2026-10-14")]
    assert _archive.extract_section_protocols(_sp2_html(), 9) == {
        0: (f"{BASE}/fileadmin/files/plenum/wp9/002stzg.pdf", 2)}
    assert _archive.extract_section_protocols(_sp2_html(), 8) == {}


def test_assign_sitzung_numbers():
    assert _archive.assign_sitzung_numbers([1, 2, None, None]) == [
        (1, "protocol"), (2, "protocol"), (3, "sequence"), (4, "sequence")]
    # Days before the first protocol are counted back from it.
    assert _archive.assign_sitzung_numbers([None, 106, None]) == [
        (105, "sequence"), (106, "protocol"), (107, "sequence")]
    assert _archive.assign_sitzung_numbers([None, None]) == [(1, "sequence"), (2, "sequence")]


def _fake_portal(monkeypatch, pages: dict[str, str]) -> list[str]:
    calls: list[str] = []

    def fake_fetch_text(url, **kwargs):
        calls.append(url)
        return pages[url]

    monkeypatch.setattr(_archive, "fetch_text", fake_fetch_text)
    return calls


def test_build_sitzung_map(tmp_path, monkeypatch):
    sp1 = _sp2_html().replace("2026-10-14", "2026-10-06").replace(
        "14.10.2026", "06.10.2026").replace("wp9/002stzg.pdf", "wp9/001stzg.pdf")
    calls = _fake_portal(monkeypatch, {
        _archive.ARCHIVE_URL: (FIXTURES / "archive.html").read_text(encoding="utf-8"),
        f"{BASE}/1-sitzungsperiode": sp1,
        SP2_URL: _sp2_html(),
    })
    kwargs = dict(period=9, proceedings_dir=tmp_path / "proc", metadata_dir=tmp_path / "meta")
    sitzung_map = _archive.fetch_archive_and_build_sitzung_map(**kwargs)
    assert sitzung_map["sitzungsperioden"][1] == {
        "sp": 2, "url": SP2_URL,
        "sittings": [{"sitzung": 2, "date": "2026-10-14", "section": 0,
                      "protocolURL": f"{BASE}/fileadmin/files/plenum/wp9/002stzg.pdf",
                      "numberSource": "protocol"}],
    }
    assert (tmp_path / "proc" / "sp-09-002.html").exists()
    map_file = tmp_path / "meta" / "sitzung-map-09.json"
    mtime = map_file.stat().st_mtime_ns

    # Second run: pages with all protocols are not fetched again and the
    # unchanged map is not rewritten (its mtime drives the parse stage).
    calls.clear()
    _archive.fetch_archive_and_build_sitzung_map(**kwargs)
    assert calls == [_archive.ARCHIVE_URL]
    assert map_file.stat().st_mtime_ns == mtime


def test_build_sitzung_map_refreshes_page_until_protocol(tmp_path, monkeypatch):
    without_protocol = _sp2_html().replace("002stzg.pdf", "002xxxx.pdf")
    pages = {
        _archive.ARCHIVE_URL: (FIXTURES / "archive.html").read_text(encoding="utf-8").replace(
            '<li><a href="/1-sitzungsperiode" class="intern">1. Sitzungsperiode vom 6. Oktober 2026</a></li>', ""),
        SP2_URL: without_protocol,
    }
    calls = _fake_portal(monkeypatch, pages)
    kwargs = dict(period=9, proceedings_dir=tmp_path / "proc", metadata_dir=tmp_path / "meta")
    m = _archive.fetch_archive_and_build_sitzung_map(**kwargs)
    assert m["sitzungsperioden"][0]["sittings"][0]["numberSource"] == "sequence"
    pages[SP2_URL] = _sp2_html()
    calls.clear()
    m = _archive.fetch_archive_and_build_sitzung_map(**kwargs)
    assert SP2_URL in calls
    assert m["sitzungsperioden"][0]["sittings"][0]["protocolURL"].endswith("/wp9/002stzg.pdf")


def test_build_sitzung_map_other_period_is_offline(tmp_path, monkeypatch):
    _fake_portal(monkeypatch, {_archive.ARCHIVE_URL: (FIXTURES / "archive.html").read_text(encoding="utf-8")})
    assert _archive.fetch_archive_and_build_sitzung_map(
        period=8, proceedings_dir=tmp_path / "proc", metadata_dir=tmp_path / "meta") is None
    assert not (tmp_path / "meta" / "sitzung-map-08.json").exists()


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #

def test_fetch_does_not_retry_404(monkeypatch):
    calls = []

    def fake_urlopen(req, timeout=None):
        calls.append(req.full_url)
        raise HTTPError(req.full_url, 404, "Not Found", {}, None)

    monkeypatch.setattr(_common, "urlopen", fake_urlopen)
    monkeypatch.setattr(_common, "POLITE_DELAY", 0)
    monkeypatch.setattr(_common.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError):
        _common.fetch(f"{BASE}/2-sitzungsperiode", retry_count=5)
    assert len(calls) == 1


def test_fetch_retries_server_errors(monkeypatch):
    calls = []

    def fake_urlopen(req, timeout=None):
        calls.append(req.full_url)
        raise HTTPError(req.full_url, 503, "Unavailable", {}, None)

    monkeypatch.setattr(_common, "urlopen", fake_urlopen)
    monkeypatch.setattr(_common, "POLITE_DELAY", 0)
    monkeypatch.setattr(_common.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError):
        _common.fetch(f"{BASE}/archiv", retry_count=3)
    assert len(calls) == 3


# --------------------------------------------------------------------------- #
# Slot parsing
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("h3,profile,expected", [
    ("Sven Schulze (CDU)  - Ministerpräsident", False, ("Sven Schulze", "CDU", "Ministerpräsident")),
    ("Dr. Gunnar Schellenberger   - Landtagspräsident", False,
     ("Dr. Gunnar Schellenberger", None, "Landtagspräsident")),
    ("Ulrich Siegmund (AfD)", True, ("Ulrich Siegmund", "AfD", None)),
    ("Dr. Claudia Wittig (BSW)", True, ("Dr. Claudia Wittig", "BSW", None)),
    ("Eva von Angern (DIE LINKE) - Fraktionsvorsitzende", False,
     ("Eva von Angern", "Die Linke", "Fraktionsvorsitzende")),
    ("Abstimmung (Namensaufruf)", False, ("", None, None)),
    ("Wahl (Feststellung des Wahlergebnisses)", False, ("", None, None)),
    ("Eröffnung (Fortsetzung)", False, ("", None, None)),
    ("Erster Wahlgang", False, ("", None, None)),
    ("Schlussbemerkungen", False, ("", None, None)),
])
def test_parse_speaker_label(h3, profile, expected):
    r = _pcommon.parse_speaker_label(h3, has_profile=profile)
    assert (r["label"], r["party"], r["role"]) == expected
    assert r["is_procedural"] == (expected[0] == "")


@pytest.mark.parametrize("line,expected", [
    ("Präsident Tobias Rausch:", ("Tobias Rausch", "Präsident", None, True)),
    ("Vizepräsidentin Anne-Marie Keding:", ("Anne-Marie Keding", "Vizepräsidentin", None, True)),
    ("Daniel Roi (AfD):", ("Daniel Roi", None, "AfD", False)),
    ("Matthias Büttner (Staßfurt) (AfD):", ("Matthias Büttner", None, "AfD", False)),
    ("Olaf Meister (GRÜNE):", ("Olaf Meister", None, "BÜNDNIS 90/DIE GRÜNEN", False)),
    ("Sven Schulze (Ministerpräsident):", ("Sven Schulze", "Ministerpräsident", None, False)),
    ("Gordon Köhler (Berichterstatter):", ("Gordon Köhler", "Berichterstatter", None, False)),
    ("Ich zitiere:", None),
    ("Frau Gensecke von der SPD - ich zitiere :", None),
    ("Der Ablauf ist wie folgt vorgesehen:", None),
])
def test_parse_speaker_line(line, expected):
    r = _pcommon.parse_speaker_line(line)
    if expected is None:
        assert r is None
    else:
        assert (r["name"], r["role"], r["faction"], r["presidium"]) == expected


def test_parse_day_section_slots():
    from lxml import html as lxml_html
    sec = lxml_html.fromstring(_sp2_html()).xpath('//section[@id="section-0"]')[0]
    blocks = {b["std_player_id"]: b for b in _proc.parse_day_section(sec)}
    assert list(blocks) == ["123543", "123547", "123548", "123551", "123554", "123567"]
    schulze = blocks["123554"]
    assert schulze["video_anchor"] == "#section-video-4-3"
    # The AJAX transcript URL is kept as linked: its cHash is bound to the
    # page path with the "-1" suffix.
    assert schulze["transcript_url"] == (
        f"{SP2_URL}?transcriptSessions=lsaSessionsAjax"
        "&tx_lsasessions_transcript%5Bspeaker%5D=19403&cHash=a8b28cad9c43903cdd808d13e4899e56")
    assert schulze["transcript_page_url"].startswith(f"{BASE}/plenarsitzungen/transkript?")
    assert (schulze["speaker_label"], schulze["party"], schulze["role"]) == (
        "Sven Schulze", "CDU", "Ministerpräsident")
    assert blocks["123551"]["party"] == "AfD"
    for pid in ("123543", "123548", "123567"):  # Eröffnung, Eröffnung (Fortsetzung), Abstimmung
        assert blocks[pid]["is_procedural"] and blocks[pid]["party"] is None


# --------------------------------------------------------------------------- #
# Sitting times
# --------------------------------------------------------------------------- #

def test_shared_sitting_times_accepts_de_st_forms():
    assert parse_sitting_times("Beginn: 10 Uhr. … Unterbrechung: 10:14 Uhr. "
                               "Wiederbeginn: 11:15 Uhr. … Schluss: 13:14 Uhr.") == {
        "begin": "10:00", "end": "13:14", "breaks": [["10:14", "11:15"]]}


def test_sitting_times_from_protocol_text():
    times = _proc.sitting_times_from_text(_protocol_text())
    assert times["begin"] == "10:00" and times["end"] == "13:14"
    [br] = times["breaks"]
    assert (br["stop"], br["resume"]) == ("10:14", "11:15")
    # The anchor skips the speaker line "Präsident Tobias Rausch:".
    assert br["anchor"].startswith("sehrgeehrtekollegenabgeordnetediesitzungsunterbrechung")


def _slots(*letters_):
    return [(60, s) for s in letters_]


def test_speech_windows_offsets_and_breaks():
    times = {"begin": "10:00", "end": "13:14",
             "breaks": [{"stop": "10:14", "resume": "11:15", "anchor": "diesitzungsunterbrechung"}]}
    w = _merger.speech_windows("2026-10-14", times, _slots("a", "b", "xxdiesitzungsunterbrechungxx", "c"))
    starts = [s.isoformat() for s, _ in w]
    assert starts == ["2026-10-14T10:00:00+02:00", "2026-10-14T10:01:00+02:00",
                      "2026-10-14T11:15:00+02:00", "2026-10-14T11:16:00+02:00"]
    # Winter time after 2026-10-25; 09:00 without a protocol.
    [(s, e)] = _merger.speech_windows("2026-11-18", None, _slots("a"))
    assert (s.isoformat(), e.isoformat()) == ("2026-11-18T09:00:00+01:00", "2026-11-18T09:01:00+01:00")


def test_speech_windows_unmatched_break_by_clock():
    times = {"begin": "10:00", "end": "12:00",
             "breaks": [{"stop": "10:01", "resume": "11:00", "anchor": "nowhere"}]}
    w = _merger.speech_windows("2026-10-14", times, _slots("a", "b", "c"))
    # The second clip would start at 10:01, the interruption: it starts at
    # the resumption instead.
    assert [s.strftime("%H:%M") for s, _ in w] == ["10:00", "11:00", "11:01"]


def test_sitting_order_follows_the_protocol():
    body = _proc.protocol_body_letters(
        "Inhalt: Tagesordnungspunkt 8 Wahl … Beginn: 11 Uhr. "
        "Präsident Tobias Rausch: Wir beraten zuerst den Antrag zur Geschäftsordnung im Landtag. "
        "Wir kommen zur Wahl des Präsidenten des Landtages von Sachsen-Anhalt jetzt. "
        "Wir setzen die Beratung zur Geschäftsordnung nach der Wahl jetzt fort. Schluss: 13 Uhr.")
    slots = [
        ["Wir beraten zuerst den Antrag zur Geschäftsordnung im Landtag."],      # TOP 6, part 1
        ["Präsident Tobias Rausch:",
         "Wir setzen die Beratung zur Geschäftsordnung nach der Wahl jetzt fort."],  # TOP 6, part 2
        ["Text, den das Protokoll so nicht enthält, aber lang genug ist."],       # not found
        ["Wir kommen zur Wahl des Präsidenten des Landtages von Sachsen-Anhalt jetzt."],  # TOP 8
    ]
    # The clip not found stays right after its predecessor on the page.
    assert _merger.sitting_order(body, slots) == [0, 3, 1, 2]
    assert _merger.sitting_order(None, slots) == [0, 1, 2, 3]


# --------------------------------------------------------------------------- #
# Speaker turns
# --------------------------------------------------------------------------- #

def test_drop_headings():
    paras = ["Tagesordnungspunkt 30", "Rückkehr zur Pflegeausbildung - Abschaffung!",
             "Antrag Fraktion AfD - Drs. 8/6495",
             "Einbringer ist Herr Siegmund. - Bitte sehr, Sie haben das Wort.",
             "Abstimmung",
             "Wer dem zustimmt, den bitte ich um das Kartenzeichen. - Das sind die Koalition",
             "(Zuruf)"]
    assert _merger.drop_headings(paras) == [
        "Einbringer ist Herr Siegmund. - Bitte sehr, Sie haben das Wort.",
        "Wer dem zustimmt, den bitte ich um das Kartenzeichen. - Das sind die Koalition",
        "(Zuruf)"]


def test_turns_people_and_textbody():
    paras = ["Einbringer ist der Ministerpräsident. - Bitte sehr.",
             "Sven Schulze (Ministerpräsident):", "Sehr geehrter Herr Präsident!",
             "(Beifall bei der CDU)", "Weiter im Text.",
             "Präsident Tobias Rausch:", "Es gibt eine Frage von Herrn Roi.",
             "Daniel Roi (AfD):", "Meine Frage lautet:",
             "Sven Schulze (Ministerpräsident):", "Die Antwort."]
    slot = {"type": "memberOfParliament", "label": "Sven Schulze", "context": "main-speaker",
            "faction": {"label": "CDU"}, "role": "Ministerpräsident"}
    previous = {"type": "memberOfParliament", "label": "Tobias Rausch", "role": "Präsident",
                "context": "president"}
    turns = _merger.split_turns(paras)
    people, turn_people = _merger.resolve_people(slot, turns, previous=previous)
    assert [(p["label"], p["context"]) for p in people] == [
        ("Sven Schulze", "main-speaker"), ("Tobias Rausch", "president"), ("Daniel Roi", "speaker")]
    body = _merger.build_textbody(turns, turn_people, "09002-006")
    assert [(b["type"], b["speaker"], b["speakerstatus"]) for b in body] == [
        ("speech", "Tobias Rausch", "president"),
        ("speech", "Sven Schulze", "main-speaker"),
        ("comment", None, None),
        ("speech", "Sven Schulze", "main-speaker"),
        ("speech", "Tobias Rausch", "president"),
        ("speech", "Daniel Roi", "speaker"),
        ("speech", "Sven Schulze", "main-speaker")]


def test_procedural_slot_takes_presiding_officer():
    turns = _merger.split_turns(["Abstimmung", "Wir stimmen ab. - Das ist die Mehrheit."])
    previous = {"type": "memberOfParliament", "label": "Wulf Gallert", "role": "Vizepräsident",
                "context": "vice-president"}
    people, _ = _merger.resolve_people({"label": "", "context": "main-speaker"}, turns, previous=previous)
    assert [(p["label"], p["context"]) for p in people] == [("Wulf Gallert", "vice-president")]
    people, _ = _merger.resolve_people(
        {"label": "", "context": "main-speaker"},
        _merger.split_turns(["Eröffnung", "Präsident Tobias Rausch:", "Ich eröffne die Sitzung."]))
    assert [(p["label"], p["context"]) for p in people] == [("Tobias Rausch", "president")]


# --------------------------------------------------------------------------- #
# End to end
# --------------------------------------------------------------------------- #

_TRANSCRIPTS = {  # std player-id → transcript speaker-id (fixture file)
    "123543": "19393", "123547": "19404", "123548": "19406",
    "123551": "19394", "123554": "19403", "123567": "19401",
}


def _data_dir(tmp_path: Path, monkeypatch) -> Path:
    data = tmp_path / "data"
    proc = data / "original" / "proceedings"
    (proc / "transcripts" / "09002").mkdir(parents=True)
    (data / "metadata").mkdir(parents=True)
    (proc / "sp-09-002.html").write_text(_sp2_html(), encoding="utf-8")
    for sid in _TRANSCRIPTS.values():
        shutil.copy(FIXTURES / "transcripts" / f"{sid}.html", proc / "transcripts" / "09002" / f"{sid}.html")
    (data / "metadata" / "sitzung-map-09.json").write_text(json.dumps({
        "period": 9,
        "sitzungsperioden": [{"sp": 2, "url": SP2_URL, "sittings": [{
            "sitzung": 2, "date": "2026-10-14", "section": 0,
            "protocolURL": f"{BASE}/fileadmin/files/plenum/wp9/002stzg.pdf",
            "numberSource": "protocol"}]}],
    }))
    # The protocol PDF is not a fixture: its text excerpt stands in for it.
    (proc / "09002.pdf").write_bytes(b"%PDF-")
    monkeypatch.setattr(_proc, "protocol_text_from_pdf", lambda pdf: _protocol_text())
    media = {"meta": {"session": "09002", "processing": {}}, "data": {
        pid: {"player_id": pid, "video_id": int(pid), "duration": 300,
              "preview_image_url": None, "videoFileURI": f"https://app.vms.landtag-lsa.de/{pid}.mp4",
              "sources_by_quality": {"mp4_1080p": f"https://app.vms.landtag-lsa.de/{pid}.mp4"}}
        for pid in _TRANSCRIPTS}}
    (data / "original" / "media" / "09002").mkdir(parents=True)
    (data / "original" / "media" / "09002-media.json").write_text(json.dumps(media))
    return data


def test_end_to_end_merge(tmp_path, monkeypatch):
    data = _data_dir(tmp_path, monkeypatch)
    monkeypatch.setattr(_merger, "fetch_text", lambda *a, **k: pytest.fail("network access"))
    _proc.parse_proceedings_directory(data / "original" / "proceedings",
                                      metadata_dir=data / "metadata", period=9)
    proceedings = data / "original" / "proceedings" / "09002-proceedings.json"
    assert json.loads(proceedings.read_text())["meta"]["sittingTimes"]["begin"] == "10:00"

    class Options:
        retry_count = 1

    doc = json.loads(_merger.merge_session("09002", Config(data), Options()).read_text())
    findings = validate_stage2(doc, schema="full", semantic=True)
    assert [f for f in findings if f["severity"] == "error"] == []
    assert not [f for f in findings if f["rule"] == "semantic.media.sourcePage.duplicate"]

    speeches = {s["debug"]["stdPlayerId"]: s for s in doc["data"]}
    schulze = speeches["123554"]
    assert schulze["media"]["sourcePage"] == f"{SP2_URL}#section-video-4-3"
    assert schulze["textContents"][0]["sourceURI"].startswith(f"{BASE}/plenarsitzungen/transkript?")
    assert schulze["textContents"][0]["license"] == "Amtliches Werk (§ 5 Abs. 2 UrhG)"
    assert [(p["label"], p["context"]) for p in schulze["people"]][:2] == [
        ("Sven Schulze", "main-speaker"), ("Tobias Rausch", "president")]
    assert {"Daniel Roi", "Matthias Büttner"} <= {p["label"] for p in schulze["people"]}

    assert doc["data"][0]["dateStart"] == "2026-10-14T10:00:00+02:00"
    assert doc["data"][0]["session"]["dateStart"] == "2026-10-14T10:00:00+02:00"
    assert doc["data"][0]["session"]["dateEnd"] == "2026-10-14T13:14:00+02:00"
    # "Eröffnung (Fortsetzung)" is the clip the sitting resumed with after the
    # 10:14–11:15 break.
    assert speeches["123548"]["dateStart"] == "2026-10-14T11:15:00+02:00"
    assert speeches["123548"]["people"][0]["label"] == "Tobias Rausch"
    assert all(p.get("faction", {}).get("label") != "Namensaufruf"
               for s in doc["data"] for p in s["people"])
    assert {s["debug"]["sittingTimesSource"] for s in doc["data"]} == {"protocol"}


def test_parse_is_idempotent(tmp_path, monkeypatch):
    data = _data_dir(tmp_path, monkeypatch)
    kwargs = dict(metadata_dir=data / "metadata", period=9)
    proc = data / "original" / "proceedings"
    _proc.parse_proceedings_directory(proc, **kwargs)
    out = proc / "09002-proceedings.json"
    mtime = out.stat().st_mtime_ns
    (proc / "sp-09-002.html").touch()  # a refreshed page with the same content
    _proc.parse_proceedings_directory(proc, **kwargs)
    assert out.stat().st_mtime_ns == mtime
