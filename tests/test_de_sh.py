"""DE-SH tests: m7k result blocks (incl. ``class="result crap"``), chair-slot
filtering, agenda resolution for speeches without a topic, and Europe/Berlin
offsets on the Stage 2 dates.

No network: result.php and iframe.php responses are generated in m7k's markup.
The DE-SH package directory has a hyphen, so it is imported via ``importlib``.
"""

from __future__ import annotations

import importlib
import json

from optv.shared.validators import validate_stage2

_fetch = importlib.import_module("optv.parliaments.DE-SH.scraper.fetch_media")
_parser = importlib.import_module("optv.parliaments.DE-SH.parsers.media2json")
_merger = importlib.import_module("optv.parliaments.DE-SH.merger.merge_session")
Config = importlib.import_module("optv.parliaments.DE-SH.common").Config

DATE = "09.10.2026"
ISO = "2026-10-09"
MP4 = "/486x374/2026-10-09T09.59.40.913P02.00.mp4"


def _block(bid, *, top="", thema="", redner="", gruppe="", beginn="10:00:00",
           ende="10:05:00", cls="result", datum=DATE):
    return f"""<a href="?wp=6&tg=178&sg=alle&fn=alle&rd=alle&b={bid}">
<div class="{cls}" id="{bid}">
	<div class="wann">
		<div class="wp">20</div>
		<div class="breaker"></div>
		<div class="datum">{datum}</div>
		<div class="breaker"></div>
		<div class="tagung">46</div>
	</div>
	<div class="hauptinfo">
		<div class="top">{top}</div>
		<div class="breaker"></div>
		<div class="thema">{thema}</div>
		<div class="breaker"></div>
		<div class="redner">{redner}</div>
		<div class="gruppe">{gruppe}</div>
	</div>
	<div class="zeit">
		<div class="beginn">{beginn}</div>
		<div class="breaker"></div>
		<div class="ende">{ende}</div>
		<div class="breaker"></div>
		<div class="dauer">00:05:00</div>
	</div>
	<div class="breaker"></div>
</div>
</a>
"""


def _iframe(start, end):
    return (f'<video><source src="{MP4}#t={start},{end}" type="video/mp4">'
            f'<source src="{MP4[:-4]}.webm#t={start},{end}" type="video/webm"></video>')


# One sitting day modelled on Tagung 46, 2026-10-09 (TOP 22+37 without topics).
_SITTING = [
    # (id, fields) — beginn order
    ("35529", dict(thema="Pr&auml;sidium", beginn="10:02:02", ende="10:06:09")),
    ("35530", dict(cls="result crap", top="22+37", redner="Serpil Midyatli", gruppe="SPD",
                   beginn="10:06:09", ende="10:10:03")),
    ("35535", dict(thema="Pr&auml;sidium", redner="Annabell Kr&auml;mer", gruppe="FDP",
                   beginn="10:39:01", ende="10:48:19")),
    ("35538", dict(cls="result crap", top="22+37", redner="Minister Claus Ruhe Madsen",
                   gruppe="Regierung", beginn="10:51:39", ende="10:59:13")),
    ("35541", dict(top="31", thema="H&auml;usliche Versorgung st&auml;rken",
                   redner="Dr. Heiner Garg", gruppe="FDP", beginn="11:03:06", ende="11:08:51")),
    ("35542", dict(cls="result crap", top="31", redner="Andrea Tschacher", gruppe="CDU",
                   beginn="11:08:51", ende="11:13:23")),
    ("35550", dict(cls="result crap", top="40", redner="Christopher Vogt", gruppe="FDP",
                   beginn="12:00:00", ende="12:05:00")),
    # Chair slot of the constituent-sitting kind: "Präsidium" as the speaker.
    ("35551", dict(redner="Pr&auml;sidium", beginn="12:05:00", ende="12:06:00")),
]


def _write_sitting(tmp_path):
    config = Config(tmp_path)
    media_dir = config.dir("media", create=True)
    result_dir = media_dir / "result" / "wp20"
    result_dir.mkdir(parents=True)
    result_dir.joinpath("tagung-046.html").write_text(
        "".join(_block(bid, **f) for bid, f in _SITTING), encoding="utf-8")
    for n, (bid, _) in enumerate(_SITTING):
        p = media_dir / "iframe" / ISO / f"{bid}.html"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(_iframe(100 * n, 100 * n + 90))
    metadata = config.dir("nel_data", create=True)
    metadata.joinpath("archive-wp20.json").write_text(json.dumps(
        {"wp": 20, "wp_internal_id": 6, "tagungen": [], "sitzung_by_date": {ISO: 131}}))
    return config


def test_result_regexes_accept_crap_class():
    html = (_block("1", top="1", thema="Aktuelle Stunde", redner="A B", gruppe="SPD")
            + _block("2", cls="result crap", top="22+37", redner="C D", gruppe="CDU")
            + _block("3", cls="results", redner="E F", gruppe="FDP"))
    assert [r["id"] for r in _parser.parse_result_html(html)] == ["1", "2"]
    assert _fetch._list_speech_ids(html) == [("1", ISO), ("2", ISO)]


def test_is_chair_slot():
    assert _parser.is_chair_slot({"redner": "", "gruppe": ""})
    assert _parser.is_chair_slot({"redner": "Präsidium", "gruppe": ""})
    assert not _parser.is_chair_slot({"redner": "Annabell Krämer", "gruppe": "FDP"})
    assert not _parser.is_chair_slot({"redner": "Präsidium", "gruppe": "SPD"})


def test_parse_media_keeps_crap_speeches_and_drops_chair_slots(tmp_path):
    config = _write_sitting(tmp_path)
    _parser.parse_media_directory(config.dir("media"))
    doc = json.loads(config.file("20131", "media").read_text())
    assert [s["speech_id"] for s in doc["data"]] == [
        "35530", "35535", "35538", "35541", "35542", "35550"]
    assert doc["data"][0]["videoFileURI"] == f"https://m7k.ltsh.de{MP4}#t=100,190"


def test_speech_span_is_dst_aware():
    span = _merger._speech_span
    start, end = span("2026-02-05", {"beginn": "10:00:00", "ende": "10:05:00"})
    assert (start.isoformat(), end.isoformat()) == (
        "2026-02-05T10:00:00+01:00", "2026-02-05T10:05:00+01:00")
    start, end = span(ISO, {"beginn": "23:58:00", "ende": "00:03:00"})
    assert end.isoformat() == "2026-10-10T00:03:00+02:00"
    start, end = span(ISO, {"beginn": "", "ende": "", "duration_seconds": 60})
    assert (start.isoformat(), end.isoformat()) == (
        "2026-10-09T00:00:00+02:00", "2026-10-09T00:01:00+02:00")


def test_merge_resolves_agenda_and_emits_offsets(tmp_path):
    config = _write_sitting(tmp_path)
    _parser.parse_media_directory(config.dir("media"))
    title = "Einführung eines Spritpreisdeckels"
    turns = [{"index": i, "matchKey": key, "agendaTitle": title,
              "sentences": [{"text": "Sehr geehrte Damen und Herren."}]}
             for i, key in enumerate(["midyatli", "kramer", "madsen"], start=1)]
    config.file("20131", "proceedings", create=True).write_text(
        json.dumps({"meta": {}, "data": turns}))

    doc = json.loads(_merger.merge_session("20131", config, None).read_text())
    by_id = {s["originID"]: s for s in doc["data"]}

    for sid in ("35530", "35535", "35538"):        # TOP 22+37: protocol title
        agenda = by_id[sid]["agendaItem"]
        assert agenda["id"] == "TOP-22+37"
        assert agenda["title"] == title
        assert agenda["officialTitle"] == f"TOP 22+37: {title}"
        assert by_id[sid]["debug"]["agendaSource"] == "proceedings"
    assert by_id["35535"]["debug"]["themaRaw"] == "Präsidium"   # TOP from neighbours
    assert by_id["35542"]["agendaItem"]["title"] == "Häusliche Versorgung stärken"
    assert by_id["35542"]["debug"]["agendaSource"] == "m7k-sibling"
    assert by_id["35550"]["agendaItem"]["title"] == "TOP 40"
    assert by_id["35550"]["debug"]["agendaSource"] == "top-number"
    assert by_id["35541"]["debug"]["agendaSource"] == "m7k"

    first = by_id["35530"]
    assert first["dateStart"] == "2026-10-09T10:06:09+02:00"
    assert first["dateEnd"] == "2026-10-09T10:10:03+02:00"
    assert first["debug"]["timesAreVideoRelative"] is False
    assert doc["meta"]["dateStart"] == "2026-10-09T10:06:09+02:00"
    assert doc["meta"]["dateEnd"] == "2026-10-09T12:05:00+02:00"
    assert {(s["session"]["dateStart"], s["session"]["dateEnd"]) for s in doc["data"]} == {
        ("2026-10-09T10:06:09+02:00", "2026-10-09T12:05:00+02:00")}
    assert by_id["35538"]["people"][0]["label"] == "Claus Ruhe Madsen"

    errors = [f for f in validate_stage2(doc, schema="full", semantic=True)
              if f["severity"] == "error"]
    assert errors == []


def test_archive_sessions_feed_the_proceedings_fetch(tmp_path):
    fetch_proc = importlib.import_module("optv.parliaments.DE-SH.scraper.fetch_proceedings")
    config = Config(tmp_path)
    config.dir("nel_data", create=True).joinpath("archive-wp20.json").write_text(json.dumps({
        "wp": 20, "wp_internal_id": 6,
        "tagungen": [{"tagung_no": 46, "internal_id": 178,
                      "sitzungen": [[1, "08.10.2026"], [2, "09.10.2026"]]}],
        # 2026-10-07 is not an m7k sitting day of the listed Tagungen.
        "sitzung_by_date": {"2026-10-07": 129, "2026-10-08": 130, "2026-10-09": 131},
    }))
    assert fetch_proc.archive_sessions(config) == [("20130", "2026-10-08"),
                                                   ("20131", ISO)]
    assert fetch_proc.url_for("20131", ISO).endswith(
        "/wahl20/plenum/plenprot/2026/20-131_10-26.pdf")
