"""DE-NI tests: agenda classification, parser flatten, end-to-end Stage 2 shape.

No network: the parser runs against a static items.json fixture (the scraper's
output shape), and the merger consumes the intermediate media JSON. The merged
result is validated against the full Stage 2 schema. The DE-NI package directory
has a hyphen, so it is imported via ``importlib`` rather than a normal import.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path

from optv.shared.agenda_types import classify_de_ni
from optv.shared.validators import validate_stage2

_scraper = importlib.import_module("optv.parliaments.DE-NI.scraper.common")
_parser = importlib.import_module("optv.parliaments.DE-NI.parsers.media2json")
_merger = importlib.import_module("optv.parliaments.DE-NI.merger.merge_session")
Config = importlib.import_module("optv.parliaments.DE-NI.common").Config

FIXTURES = Path(__file__).parent / "fixtures" / "DE-NI"


# --------------------------------------------------------------------------- #
# Agenda classification (title + subjectArt + consultationType)
# --------------------------------------------------------------------------- #

def test_classify_de_ni():
    assert classify_de_ni("Haushaltsgesetz 2026", "Gesetzentwurf", "Erste Beratung") == \
        ("DE-NI-budget", "budget")
    assert classify_de_ni("Aktuelle Stunde zur Lage", None, None) == \
        ("DE-NI-current_affairs", "current_affairs")
    assert classify_de_ni("Irgendein Thema", "Gesetzentwurf", "Erste Beratung") == \
        ("DE-NI-gesetzentwurf", "regular")
    assert classify_de_ni("Schulpolitik", "Antrag", "Abschließende Beratung") == \
        ("DE-NI-antrag", "regular")
    assert classify_de_ni("Mitteilungen der Präsidentin", "", "") == \
        ("DE-NI-announcements", "procedural")
    assert classify_de_ni("Mündliche Anfragen", "", "") == ("DE-NI-question_time", "qa")
    # nothing recognisable falls through to regular
    assert classify_de_ni("", None, None) == (None, "regular")


# --------------------------------------------------------------------------- #
# Scraper helpers: session key + clip URL construction
# --------------------------------------------------------------------------- #

def test_session_key_and_clip_uri():
    assert _scraper.session_key(19, 80) == "19080"
    assert _scraper.video_clip_uri("session30_meeting80_85516", 382.0, 505.0) == \
        "https://vod.plenartv.de/stream/session30_meeting80_85516/index.m3u8?start=382&end=505"
    assert _scraper.video_clip_uri("", 1, 2) == ""


# --------------------------------------------------------------------------- #
# Parser: flatten subjects × speakerTimings; context + wall-clock + ordering
# --------------------------------------------------------------------------- #

def test_parse_session():
    doc = _parser.parse_session(FIXTURES / "items-sample.json")
    data = doc["data"]
    assert len(data) == 3
    # global order is by stream start second: 382, 700, 1000
    assert [s["start_secs"] for s in data] == [382.0, 700.0, 1000.0]
    assert [s["speech_index"] for s in data] == [1, 2, 3]

    pres = data[0]
    assert pres["label"] == "Barbara Otte-Kinast"
    assert pres["context"] == "president"          # speechType "Mitteilungen"
    assert pres["faction"] == "CDU"
    # wall-clock = stream startTime (UTC) + offset
    assert pres["start_datetime"] == "2025-12-16T08:01:38Z"
    assert pres["end_datetime"] == "2025-12-16T08:03:41Z"

    minister = data[1]
    assert minister["label"] == "Gerald Heere"
    assert minister["context"] == "main-speaker"   # speechType "Antwort"
    assert minister["faction"] == ""               # government speaker, no party

    mp = data[2]
    assert mp["context"] == "main-speaker"         # speechType "RZ"
    assert mp["faction"] == "SPD"


# --------------------------------------------------------------------------- #
# End-to-end merge → Stage 2 shape + schema validation
# --------------------------------------------------------------------------- #

def test_merge_session_end_to_end(tmp_path):
    media_doc = _parser.parse_session(FIXTURES / "items-sample.json")
    config = Config(tmp_path)
    config.dir("media", create=True)
    config.file("19080", "media", create=True).write_text(json.dumps(media_doc))

    out = _merger.merge_session("19080", config, None)
    doc = json.loads(out.read_text())
    data = doc["data"]
    assert len(data) == 3
    assert "merge" in doc["meta"]["processing"]
    assert doc["meta"]["tagungsabschnitt"] == 30

    pres = data[0]
    assert pres["parliament"] == "DE-NI"
    assert pres["electoralPeriod"]["number"] == 19
    assert pres["session"]["number"] == 80
    assert pres["people"][0]["context"] == "president"
    assert pres["people"][0]["faction"]["label"] == "CDU"
    assert pres["people"][0]["originPersonID"] == "202835"
    assert pres["agendaItem"]["type"] == "procedural"
    assert pres["textContents"] == []
    # per-speech server-side clip URL (the clip IS the speech, no #t fragment)
    assert pres["media"]["videoFileURI"] == (
        "https://vod.plenartv.de/stream/session30_meeting80_85516/index.m3u8"
        "?start=382&end=505")
    assert pres["debug"]["timesAreVideoRelative"] is False

    budget = data[2]
    assert budget["agendaItem"]["type"] == "budget"
    assert budget["agendaItem"]["nativeType"] == "DE-NI-budget"

    # sourcePage must be unique per speech (platform keys speech identity on it)
    pages = [s["media"]["sourcePage"] for s in data]
    assert len(set(pages)) == len(pages)

    # downstream-required keys present on every speech
    required = {"speechIndex", "session", "electoralPeriod", "people",
                "textContents", "media", "agendaItem", "originID"}
    for sp in data:
        assert not (required - set(sp))

    # full Stage 2 schema: structural validation must be error-free
    findings = validate_stage2(doc, schema="full", semantic=True)
    errors = [f for f in findings if f["severity"] == "error"]
    assert errors == []
