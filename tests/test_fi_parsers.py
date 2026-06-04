"""FI parser unit tests (PTK proceedings + broadcast media + helpers).

Fixtures under ``tests/fixtures/FI/`` are trimmed, synthetic copies modelled on
the real PTK 58/2026 vp minutes and the verkkolähetys ``speakers[]`` payload for
plenary 58/2026. No spaCy or network access is needed: the PTK parser is driven
with a stub sentencizer.
"""

from __future__ import annotations

import json
from pathlib import Path

import lxml.etree as ET

from optv.parliaments.FI.common import (
    parse_session_str, session_number_int, session_str, term_years,
)
from optv.parliaments.FI.parsers import proceedings2json as p2j
from optv.parliaments.FI.parsers.media2json import speaker_media_record
from optv.parliaments.FI.scraper.fetch_media import _extract_json_value
from optv.shared.agenda_types import (
    CORE_BUDGET, CORE_QA, CORE_REGULAR, CORE_VOTING, classify_fi,
)

FIX = Path(__file__).resolve().parent / "fixtures" / "FI"


class StubNLP:
    """Minimal sentencizer: split on sentence-final punctuation."""

    def __call__(self, text):
        import re

        parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", text) if p.strip()]

        class S:
            def __init__(self, t):
                self.text = t

        class Doc:
            sents = [S(p) for p in parts]

        return Doc()


# ---- session-key helpers ----

def test_session_key_round_trip():
    assert session_str(2026, 58) == "2026-058"
    assert parse_session_str("2026-058") == (2026, 58)


def test_session_number_encoding_is_term_unique():
    # Per-year numbering collides; the encoding separates years of a term.
    assert session_number_int(2023, 2023, 58) == 58
    assert session_number_int(2023, 2026, 58) == 3058
    assert session_number_int(2023, 2024, 58) != session_number_int(2023, 2025, 58)


def test_term_years_spans_five_valtiopaivat():
    assert term_years(2023) == [2023, 2024, 2025, 2026, 2027]


# ---- agenda classification ----

def test_classify_fi_core_types():
    assert classify_fi("Suullinen kyselytunti") == ("FI-suullinen_kyselytunti", CORE_QA)
    assert classify_fi("Valtion talousarvio vuodelle 2027") == ("FI-talousarvio", CORE_BUDGET)
    assert classify_fi("Hallituksen esitys eduskunnalle ...") == ("FI-esitys", CORE_REGULAR)
    assert classify_fi("Ainoan käsittelyn äänestykset")[1] == CORE_VOTING
    assert classify_fi(None) == (None, CORE_REGULAR)
    assert classify_fi("Jokin tuntematon otsikko") == (None, CORE_REGULAR)


# ---- RSC flight value extraction (the riskiest media-scraper bit) ----

def test_extract_json_value_handles_rsc_date_and_undefined():
    flight = (
        'noise {"speakers":[{"name":"A","time":1,"x":$undefined,'
        '"ts":"$D2026-05-29T10:07:28.000Z"}]} trailing'
    )
    speakers = _extract_json_value(flight, "speakers")
    assert speakers == [{"name": "A", "time": 1, "x": None,
                         "ts": "2026-05-29T10:07:28.000Z"}]
    assert _extract_json_value(flight, "missing") is None


# ---- media records ----

def test_speaker_media_record_offsets_and_fragment():
    event = json.loads((FIX / "event-sample.json").read_text())
    rec = speaker_media_record(event["speakers"][0], event,
                               "https://verkkolahetys.eduskunta.fi/x")
    assert rec["personNumber"] == 1391
    assert rec["isReply"] is False
    media = rec["media"]
    assert media["videoFileURI"].endswith("playlist.m3u8#t=455,483")
    assert "#t=" not in media["audioFileURI"]  # clean master for align_prep
    assert media["additionalInformation"]["startOffset"] == 455.0
    assert media["duration"] == 28.0
    assert media["additionalInformation"]["eventRef"] == event["eventId"]


def test_speaker_media_record_reply_flag():
    event = json.loads((FIX / "event-sample.json").read_text())
    rec = speaker_media_record(event["speakers"][1], event, "x")
    assert rec["isReply"] is True


# ---- PTK proceedings parsing ----

def _parse_fixture_speeches():
    root = ET.fromstring((FIX / "ptk-sample.xml").read_bytes())
    nlp = StubNLP()
    recs = []
    for tp in p2j._findall_local(root, "PuheenvuoroToimenpide"):
        rec = p2j.speech_record(tp, len(recs) + 1, nlp, "Täysistunto")
        if rec:
            recs.append(rec)
    return recs


def test_ptk_parser_extracts_speaker_and_join_keys():
    recs = _parse_fixture_speeches()
    assert len(recs) == 2
    a = recs[0]
    assert a["people"][0]["label"] == "Ari Koponen"
    assert a["people"][0]["faction"] == {"label": "ps"}
    assert a["people"][0]["originPersonID"] == "1391"
    assert a["debug"]["personNumber"] == "1391"
    assert a["dateStart"] == "2026-05-29T13:07:28"
    assert a["originalLanguage"] == "fi"
    assert a["originID"] == "647919"


def test_ptk_parser_strips_brackets_and_excludes_chair():
    recs = _parse_fixture_speeches()
    sentences = recs[0]["textContents"][0]["textBody"][0]["sentences"]
    joined = " ".join(s["text"] for s in sentences)
    assert "Hälinää" not in joined            # editorial bracket stripped
    assert "Kiitoksia" not in joined          # chair PuheenjohtajaRepliikki excluded
    assert "työeläkejärjestelmää" in joined


def test_ptk_parser_agenda_classification():
    recs = _parse_fixture_speeches()
    agenda = recs[0]["agendaItem"]
    assert agenda["id"] == "kohta-6"
    assert agenda["type"] == CORE_REGULAR
    assert agenda["nativeType"] == "FI-esitys"
