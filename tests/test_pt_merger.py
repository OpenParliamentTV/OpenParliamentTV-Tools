"""PT merger tests: speaker-sequence alignment + end-to-end Stage 2 shape.

No network / no spaCy: the merger consumes the intermediate media + proceedings
JSON directly, and the result is validated against the full Stage 2 schema.
"""

from __future__ import annotations

import json

from optv.parliaments.PT.common import Config
from optv.parliaments.PT.merger.merge_session import align_speeches, merge_session
from optv.shared.validators import validate_stage2


# --------------------------------------------------------------------------- #
# Needleman-Wunsch alignment of av spine vs finer text turns
# --------------------------------------------------------------------------- #

def test_align_speeches_subsequence():
    # av spine (coarse) is a subsequence of the text turns (chair interjections
    # interleaved). Each av speech must map to the correct text turn index.
    av = ["presidente", "secretario", "presidente", "figueiredo", "real"]
    text = ["presidente", "secretario", "presidente", "figueiredo",
            "presidente", "real", "presidente"]
    mapping = align_speeches(av, text)
    assert mapping == {0: 0, 1: 1, 2: 2, 3: 3, 4: 5}


def test_align_speeches_repeated_surname_in_order():
    # Two av speeches by the same deputy align to successive text turns in order.
    av = ["figueiredo", "pinto", "figueiredo"]
    text = ["figueiredo", "presidente", "pinto", "presidente", "figueiredo"]
    mapping = align_speeches(av, text)
    assert mapping == {0: 0, 1: 2, 2: 4}


def test_align_speeches_empty():
    assert align_speeches([], ["a"]) == {}
    assert align_speeches(["a"], []) == {}


# --------------------------------------------------------------------------- #
# End-to-end merge → Stage 2 shape + schema validation
# --------------------------------------------------------------------------- #

def _media_doc():
    base = "https://av.parlamento.pt/content/hls/DAR/L17/SL1/A2026/M02/2026_02_25_059.mp4"
    return {
        "meta": {
            "session": "17-1-059", "parliament": "PT",
            "description": "Fixação da ordem do dia / Votações",
            "eventDate": "2026-02-25T14:58:00", "dateStart": "2026-02-25T14:58:00",
            "legislature": 17, "legislativeSession": 1, "meeting": 59,
            "processing": {"parse_media": "2026-02-25T00:00:00"},
        },
        "data": [
            {"number": 1, "interventionType": "Abertura da sessão", "speakerType": "Deputado",
             "speaker": "JOSÉ PEDRO AGUIAR BRANCO", "role": "Presidente",
             "affiliation": {"name": "Partido Social Democrata", "initials": "PSD"},
             "startOffset": 8.0, "duration": 85.0,
             "videoFileURI": f"{base}/ClipFrom/8000/ClipTo/93000/index.m3u8",
             "audioFileURI": f"{base}/index.m3u8",
             "sourcePage": "https://av.parlamento.pt/videos/Plenary/17/1/59/1"},
            {"number": 4, "interventionType": "Intervenção", "speakerType": "Deputado",
             "speaker": "FABIAN FIGUEIREDO", "role": "Deputado",
             "affiliation": {"name": "Bloco de Esquerda", "initials": "BE"},
             "startOffset": 681.7, "duration": 692.6,
             "videoFileURI": f"{base}/ClipFrom/681700/ClipTo/1374300/index.m3u8",
             "audioFileURI": f"{base}/index.m3u8",
             "sourcePage": "https://av.parlamento.pt/videos/Plenary/17/1/59/4"},
        ],
    }


def _proceedings_doc():
    return {
        "meta": {"session": "17-1-059", "parliament": "PT",
                 "processing": {"parse_proceedings": "2026-02-25T00:00:00"}},
        "data": [
            {"index": 1, "speaker": "Presidente", "surname": "", "matchKey": "presidente",
             "role": "Presidente", "party": "", "isChair": True,
             "sentences": [{"text": "Está aberta a sessão."}]},
            {"index": 2, "speaker": "Fabian Figueiredo", "surname": "figueiredo",
             "matchKey": "figueiredo", "role": "", "party": "BE", "isChair": False,
             "sentences": [{"text": "As tempestades destruíram tudo."},
                           {"text": "Precisamos de agir agora."}]},
        ],
    }


def test_merge_session_end_to_end(tmp_path):
    config = Config(tmp_path)
    config.dir("media", create=True)
    config.file("17-1-059", "media", create=True).write_text(json.dumps(_media_doc()))
    config.file("17-1-059", "proceedings", create=True).write_text(json.dumps(_proceedings_doc()))

    class Args:
        period = 17

    out = merge_session("17-1-059", config, Args())
    doc = json.loads(out.read_text())
    data = doc["data"]
    assert len(data) == 2

    # envelope + encoding
    assert doc["meta"]["sourceLabel"] == "L17 SL1 Reunião 059"
    assert data[0]["session"]["number"] == 1059
    assert data[0]["electoralPeriod"]["number"] == 17

    # speech 1: chair, opening agenda type, no faction, no text matched is fine
    pres = data[0]
    assert pres["people"][0]["context"] == "president"
    assert pres["agendaItem"]["type"] == "opening"
    assert pres["agendaItem"]["nativeType"] == "PT-abertura"
    assert pres["textContents"][0]["textBody"][0]["sentences"][0]["text"] == "Está aberta a sessão."

    # speech 2: deputy, faction from av, verbatim text from DAR
    fig = data[1]
    assert fig["people"][0]["faction"]["label"] == "BE"
    assert fig["agendaItem"]["type"] == "regular"
    assert fig["media"]["videoFileURI"].endswith("ClipFrom/681700/ClipTo/1374300/index.m3u8")
    assert fig["media"]["additionalInformation"]["startOffset"] == 681.7
    tb = fig["textContents"][0]["textBody"][0]
    assert tb["speaker"] == "FABIAN FIGUEIREDO"
    assert len(tb["sentences"]) == 2

    # per-speech dateStart = session start + offset, monotonic
    assert data[0]["dateStart"] < data[1]["dateStart"]

    # full Stage 2 semantic validation: no errors
    findings = validate_stage2(doc, schema="full", semantic=True)
    errors = [f for f in findings if f["severity"] == "error"]
    assert errors == [], errors
