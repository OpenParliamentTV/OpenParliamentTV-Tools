"""PT parser + common-config unit tests (no network / no spaCy).

Synthetic fixtures model the av.parlamento.pt per-meeting JSON and the
debates.parlamento.pt ``?sft=true`` full-text HTML for reunião 17/1/59.
"""

from __future__ import annotations

from optv.parliaments.PT.common import (
    make_session, parse_session, session_number_int, source_label,
)
from optv.parliaments.PT.parsers.media2json import (
    _content_path, _hms_to_seconds, parse_av,
)
from optv.parliaments.PT.parsers.proceedings2json import parse_dar


# --------------------------------------------------------------------------- #
# common.py session encoding
# --------------------------------------------------------------------------- #

def test_session_roundtrip_and_encoding():
    assert make_session(17, 1, 59) == "17-1-059"
    assert parse_session("17-1-059") == (17, 1, 59)
    # sessão legislativa banded ×1000 so reuniões don't collide across years
    assert session_number_int("17-1-059") == 1059
    assert session_number_int("17-2-059") == 2059
    assert session_number_int("17-1-007") == 1007
    assert source_label("17-1-059") == "L17 SL1 Reunião 059"


# --------------------------------------------------------------------------- #
# media2json: time parsing + HLS construction
# --------------------------------------------------------------------------- #

def test_hms_to_seconds():
    # 0h 11m 21.7085479s
    assert round(_hms_to_seconds("00:11:21.7085479"), 4) == round(11 * 60 + 21.7085479, 4)
    assert _hms_to_seconds("01:00:00") == 3600.0
    assert _hms_to_seconds(None) is None
    assert _hms_to_seconds("garbage") is None


def test_content_path():
    assert _content_path(17, 1, 59, "2026-02-25T14:58:00") == \
        "/content/hls/DAR/L17/SL1/A2026/M02/2026_02_25_059.mp4"
    assert _content_path(17, 1, 59, "") is None


def test_parse_av_builds_clip_and_session_urls():
    av_doc = {
        "title": "DAR 059",
        "description": "Fixação da ordem do dia / Votações",
        "eventDate": "2026-02-25T14:58:00",
        "interventions": [
            {"number": 4, "interventionType": "Intervenção", "speakerType": "Deputado",
             "speaker": "FABIAN FIGUEIREDO", "role": "Deputado",
             "affiliation": {"name": "Bloco de Esquerda", "initials": "BE"},
             "startTime": "00:11:21.708", "endTime": "00:22:54.379",
             "duration": "00:11:32.671"},
        ],
    }
    doc = parse_av(av_doc, "17-1-059")
    assert doc["meta"]["description"].startswith("Fixação")
    assert doc["meta"]["sessionVideo"].endswith("2026_02_25_059.mp4/index.m3u8")
    item = doc["data"][0]
    assert item["videoFileURI"] == (
        "https://av.parlamento.pt/content/hls/DAR/L17/SL1/A2026/M02/"
        "2026_02_25_059.mp4/ClipFrom/681708/ClipTo/1374379/index.m3u8")
    assert item["audioFileURI"].endswith("2026_02_25_059.mp4/index.m3u8")
    assert item["sourcePage"] == "https://av.parlamento.pt/videos/Plenary/17/1/59/4"
    assert round(item["startOffset"], 3) == 681.708
    assert round(item["duration"], 3) == round(22 * 60 + 54.379 - (11 * 60 + 21.708), 3)
    assert item["affiliation"] == {"name": "Bloco de Esquerda", "initials": "BE"}


# --------------------------------------------------------------------------- #
# proceedings2json: speaker-turn extraction (regex sentencizer fallback)
# --------------------------------------------------------------------------- #

_DAR_HTML = """
<html><body>
<p>26 DE FEVEREIRO DE 2026</p>
<p>3</p>
<p>O Sr. Presidente: Está aberta a sessão. Tem a palavra o Sr. Deputado.</p>
<p>O Sr. Secretário (Francisco Figueira): Vou ler a lista. Obrigado.</p>
<p>O Sr. Fabian Figueiredo (BE): Senhor Presidente. As tempestades destruíram tudo.</p>
<p>Aplausos do BE.</p>
<p>A Sr.ª Inês de Sousa Real (PAN): Muito obrigada. Precisamos de agir.</p>
<p>O Sr. Presidente: Terminou o seu tempo.</p>
</body></html>
"""


def test_parse_dar_turns():
    doc = parse_dar(_DAR_HTML, "17-1-059", spacy_model=None)
    turns = doc["data"]
    keys = [(t["matchKey"], t["party"], t["isChair"]) for t in turns]
    assert keys == [
        ("presidente", "", True),
        ("secretario", "", True),       # parenthetical is the officer's NAME, not a party
        ("figueiredo", "BE", False),
        ("real", "PAN", False),         # surname is the last token
        ("presidente", "", True),
    ]
    # the secretary's name comes from the parenthetical
    assert turns[1]["speaker"] == "Francisco Figueira"
    # deputy turn keeps the verbatim text
    fig = turns[2]
    assert fig["speaker"] == "Fabian Figueiredo"
    assert fig["sentences"]
    assert "tempestades" in fig["sentences"][-1]["text"].lower()
    # running header / page number paragraphs are dropped (not turns)
    assert all("FEVEREIRO" not in t["speaker"] for t in turns)
