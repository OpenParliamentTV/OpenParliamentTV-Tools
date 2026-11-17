"""PT parser + common-config unit tests (no network / no spaCy).

Synthetic fixtures model the av.parlamento.pt per-meeting JSON, the meeting
page, and the debates.parlamento.pt ``?sft=true`` full-text HTML (one ``<p>``
per PDF line inside one ``waypointMarker`` div per Diário page).
"""

from __future__ import annotations

from optv.parliaments.PT.common import (
    make_session, parse_session, session_number_int, source_label,
)
from optv.parliaments.PT.parsers.media2json import (
    _content_path, _hms_to_seconds, localize, parse_av,
)
from optv.parliaments.PT.parsers.proceedings2json import join_split_fragments, parse_dar
from optv.parliaments.PT.scraper.fetch_media import parse_meeting_sources


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
# media2json: time parsing, recording URLs, timezone, sidecars
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


def test_localize_lisbon_offsets():
    # Lisbon is UTC+0 in winter and UTC+1 in summer.
    assert localize("2026-02-25T14:58:00") == "2026-02-25T14:58:00+00:00"
    assert localize("2026-09-16T15:01:00") == "2026-09-16T15:01:00+01:00"
    assert localize("") is None
    assert localize("2026-02-25") is None


def _av_doc():
    return {
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


def test_parse_av_builds_clip_and_session_urls():
    doc = parse_av(_av_doc(), "17-1-059")
    assert doc["meta"]["description"].startswith("Fixação")
    assert doc["meta"]["sessionVideo"].endswith("2026_02_25_059.mp4/index.m3u8")
    assert doc["meta"]["dateStart"] == "2026-02-25T14:58:00+00:00"
    assert doc["meta"]["openData"] is False
    item = doc["data"][0]
    assert item["videoFileURI"] == (
        "https://av.parlamento.pt/content/hls/DAR/L17/SL1/A2026/M02/"
        "2026_02_25_059.mp4/ClipFrom/681708/ClipTo/1374379/index.m3u8")
    assert item["audioFileURI"].endswith("2026_02_25_059.mp4/index.m3u8")
    assert item["sourcePage"] == "https://av.parlamento.pt/videos/Plenary/17/1/59/4"
    assert round(item["startOffset"], 3) == 681.708
    assert round(item["duration"], 3) == round(22 * 60 + 54.379 - (11 * 60 + 21.708), 3)
    assert item["affiliation"] == {"name": "Bloco de Esquerda", "initials": "BE"}
    assert item["summary"] == ""
    assert "od" not in item


def test_parse_av_uses_meeting_sources_summaries_and_open_data():
    sources = {"hls": "https://av.parlamento.pt/content/hls/DAR/L17/SL1/A2026/M02/"
                      "2026_02_26_059.mp4/index.m3u8"}
    metadata = [{"number": 4, "title": "Apresenta o Projeto de Lei n.º 447/XVII/1.ª (BE)."}]
    od = {"session": "17-1-059", "interventions": {
        "4": {"id": "1", "personID": "4321", "pages": [4, 6], "debateID": "9"}}}
    doc = parse_av(_av_doc(), "17-1-059", metadata=metadata, sources=sources, od=od)
    item = doc["data"][0]
    # the meeting page's recording wins over the path built from eventDate
    assert "2026_02_26_059.mp4/ClipFrom/681708/" in item["videoFileURI"]
    assert item["audioFileURI"] == sources["hls"]
    assert item["summary"].startswith("Apresenta o Projeto de Lei")
    assert item["od"]["personID"] == "4321"
    assert item["od"]["pages"] == [4, 6]
    assert doc["meta"]["openData"] is True


def test_parse_meeting_sources():
    page = """
    <script type="text/javascript">
        var meetingVideoSources = [
            { src: "https://av.parlamento.pt/content/hls/DAR/L17/SL2/A2026/M09/2026_09_16_001.mp4/index.m3u8", type: "application/vnd.apple.mpegurl" },
            { src: "https://av.parlamento.pt/content/dash/DAR/L17/SL2/A2026/M09/2026_09_16_001.mp4/manifest.mpd", type: "application/dash+xml" },
            { src: "https://av.parlamento.pt/content/pd/DAR/L17/SL2/A2026/M09/2026_09_16_001.mp4", type: "video/mp4" }
        ];
        var labelPAOD = "Antes da Ordem do Dia";
    </script>"""
    sources = parse_meeting_sources(page)
    assert sources["hls"].endswith("2026_09_16_001.mp4/index.m3u8")
    assert sources["dash"].endswith("manifest.mpd")
    assert sources["mp4"].endswith("/content/pd/DAR/L17/SL2/A2026/M09/2026_09_16_001.mp4")
    assert parse_meeting_sources("<html></html>") == {}


# --------------------------------------------------------------------------- #
# proceedings2json: speaker-turn extraction (regex sentencizer fallback)
# --------------------------------------------------------------------------- #

def _page(n: int, *lines: str) -> str:
    body = "".join(f"<p>{line} </p>" for line in lines)
    return (f'<div class="waypointMarker" data-page="{n}">'
            f'<h5 class="waypointMarkerTarget" id="p{n}">Página {n}</h5>{body}</div>')


_DAR_HTML = "<html><body>" + _page(
    3,
    "26 DE FEVEREIRO DE 2026",
    "3",
    "O Sr. Presidente: — Está aberta a sessão.",
    "Eram 15 horas e 3 minutos.",
    "O Sr. Secretário (Francisco Figueira): — Vou ler a lista. Obrigado.",
    "O Sr. Primeiro-Ministro (Luís Montenegro): — Sr. Presidente, começo por",
    "agradecer.",
    "O Sr. Fabian Figueiredo (BE): — As tempestades destruíram tudo. Fazemo-",
    "lo por todos.",
) + _page(
    4,
    "Aplausos do BE.",
    "O Sr. Hugo Soares (PSD) — Muito bem!",
    "O Sr. Fabian Figueiredo (BE): — … e continuamos.",
    "A Sr.a Sandra Ribeiro (CH): — Uma pergunta.",
    "O Sr. Pedro dos Santos Frazão CH): — Como assim?",
    "O Sr. Pedro Pinto (CH: — Exatamente!",
    "O Sr. FilipeSousa (JPP): — Obrigado.",
    "O Sr. Deputado Pedro Pinto pede a palavra. Faça favor.",
    "O Sr. Ministro disse — e bem — que não.",
    "O Sr. Primeiro-Ministro: — Respondo já.",
    "O Sr. Presidente (Teresa Morais): — Terminou o seu tempo.",
) + "</body></html>"


def test_parse_dar_turns():
    turns = parse_dar(_DAR_HTML, "17-1-059", spacy_model=None)["data"]
    got = [(t["speaker"], t["matchKey"], t["party"], t["isChair"]) for t in turns]
    assert got == [
        ("Presidente", "presidente", "", True),
        ("Francisco Figueira", "figueira", "", False),   # office: paren is the NAME
        ("Luís Montenegro", "montenegro", "", False),
        ("Fabian Figueiredo", "figueiredo", "BE", False),
        ("Hugo Soares", "soares", "PSD", False),         # no colon
        ("Fabian Figueiredo", "figueiredo", "BE", False),
        ("Sandra Ribeiro", "ribeiro", "CH", False),      # "Sr.a" for "Sr.ª"
        ("Pedro dos Santos Frazão", "frazao", "CH", False),  # lost "("
        ("Pedro Pinto", "pinto", "CH", False),           # lost ")"
        ("FilipeSousa", "filipesousa", "JPP", False),    # lost space
        ("Luís Montenegro", "montenegro", "", False),    # office name carried forward
        ("Teresa Morais", "presidente", "", True),       # acting chair
    ]


def test_parse_dar_text_pages_and_continuation():
    turns = parse_dar(_DAR_HTML, "17-1-059", spacy_model=None)["data"]
    by_index = {t["index"]: t for t in turns}
    chair = by_index[1]
    # the time note is an editorial line, not speech
    assert " ".join(s["text"] for s in chair["sentences"]) == "Está aberta a sessão."
    fig = by_index[4]
    text = " ".join(s["text"] for s in fig["sentences"])
    # no leading dash; a line-final hyphen joins without a space
    assert text.startswith("As tempestades")
    assert "Fazemo-lo por todos." in text
    # the stage direction on the next page is dropped
    assert "Aplausos" not in text
    assert (fig["page"], fig["pageEnd"]) == (3, 3)
    assert fig["continuation"] is False
    resumed = by_index[6]
    assert resumed["continuation"] is True and resumed["page"] == 4
    # prose that only looks like a marker stays inside the previous turn
    sousa = by_index[10]
    assert "pede a palavra" in " ".join(s["text"] for s in sousa["sentences"])
    assert "disse — e bem" in " ".join(s["text"] for s in sousa["sentences"])
    assert sousa["nameKey"] == "filipesousa"
    assert all(t["chars"] > 0 for t in turns)


def test_sentence_fragments_are_rejoined():
    assert join_split_fragments(["Sr. Presidente, Sr.as e Srs.", "Deputados: começo.", "Fim."]) == \
        ["Sr. Presidente, Sr.as e Srs. Deputados: começo.", "Fim."]
    assert join_split_fragments(["Sr.as", "Deputadas e Srs.", "Deputados, boa tarde."]) == \
        ["Sr.as Deputadas e Srs. Deputados, boa tarde."]
    assert join_split_fragments(["Votou o PS.", "Aprovado."]) == ["Votou o PS.", "Aprovado."]
    # the "…" opening a resumed speech belongs to the next sentence; later
    # punctuation-only pieces close the previous one
    assert join_split_fragments(["…", "e continuamos.", "Mais de 15 dias?", "!", "Pois…", "…"]) == \
        ["… e continuamos.", "Mais de 15 dias?!", "Pois……"]
    assert join_split_fragments(["…"]) == ["…"]
    html = "<html><body>" + _page(
        2, "O Sr. Presidente: — Sr. Primeiro-Ministro, Sr.as e Srs. Deputados: está aberta a sessão.",
    ) + "</body></html>"
    turn = parse_dar(html, "17-1-059", spacy_model=None)["data"][0]
    assert [s["text"] for s in turn["sentences"]] == \
        ["Sr. Primeiro-Ministro, Sr.as e Srs. Deputados: está aberta a sessão."]
