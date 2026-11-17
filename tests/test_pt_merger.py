"""PT merger tests: speech units, the av ⋈ text join, agenda items, Stage 2 shape.

No network / no spaCy: the merger consumes the intermediate media + proceedings
JSON directly, and the result is validated against the full Stage 2 schema.
"""

from __future__ import annotations

import json

from optv.parliaments.PT.common import Config
from optv.parliaments.PT.merger.merge_session import (
    agenda_items, align_speeches, assign_agenda, build_units, merge_session,
    resolve_office_keys,
)
from optv.shared.validators import validate_stage2


def _turn(index, speaker, key, text, *, party="", chair=False, page=1,
          continuation=False, role="", name_key=None):
    return {"index": index, "speaker": speaker, "lead": role or speaker, "role": role,
            "party": party, "isChair": chair, "matchKey": key,
            "nameKey": name_key if name_key is not None else key,
            "continuation": continuation, "page": page, "pageEnd": page,
            "chars": len(text), "sentences": [{"text": text}]}


def _iv(number, speaker, *, role="Deputado", duration=10.0, od=None, itype="Intervenção",
        summary=""):
    iv = {"number": number, "interventionType": itype, "speaker": speaker, "role": role,
          "affiliation": {"name": "", "initials": ""}, "duration": duration,
          "startOffset": float(number * 100), "summary": summary}
    if od:
        iv["od"] = od
    return iv


LONG = "Palavras de um discurso longo. " * 40     # ~1,240 characters


# --------------------------------------------------------------------------- #
# Speech units
# --------------------------------------------------------------------------- #

def test_units_join_a_speech_across_apartes():
    turns = [
        _turn(1, "Presidente", "presidente", "Tem a palavra.", chair=True),
        _turn(2, "Fabian Figueiredo", "figueiredo", LONG),
        _turn(3, "Hugo Soares", "soares", "Muito bem!"),
        _turn(4, "Fabian Figueiredo", "figueiredo", "… e continuamos.", continuation=True),
        _turn(5, "Presidente", "presidente", "Terminou o tempo.", chair=True),
        _turn(6, "Fabian Figueiredo", "figueiredo", "Termino já."),
    ]
    units = build_units(turns)
    assert [(u.key, u.turns, u.own) for u in units] == [
        ("presidente", [0], [0]),
        ("figueiredo", [1, 2, 3], [1, 3]),   # the aparte is a member, not own text
        ("presidente", [4], [4]),            # a chair turn ends the unit
        ("figueiredo", [5], [5]),
    ]


# --------------------------------------------------------------------------- #
# Join
# --------------------------------------------------------------------------- #

def test_join_skips_an_aparte_for_a_long_speech():
    # Núncio's "Muito bem!" during Figueiredo's speech must not take his own
    # two-minute speech, which comes later.
    turns = [
        _turn(1, "Fabian Figueiredo", "figueiredo", LONG),
        _turn(2, "Paulo Núncio", "nuncio", "Muito bem!"),
        _turn(3, "Presidente", "presidente", "Tem a palavra.", chair=True),
        _turn(4, "Paulo Núncio", "nuncio", LONG),
    ]
    ivs = [_iv(1, "FABIAN FIGUEIREDO", duration=120), _iv(2, "PAULO NÚNCIO", duration=120)]
    units = build_units(turns)
    spans = align_speeches(ivs, units)
    nuncio_units = spans[1]
    assert [t for u in nuncio_units for t in units[u].own] == [3]


def test_join_resumes_a_speech_after_an_unlisted_chair_remark():
    turns = [
        _turn(1, "Rui Rocha", "rocha", LONG),
        _turn(2, "Presidente", "presidente", "Sr. Deputado, tem de concluir.", chair=True),
        _turn(3, "Rui Rocha", "rocha", "Concluo já, Sr. Presidente."),
    ]
    units = build_units(turns)
    spans = align_speeches([_iv(1, "RUI ROCHA", duration=110)], units)
    assert spans == {0: [0, 1, 2]}


def test_join_office_holders_by_name():
    turns = [
        _turn(1, "Luís Montenegro", "montenegro", LONG, role="Primeiro-Ministro"),
        _turn(2, "Ministra da Justiça", "", LONG, role="Ministra da Justiça"),
    ]
    ivs = [_iv(1, "Luís Montenegro", role="Primeiro-Ministro", duration=100),
           _iv(2, "Rita Alarcão Júdice", role="Ministra da Justiça", duration=100)]
    resolve_office_keys(turns, ivs)
    assert turns[1]["matchKey"] == "judice"
    spans = align_speeches(ivs, build_units(turns))
    assert spans == {0: [0], 1: [1]}


def test_join_run_on_name():
    turns = [_turn(1, "FilipeSousa", "filipesousa", LONG, party="JPP")]
    spans = align_speeches([_iv(1, "Filipe Sousa", duration=60)], build_units(turns))
    assert spans == {0: [0]}


def test_join_keeps_namesakes_apart():
    # Same surname, different first names: Filipa Pinto's speech must neither
    # take nor absorb Pedro Pinto's.
    turns = [
        _turn(1, "Pedro Pinto", "pinto", LONG, page=30),
        _turn(2, "Presidente", "presidente", "Tem a palavra.", chair=True, page=31),
        _turn(3, "Filipa Pinto", "pinto", LONG, page=66),
    ]
    spans = align_speeches([_iv(1, "FILIPA PINTO", duration=60)], build_units(turns))
    assert spans == {0: [2]}


def test_join_open_data_pages_pick_the_right_unit():
    turns = [
        _turn(1, "Pedro Pinto", "pinto", LONG, page=30),
        _turn(2, "Fabian Figueiredo", "figueiredo", LONG, page=31),
        _turn(3, "Pedro Pinto", "pinto", LONG, page=66),
    ]
    units = build_units(turns)
    # without pages the backtrack takes the latest equal key …
    assert align_speeches([_iv(1, "PEDRO PINTO", duration=60)], units) == {0: [2]}
    # … the open data record's pages anchor it to the first speech
    anchored = align_speeches([_iv(1, "PEDRO PINTO", duration=60, od={"pages": [30, 30]})],
                              units)
    assert anchored == {0: [0]}


# --------------------------------------------------------------------------- #
# Agenda items
# --------------------------------------------------------------------------- #

def test_assign_agenda_monotone_with_votes():
    items = agenda_items("Relatório Anual de Segurança Interna 2025 (RASI) / "
                         "Regras de utilização de bandeiras em edifícios públicos / Votações")
    assert len(items) == 3
    ivs = [
        _iv(1, "PRESIDENTE", role="Presidente", itype="Abertura da sessão",
            summary="Abre a sessão."),
        _iv(2, "LUÍS NEVES", summary="Abre o debate sobre o Relatório Anual de Segurança Interna."),
        _iv(3, "PRESIDENTE", role="Presidente", summary="Apela à Câmara."),
        _iv(4, "PAULA SANTOS", summary="Reapreciação do decreto sobre bandeiras em edifícios."),
        _iv(5, "PRESIDENTE", role="Presidente", itype="Votações", summary="Votações."),
        _iv(6, "PRESIDENTE", role="Presidente", itype="Encerramento da sessão",
            summary="Encerra a sessão."),
    ]
    assert assign_agenda(ivs, items) == [0, 0, 0, 1, 2, 2]


def test_assign_agenda_without_evidence_falls_back():
    items = agenda_items("Primeiro ponto / Votações")
    ivs = [_iv(1, "A"), _iv(2, "B", itype="Votações")]
    assert assign_agenda(ivs, items) is None
    assert assign_agenda(ivs, ["Só um ponto"]) is None


# --------------------------------------------------------------------------- #
# End-to-end merge → Stage 2 shape + schema validation
# --------------------------------------------------------------------------- #

def _media_doc():
    base = "https://av.parlamento.pt/content/hls/DAR/L17/SL1/A2026/M02/2026_02_25_059.mp4"

    def media(number, start, duration):
        return {"startOffset": start, "duration": duration,
                "videoFileURI": f"{base}/ClipFrom/{int(start * 1000)}/ClipTo/"
                                f"{int((start + duration) * 1000)}/index.m3u8",
                "audioFileURI": f"{base}/index.m3u8",
                "sourcePage": f"https://av.parlamento.pt/videos/Plenary/17/1/59/{number}"}

    return {
        "meta": {
            "session": "17-1-059", "parliament": "PT",
            "description": "Fixação da ordem do dia / Votações",
            "eventDate": "2026-02-25T14:58:00", "dateStart": "2026-02-25T14:58:00+00:00",
            "legislature": 17, "legislativeSession": 1, "meeting": 59,
            "processing": {"parse_media": "2026-02-25T00:00:00"},
        },
        "data": [
            {"number": 1, "interventionType": "Abertura da sessão", "speakerType": "Deputado",
             "speaker": "JOSÉ PEDRO AGUIAR BRANCO", "role": "Presidente",
             "affiliation": {"name": "Partido Social Democrata", "initials": "PSD"},
             "summary": "Abre a sessão.", **media(1, 8.0, 15.0)},
            {"number": 4, "interventionType": "Intervenção", "speakerType": "Deputado",
             "speaker": "FABIAN FIGUEIREDO", "role": "Deputado",
             "affiliation": {"name": "Bloco de Esquerda", "initials": "BE"},
             "summary": "Abre o debate da fixação da ordem do dia (BE).",
             "od": {"id": "505126", "personID": "7123", "pages": [3, 4]},
             **media(4, 681.7, 120.0)},
        ],
    }


def _proceedings_doc():
    return {
        "meta": {"session": "17-1-059", "parliament": "PT",
                 "processing": {"parse_proceedings": "2026-02-25T00:00:00"}},
        "data": [
            _turn(1, "Presidente", "presidente", "Está aberta a sessão.", chair=True, page=3),
            _turn(2, "Fabian Figueiredo", "figueiredo", LONG, party="BE", page=3),
            _turn(3, "Hugo Soares", "soares", "Muito bem!", party="PSD", page=3),
            _turn(4, "Fabian Figueiredo", "figueiredo", "… e continuamos.", party="BE",
                  page=4, continuation=True),
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

    # speech 1: chair, opening agenda type, no faction
    pres = data[0]
    assert pres["people"][0]["context"] == "president"
    assert pres["agendaItem"]["type"] == "opening"
    assert pres["agendaItem"]["nativeType"] == "PT-abertura"
    body = pres["textContents"][0]["textBody"]
    assert body[0]["speakerstatus"] == "president"
    assert body[0]["sentences"][0]["text"] == "Está aberta a sessão."

    # speech 2: deputy, faction from av, open data person id, whole speech
    fig = data[1]
    assert fig["people"][0]["faction"]["label"] == "BE"
    assert fig["people"][0]["originPersonID"] == "7123"
    assert fig["agendaItem"]["type"] == "regular"
    assert fig["agendaItem"]["title"] == "Fixação da ordem do dia"
    assert fig["media"]["additionalInformation"]["startOffset"] == 681.7
    body = fig["textContents"][0]["textBody"]
    assert [(b["type"], b["speakerstatus"]) for b in body] == [
        ("speech", "main-speaker"), ("comment", "speaker"), ("speech", "main-speaker")]
    assert body[0]["speaker"] == "FABIAN FIGUEIREDO"
    assert body[1]["sentences"] == [{"text": "(Hugo Soares [PSD]: Muito bem!)"}]
    assert body[2]["sentences"][0]["text"] == "… e continuamos."
    assert fig["debug"]["textTurnIndex"] == [2, 4]
    assert fig["debug"]["proceedingIndex"] == 2
    assert fig["debug"]["odPages"] == [3, 4]

    # per-speech dateStart = session start + offset, with the Lisbon offset
    assert data[0]["dateStart"] == "2026-02-25T14:58:08+00:00"
    assert data[1]["dateStart"].endswith("+00:00")
    assert data[0]["dateStart"] < data[1]["dateStart"]

    # full Stage 2 semantic validation: no errors
    findings = validate_stage2(doc, schema="full", semantic=True)
    errors = [f for f in findings if f["severity"] == "error"]
    assert errors == [], errors
