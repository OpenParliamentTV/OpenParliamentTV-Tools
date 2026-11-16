"""ES parser + scraper unit tests (no network).

Synthetic records model the Congreso open data interventions feed for
Diario DSCD-15-PL-184, which holds plenary session 178 (2026-05-07): the
Diario series is shared with the Diputación Permanente, so the plenary
session number comes from the MP4 path, not from the Diario number.
"""

from __future__ import annotations

import logging

from optv.parliaments.ES.parsers.common import (
    collapse_ws, parse_orador, parse_video_url,
)
from optv.parliaments.ES.parsers.media2json import parse_intervention, parse_media_data
from optv.parliaments.ES.parsers.proceedings2json import parse_proceedings_html
from optv.parliaments.ES.scraper.fetch_interventions import split_pleno_sessions

PLENO_MP4 = ("https://www.congreso.es/audiovisual/video/leg15/400/15_000400_178/"
             "cortes/15_000400_178_0_27272_772999.mp4")
DP_MP4 = ("https://www.congreso.es/audiovisual/video/leg15/500/15_000500_002/"
          "cortes/15_000500_002_0_24521_727481.mp4")
PLACEHOLDER_MP4 = "https://www.congreso.es/audiovisual/video/leg/400//cortes/.mp4"
PLACEHOLDER_MP4_NEW = ("https://www.congreso.es/audiovisual/video/leg__/400/__10000000/"
                       "cortes/__10000000.mp4")


def _clip(iid: int, session: int = 12) -> str:
    return (f"https://www.congreso.es/audiovisual/video/leg15/400/15_000400_{session:03d}/"
            f"cortes/15_000400_{session:03d}_0_24414_{iid}.mp4")


def _rec(diario: int, mp4: str, leg: int = 15, organo: str = "Pleno",
         tipo: str = "Intervención", cargo: str = "Diputado",
         orador: str = "Albares Bueno, José Manuel",
         start: str = "11:04", end: str = "11:38") -> dict:
    return {
        "SESION": "07/05/2026",
        "ORGANO": organo,
        "FASE": "Celebración",
        "TIPOINTERVENCION": tipo,
        "OBJETOINICIATIVA": ("Solicitud de comparecencia urgente del Ministro de Asuntos\n"
                             "Exteriores, Unión Europea y Cooperación ante el Pleno"),
        "ORADOR": orador,
        "CARGOORADOR": cargo,
        "INICIOINTERVENCION": start,
        "FININTERVENCION": end,
        "ENLACEDIFERIDO": ("https://app.congreso.es/v1/1572999I "
                           "https://app.congreso.es/v1/15772999I"),
        "ENLACEDESCARGADIRECTA": mp4,
        "ENLACEPDF": (f"https://www.congreso.es/public_oficiales/L{leg}/CONG/DS/PL/"
                      f"DSCD-{leg}-PL-{diario}.PDF#page=3"),
    }


# --------------------------------------------------------------------------- #
# parsers/common.py
# --------------------------------------------------------------------------- #

def test_parse_video_url():
    assert parse_video_url(PLENO_MP4) == {
        "leg": 15, "organ": 400, "session": 178, "iid": "772999",
        "stem": "15_000400_178_0_27272_772999"}
    dp = parse_video_url(DP_MP4)
    assert (dp["organ"], dp["session"]) == (500, 2)
    assert parse_video_url(PLACEHOLDER_MP4) is None
    assert parse_video_url(PLACEHOLDER_MP4_NEW) is None
    assert parse_video_url("") is None
    assert parse_video_url(None) is None


def test_collapse_ws_and_role():
    assert collapse_ws(" a\n b  c ") == "a b c"
    person = parse_orador(
        "Cuerpo Caballero, Carlos",
        "Vicepresidente Primero del Gobierno y Ministro de Economía, Comercio\ny Empresa")
    assert person["role"] == ("Vicepresidente Primero del Gobierno y Ministro de "
                              "Economía, Comercio y Empresa")


# --------------------------------------------------------------------------- #
# scraper/fetch_interventions.py: session grouping
# --------------------------------------------------------------------------- #

def test_split_pleno_sessions_uses_plenary_number():
    records = [
        _rec(184, PLENO_MP4),
        _rec(184, PLACEHOLDER_MP4),              # no video, stays in the session
        _rec(184, f"{_clip(772998, 178)}\n{_clip(773000, 178)}"),  # two clips
        _rec(184, PLENO_MP4, tipo="Votación"),   # dropped
        _rec(19, DP_MP4, organo="Diputación Permanente"),   # DP Diario: dropped
        _rec(92, PLACEHOLDER_MP4),               # plenary Diario without video: dropped
        _rec(5, PLENO_MP4, leg=14),              # other legislature: dropped
    ]
    sessions = split_pleno_sessions(records, 15)
    assert list(sessions) == ["15178"]
    sess = sessions["15178"]
    assert (sess["number"], sess["diario"]) == (178, 184)
    assert len(sess["records"]) == 3


def test_split_pleno_sessions_duplicate_number_keeps_first():
    sessions = split_pleno_sessions([_rec(184, PLENO_MP4), _rec(185, PLENO_MP4)], 15)
    assert list(sessions) == ["15178"]
    assert sessions["15178"]["diario"] == 184


# --------------------------------------------------------------------------- #
# parsers/media2json.py
# --------------------------------------------------------------------------- #

def test_parse_intervention_links_and_titles():
    [item] = parse_intervention(_rec(184, PLENO_MP4, cargo="Ministro de Asuntos\nExteriores"),
                                15, 178)
    assert item["session"] == {"number": 178}
    media = item["media"]
    # Built from the intervention id, not the feed's broken/doubled ENLACEDIFERIDO.
    assert media["sourcePage"] == "https://app.congreso.es/v1/15772999I"
    assert media["videoFileURI"] == PLENO_MP4.replace("https://www.", "https://static.")
    assert media["originMediaID"] == "15_000400_178_0_27272_772999"
    assert "\n" not in item["agendaItem"]["officialTitle"]
    assert "\n" not in item["agendaItem"]["title"]
    assert item["people"][0]["role"] == "Ministro de Asuntos Exteriores"


def test_parse_intervention_without_video():
    assert parse_intervention(_rec(184, PLACEHOLDER_MP4), 15, 178) == []
    assert parse_intervention(_rec(184, PLACEHOLDER_MP4_NEW), 15, 178) == []


def test_multi_clip_record_becomes_one_item_per_clip_in_sequence():
    # Question time: the minister's answer and rejoinder are one record with
    # two clips; the questioner's reply (726477) was recorded between them.
    minister = _rec(13, f"{_clip(726476)}\n{_clip(726478)}",
                    orador="Calviño Santamaría, Nadia", start="09:04", end="09:05")
    questioner = _rec(13, _clip(726477), orador="Gamarra Ruiz-Clavijo, Concepción (GP)",
                      start="09:05", end="09:07")
    items = parse_intervention(minister, 15, 12)
    assert [i["media"]["sourcePage"] for i in items] == [
        "https://app.congreso.es/v1/15726476I", "https://app.congreso.es/v1/15726478I"]
    assert [i["debug"]["clipIndex"] for i in items] == [0, 1]
    assert all("\n" not in i["media"]["videoFileURI"] for i in items)

    raw = {"meta": {"session": "15012", "period": 15, "sessionNumber": 12},
           "interventions": [minister, questioner]}
    data = parse_media_data(raw)["data"]
    assert [d["media"]["originMediaID"].rsplit("_", 1)[1] for d in data] == [
        "726476", "726477", "726478"]
    assert [d["speechIndex"] for d in data] == [1, 2, 3]
    # The rejoinder clip starts no earlier than the turn before it.
    assert data[2]["dateStart"] == data[1]["dateStart"]


# --------------------------------------------------------------------------- #
# parsers/proceedings2json.py
# --------------------------------------------------------------------------- #

def _diario_html(cve: str = "DSCD-15-PL-184") -> str:
    # Layout of the texto íntegro view: cve + cabecera, then textoCompleto.
    return (
        '<div class="textoIntegro"><div><i>DS. Congreso de los Diputados, Pleno y '
        f'Dip. Perm., núm. 184, de 07/05/2026</i>cve: {cve}</div>'
        '<p class="cabecera2"><br>Año 2026 XV LEGISLATURA Núm. 184 <br>'
        'Sesión plenaria núm. 178 <br>celebrada el jueves, <br>7 de mayo de 2026</p>'
        '<p class="textoCompleto">'
        "<a name='(Página3)'></a>"
        'El señor ALBARES BUENO (ministro de Asuntos Exteriores, Unión Europea y '
        'Cooperación): Gracias, presidenta. Señorías, comparezco a petición propia.'
        '</p></div>'
    )


_DIARIO_HTML = _diario_html()


def test_parse_proceedings_uses_diario_id(caplog):
    with caplog.at_level(logging.WARNING):
        out = parse_proceedings_html(_DIARIO_HTML, 178, 15, "DSCD-15-PL-184", source_uri="x")
    assert out["meta"]["session"] == "15178"
    speech = out["data"][0]
    assert speech["session"] == {"number": 178}
    assert speech["originID"] == "DSCD-15-PL-184-1"
    assert speech["textContents"][0]["sourceURI"] == (
        "https://www.congreso.es/public_oficiales/L15/CONG/DS/PL/DSCD-15-PL-184.PDF#page=3")
    assert not caplog.records


def test_parse_proceedings_cve_fallback_drops_correction_suffix(caplog):
    html = _diario_html("DSCD-15-PL-184-C1")
    with caplog.at_level(logging.WARNING):
        with_id = parse_proceedings_html(html, 178, 15, "DSCD-15-PL-184", source_uri="x")
        without_id = parse_proceedings_html(html, 178, 15, "", source_uri="x")
    assert with_id["data"][0]["originID"] == "DSCD-15-PL-184-1"
    assert without_id["data"][0]["originID"] == "DSCD-15-PL-184-1"
    assert not caplog.records


def test_parse_proceedings_warns_on_header_mismatch(caplog):
    with caplog.at_level(logging.WARNING):
        parse_proceedings_html(_DIARIO_HTML, 184, 15, "DSCD-15-PL-184", source_uri="x")
    assert any("header gives plenary session 178" in r.getMessage() for r in caplog.records)
