"""ES parser + scraper unit tests (no network).

Synthetic records model the Congreso open data interventions feed for
Diario DSCD-15-PL-184, which holds plenary session 178 (2026-05-07): the
Diario series is shared with the Diputación Permanente, so the plenary
session number comes from the MP4 path, not from the Diario number.
"""

from __future__ import annotations

import json
import logging

from optv.parliaments.ES.parsers.common import (
    collapse_ws, parse_orador, parse_video_url,
)
from optv.parliaments.ES.merger.merge_session import merge_text_contents, norm_surname
from optv.parliaments.ES.parsers.language import paragraph_language, split_language_passes
from optv.parliaments.ES.parsers.media2json import parse_intervention, parse_media_data
from optv.parliaments.ES.parsers.proceedings2json import parse_proceedings_html
from optv.parliaments.ES.scraper import fetch_interventions, fetch_search
from optv.parliaments.ES.scraper.fetch_interventions import split_pleno_sessions, write_raw_session
from optv.parliaments.ES.scraper.fetch_search import to_feed_record

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
# scraper/fetch_search.py: intervention search fallback
# --------------------------------------------------------------------------- #

def _entry(doc: int = 16, clips=((742897, "11:07:00", "11:25:07"),)) -> dict:
    """One result of the intervention search list (filtrarListado), trimmed."""
    video = {"legislatura": "15", "secuencia": "81", "organo": "400"}
    for k, (iid, start, end) in enumerate(clips, 1):
        mp4 = (f"https://static.congreso.es/audiovisual/video/leg15/400/15_000400_081/"
               f"cortes/15_000400_081_0_25504_{iid}.mp4")
        video.update({f"id{k:02d}": str(iid), f"inicio{k:02d}": start, f"fin{k:02d}": end,
                      f"enlace_descarga{2 * k:02d}": mp4,
                      f"enlace_emision{2 * k:02d}": f"https://app.congreso.es/v1/15{iid}I"})
    return {
        "doc": str(doc), "fecha": "20241127", "legislatura": "XV",
        "orador": "Aizpurua Arzallus, Mertxe (GEH Bildu)", "cargo_orador": "Diputada",
        "sesion": {"nombre_sesion": "Pleno", "idsesion": "81", "fase1": "Celebración"},
        "video_intervencion": video,
        "iniciativa": {"objeto_iniciativa": "Solicitud de comparecencia urgente",
                       "enlace_expediente": {"id_iniciativa": "210/000057/0000"},
                       "autores": {"autor01": {"orden": "1", "nombre": "Tellado Filgueira, Miguel (GP)"}}},
        "edia": "(DSCD-15-PL-84.CODI.)#(Página29)",
        "pdia": "CONG/DS/PL/DSCD-15-PL-84.PDF#page=29",
    }


def test_search_entry_maps_to_open_data_record():
    rec = to_feed_record(_entry(), 15)
    assert rec["SESION"] == "27/11/2024"
    assert (rec["ORGANO"], rec["FASE"], rec["TIPOINTERVENCION"]) == ("Pleno", "Celebración", "Intervención")
    assert (rec["INICIOINTERVENCION"], rec["FININTERVENCION"]) == ("11:07:00", "11:25:07")
    assert rec["NUMEXPEDIENTE"] == "210/000057/0000"
    assert rec["ENLACEPDF"] == ("https://www.congreso.es/public_oficiales/L15/"
                                "CONG/DS/PL/DSCD-15-PL-84.PDF#page=29")
    sessions = split_pleno_sessions([rec], 15)
    assert (sessions["15081"]["number"], sessions["15081"]["diario"]) == (81, 84)


def test_search_entry_with_two_clips():
    rec = to_feed_record(_entry(clips=((742935, "14:26:17", "14:26:46"),
                                       (742937, "14:26:53", "14:29:29"))), 15)
    assert [v["iid"] for v in map(parse_video_url, rec["ENLACEDESCARGADIRECTA"].split())] == [
        "742935", "742937"]
    # As in the open data, the record's times are the first clip's.
    assert (rec["INICIOINTERVENCION"], rec["FININTERVENCION"]) == ("14:26:17", "14:26:46")


def test_search_times_are_not_coarse():
    [item] = parse_intervention(to_feed_record(_entry(), 15), 15, 81)
    assert item["dateStart"] == "2024-11-27T11:07:00+01:00"
    assert item["media"]["duration"] == 1087.0
    assert "coarseTiming" not in item["debug"]
    [coarse] = parse_intervention(_rec(184, PLENO_MP4), 15, 178)
    assert coarse["debug"]["coarseTiming"] is True


def test_feed_failure_falls_back_to_search(tmp_path, monkeypatch, caplog):
    pages = {1: {"intervenciones_encontradas": "2",
                 "lista_intervenciones": {"0": _entry(16), "1": _entry(17, ((742898, "11:25:07", "11:30:00"),))}}}
    calls = []

    def fake_page(period, date_from, date_to, page, retry_count=0):
        calls.append((date_from, page))
        return pages[page]

    monkeypatch.setattr(fetch_interventions, "download_feed", lambda retry_count=0: None)
    monkeypatch.setattr(fetch_search, "fetch_list_page", fake_page)
    with caplog.at_level(logging.WARNING):
        seen = fetch_interventions.update_interventions_period(15, tmp_path)
    assert seen == {"15081": "DSCD-15-PL-84"}
    assert any("falling back to the intervention search" in r.getMessage() for r in caplog.records)
    raw = json.loads((tmp_path / "raw-15081-media.json").read_text())
    assert raw["meta"]["source"] == "search"
    assert len(raw["interventions"]) == 2
    # The next fallback run lists from the newest sitting on file.
    fetch_interventions.update_interventions_period(15, tmp_path)
    assert calls[-1] == ("27/11/2024", 1)


def test_search_records_never_replace_open_data_files(tmp_path):
    records = [_rec(184, PLENO_MP4)]
    assert write_raw_session(tmp_path, "15178", 15, 178, 184, records)
    assert not write_raw_session(tmp_path, "15178", 15, 178, 184, records + records,
                                 source="search")
    # The open data replaces a fallback file.
    assert write_raw_session(tmp_path, "15179", 15, 179, 185, records, source="search")
    assert write_raw_session(tmp_path, "15179", 15, 179, 185, records + records)
    meta = json.loads((tmp_path / "raw-15179-media.json").read_text())["meta"]
    assert meta["source"] == "opendata"


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


def test_joint_debate_copies_become_one_item():
    # The feed lists an intervention in a joint debate once per initiative:
    # same MP4, speaker and times, different initiative.
    first = dict(_rec(84, _clip(742890, 81), orador="Sánchez Pérez-Castejón, Pedro (GS)",
                      start="09:03", end="09:42"),
                 NUMEXPEDIENTE="210/000058/0000", OBJETOINICIATIVA="Comparecencia a petición propia")
    second = dict(first, NUMEXPEDIENTE="210/000057/0000",
                  OBJETOINICIATIVA="Comparecencia urgente\nsobre la DANA")
    reply = _rec(84, _clip(742891, 81), orador="Tellado Filgueira, Miguel (GP)",
                 start="09:42", end="10:00")
    raw = {"meta": {"session": "15081", "period": 15, "sessionNumber": 81},
           "interventions": [first, second, reply]}
    data = parse_media_data(raw)["data"]
    assert [d["media"]["originMediaID"][-6:] for d in data] == ["742890", "742891"]
    # The lowest expediente is the agenda item, whatever the feed order.
    assert data[0]["agendaItem"]["officialTitle"] == "Comparecencia urgente sobre la DANA"
    assert data[0]["debug"]["jointInitiatives"] == [
        {"numExpediente": "210/000058/0000", "title": "Comparecencia a petición propia"}]
    assert "jointInitiatives" not in data[1]["debug"]


# --------------------------------------------------------------------------- #
# parsers/language.py
# --------------------------------------------------------------------------- #

CA = ("Senyor ministre, nosaltres sí que considerem pertinent i necessari l'acte "
      "parlamentari que estem fent avui, perquè també és la nostra feina.")
CA_ES = ("Señor ministro, nosotros sí consideramos pertinente y necesario el acto "
         "parlamentario de hoy, porque también es nuestro trabajo.")
CA2 = "Per tant, senyor Albares, ja venim de lluny i ja sabem de què va això."
CA2_ES = "Por lo tanto, señor Albares, ya venimos de lejos y ya sabemos de qué va esto."
ES_ONLY = ("Ustedes, señores socialistas, nos hacen decidir entre el bloqueo y la "
           "corrupción, y eso no lo vamos a aceptar nunca más.")


def test_paragraph_language():
    assert paragraph_language(CA) == "ca"
    assert paragraph_language(CA_ES) == "es"
    assert paragraph_language("Moitas grazas, señora presidenta.") == "gl"
    assert paragraph_language("Eskerrik asko, mahaiburu andrea.") == "eu"
    assert paragraph_language("Señor ministro.") is None


def test_split_whole_turn_original_then_translation():
    passes = split_language_passes(["Gràcies.", CA, CA2, "Gracias.", CA_ES, CA2_ES])
    assert passes == ("ca", ["Gràcies.", CA, CA2], ["Gracias.", CA_ES, CA2_ES])


def test_split_interleaved_keeps_spanish_passages_in_both():
    passes = split_language_passes([CA, CA_ES, ES_ONLY, CA2, CA2_ES])
    assert passes == ("ca", [CA, ES_ONLY, CA2], [CA_ES, ES_ONLY, CA2_ES])


def test_no_split_for_spanish_or_untranslated_turns():
    assert split_language_passes([CA_ES, ES_ONLY]) is None
    # Catalan with no Spanish version after it: the Diario carries only the
    # text as spoken.
    assert split_language_passes([CA, CA2, "Muchas gracias."]) is None


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


def _turn_html(body: str) -> str:
    return _diario_html().replace(
        "Gracias, presidenta. Señorías, comparezco a petición propia.", body)


def test_page_label_is_dropped_and_mid_sentence_break_joins():
    body = ("Gracias, presidenta. Señorías, la situación es "
            "<br><br><br /><p style='text-align:center'><a name='(Página4)'>"
            "<b>Página 4</b></a></p><br /> <br><br>muy grave.")
    out = parse_proceedings_html(_turn_html(body), 178, 15, "DSCD-15-PL-184", source_uri="x")
    [tc] = out["data"][0]["textContents"]
    assert tc["textBody"][0]["text"] == (
        "Gracias, presidenta. Señorías, la situación es muy grave.")


def test_co_official_turn_gets_two_renderings():
    body = "<br><br>".join([CA, CA2, CA_ES, CA2_ES])
    out = parse_proceedings_html(_turn_html(body), 178, 15, "DSCD-15-PL-184", source_uri="x")
    speech = out["data"][0]
    assert speech["originalLanguage"] == "ca"
    spoken, spanish = speech["textContents"]
    assert (spoken["language"], spanish["language"]) == ("ca", "es")
    assert spoken["textBody"][0]["text"] == f"{CA} {CA2}"
    assert spanish["textBody"][0]["text"] == f"{CA_ES} {CA2_ES}"


# --------------------------------------------------------------------------- #
# merger/merge_session.py
# --------------------------------------------------------------------------- #

def _tc(language: str, text: str) -> dict:
    return {"type": "proceedings", "language": language, "originTextID": text[:5],
            "creator": "c", "license": "l",
            "textBody": [{"type": "speech", "text": text, "sentences": [{"text": text}]}]}


def test_merged_turns_share_one_entry_per_rendering():
    # Body + closing of one speech (split by a chair aside): one Spanish entry,
    # since the platform stores one text per (type, language).
    tcs, language = merge_text_contents([{"textContents": [_tc("es", "Cuerpo.")]},
                                         {"textContents": [_tc("es", "Cierre.")]}])
    assert language is None
    assert [t["language"] for t in tcs] == ["es"]
    assert [b["text"] for b in tcs[0]["textBody"]] == ["Cuerpo.", "Cierre."]


def test_merged_co_official_turns_keep_spoken_and_spanish_renderings():
    tcs, language = merge_text_contents([
        {"textContents": [_tc("ca", "Cos."), _tc("es", "Cuerpo.")]},
        {"textContents": [_tc("es", "Cierre.")]},   # closing spoken in Spanish
    ])
    assert language == "ca"
    assert [t["language"] for t in tcs] == ["ca", "es"]
    assert [b["text"] for b in tcs[0]["textBody"]] == ["Cos.", "Cierre."]
    assert [b["text"] for b in tcs[1]["textBody"]] == ["Cuerpo.", "Cierre."]


def test_norm_surname_folds_hyphens():
    assert (norm_surname({"people": [{"lastname": "Sánchez Pérez Castejón"}]})
            == norm_surname({"people": [{"lastname": "Sánchez Pérez-Castejón"}]}))
