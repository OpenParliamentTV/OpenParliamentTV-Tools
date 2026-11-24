"""IT session keys, scraper helpers and the two parsers, on synthetic fixtures."""

from __future__ import annotations

import argparse
import json

import pytest
from lxml import etree

from optv.parliaments.IT.common import Config, in_scope, parse_session_key, session_key
from optv.parliaments.IT.parsers import media2json as m2j
from optv.parliaments.IT.parsers import proceedings2json as p2j
from optv.parliaments.IT.parsers.names import person, person_key, split_name
from optv.parliaments.IT.scraper import fetch
from optv.shared.agenda_types import (
    CORE_BRIEFING, CORE_BUDGET, CORE_CLOSING, CORE_OPENING, CORE_PROCEDURAL, CORE_QA, CORE_REGULAR,
    CORE_RULES_OF_PROCEDURE, CORE_TYPES, CORE_VOTING, classify_it,
)

from .it_fixtures import (
    ARCHIVE_ENTRY, SESSION, StubNLP, archive_bytes, page_bytes, parsed_media, parsed_proceedings, record_bytes,
)


@pytest.fixture(scope="module")
def media() -> dict:
    return parsed_media()


@pytest.fixture(scope="module")
def proceedings() -> dict:
    return parsed_proceedings()


def _entry(media: dict, entry_id: str) -> dict:
    return next(e for e in media["data"] if e["id"] == entry_id)


def _intervention(proceedings: dict, intervention_id: str) -> dict:
    return next(i for i in proceedings["data"] if i["id"] == intervention_id)


def _sentences(intervention: dict, kind: str) -> list[str]:
    return [s for b in intervention["blocks"] if b["kind"] == kind for s in b["sentences"]]


# ---- session keys ----

def test_session_key_round_trip():
    assert session_key(19, 731) == "190731"
    assert parse_session_key("190731") == (19, 731)
    assert parse_session_key(session_key(19, 7)) == (19, 7)


@pytest.mark.parametrize("bad", ["19731", "19-0731", "1907310", "abcdef", ""])
def test_parse_session_key_rejects_other_forms(bad):
    with pytest.raises(ValueError):
        parse_session_key(bad)


def test_in_scope_applies_period_and_limit_session():
    args = argparse.Namespace(period=19, limit_to_period=True, limit_session="")
    assert in_scope(args, "190731")
    assert not in_scope(args, "180731")
    assert not in_scope(args, "garbage")
    args.limit_session = r"^(190731|190744)$"
    assert in_scope(args, "190744")
    assert not in_scope(args, "190745")
    args.limit_to_period = False
    args.limit_session = ""
    assert in_scope(args, "180731")


def test_config_names_the_raw_files_of_a_session(tmp_path):
    config = Config(tmp_path)
    assert config.raw_event("190731").name == "190731-evento.html"
    assert config.raw_archive_entry("190731").name == "190731-archivio.json"
    assert config.raw_record("190731") == tmp_path / "original" / "proceedings" / "190731-stenografico.xml"
    config.dir("media", create=True)
    config.raw_event("190731").write_text("x")
    config.raw_archive_entry("190731").write_text("{}")
    assert config.sessions() == ["190731"]


# ---- names ----

@pytest.mark.parametrize("written,first,last", [
    ("PICCOLOTTI Elisabetta", "Elisabetta", "Piccolotti"),
    ("DELLA VEDOVA Benedetto", "Benedetto", "Della Vedova"),
    ("D'ALESSIO Antonio", "Antonio", "D'Alessio"),
    ("CALDERONE Tommaso Antonino", "Tommaso Antonino", "Calderone"),
    ("CANTÙ Maria", "Maria", "Cantù"),
    ("ROSSI", "", "Rossi"),
    ("", "", ""),
])
def test_split_name(written, first, last):
    assert split_name(written) == (first, last)


def test_person_carries_what_is_known():
    assert person("DE LUCA Anna", person_id="300003", role="Deputata", group="PARTITO UNO") == {
        "label": "Anna De Luca", "firstname": "Anna", "lastname": "De Luca", "role": "Deputata",
        "faction": {"label": "PARTITO UNO"}, "originPersonID": "300003"}
    assert person("ROSSI Maria") == {"label": "Maria Rossi", "firstname": "Maria", "lastname": "Rossi"}
    assert person_key("  rossi   Maria ") == "ROSSI MARIA"


# ---- agenda types ----

@pytest.mark.parametrize("title,phase,subject,native,core", [
    ("Lettura Verbale", None, None, "IT-lettura_verbale", CORE_OPENING),
    ("Missioni", None, None, "IT-comunicazioni", CORE_PROCEDURAL),
    ("Preavviso di Votazioni Elettroniche", None, None, "IT-comunicazioni", CORE_PROCEDURAL),
    ("Sull'ordine dei lavori", None, None, "IT-ordine_dei_lavori", CORE_PROCEDURAL),
    ("Sui lavori dell'Assemblea", None, None, "IT-ordine_dei_lavori", CORE_PROCEDURAL),
    ("Per un richiamo al Regolamento", None, None, "IT-richiamo_al_regolamento", CORE_RULES_OF_PROCEDURE),
    ("Ordine del giorno della prossima seduta", None, None, "IT-ordine_del_giorno", CORE_CLOSING),
    ("Interpellanze urgenti (Svolgimento)", "Risposta", "Iniziative in materia di esempi", "IT-interpellanze", CORE_QA),
    ("Interrogazioni a risposta immediata (Svolgimento)", None, None, "IT-question_time", CORE_QA),
    ("Informativa urgente del Governo", None, None, "IT-informativa", CORE_BRIEFING),
    ("Disegno di legge", None, "Bilancio di previsione dello Stato per l'anno finanziario 2028", "IT-bilancio", CORE_BUDGET),
    ("Interventi di fine seduta", None, None, "IT-interventi_di_fine_seduta", CORE_REGULAR),
    ("Disegno di legge: Disposizioni in materia di esempi", "Dichiarazioni di voto finale - A.C. 100", None, None, CORE_REGULAR),
    ("Disegno di legge: Disposizioni in materia di esempi", "Votazione finale ed approvazione - A.C. 100", None,
     "IT-votazione", CORE_VOTING),
    ("Mozioni", "Votazioni", None, "IT-votazione", CORE_VOTING),
    (None, None, None, None, CORE_REGULAR),
])
def test_classify_it(title, phase, subject, native, core):
    assert classify_it(title, phase, subject) == (native, core)
    assert core in CORE_TYPES


# ---- scraper ----

class FakeSource:
    """Answers archive queries from a list of sittings, 20 per page, and counts the requests."""

    def __init__(self, sittings: list[int]):
        self.sittings = sittings
        self.requests = 0
        self.queries: list[dict] = []

    def post(self, url, data):
        self.requests += 1
        self.queries.append(dict(data))
        keyword = data.get("filter_label_txt_1")
        matching = [n for n in self.sittings if not keyword or keyword.split()[-1] in str(n)]
        page = int(data["page"])
        items = "".join(
            f"<item><id>{9000 + n}</id><titolo>AULA, Seduta {n} - Esempio</titolo>"
            f"<dataora_evento>2027-01-01 10:00:00</dataora_evento><numero_seduta>{n}</numero_seduta>"
            f"<legislatura>19</legislatura><dataora_fine>2027-01-01 12:00:00</dataora_fine></item>"
            for n in matching[(page - 1) * fetch.PAGE_SIZE:page * fetch.PAGE_SIZE])
        return (f"<result><tot_records>{len(matching)}</tot_records><list>{items}</list></result>").encode()


def test_parse_archive_lists_the_numbered_sittings():
    total, sittings = fetch.parse_archive(archive_bytes())
    assert total == 3
    assert [s["sitting"] for s in sittings] == [901, 900]          # the joint sitting has no number
    assert sittings[1] == {"event": 99001, "legislature": 19, "sitting": 900,
                           "title": "AULA, Seduta 900 - Esempio di una seduta",
                           "dateStart": "2027-03-02 10:00:00", "dateEnd": "2027-03-02 10:21:45"}


def test_find_sitting_asks_for_the_number_and_takes_the_exact_match():
    source = FakeSource(list(range(750, 0, -1)))
    found = fetch.find_sitting(source, 19, 74)                     # "Seduta 74" also matches 740 to 749
    assert found["sitting"] == 74 and found["event"] == 9074
    assert source.queries[0] == {"filter_label_sez_1": "1", "filter_label_leg_19": "19",
                                 "filter_label_txt_1": "Seduta 74", "page": "1"}
    assert source.requests == 1


def test_find_sitting_pages_on_until_the_matches_are_exhausted():
    source = FakeSource(list(range(750, 0, -1)))
    assert fetch.find_sitting(source, 19, 7)["sitting"] == 7       # every number with a 7 in it matches: 196 sittings
    assert source.requests == 10
    assert fetch.find_sitting(FakeSource([750, 749]), 19, 5) is None


def test_list_sittings_stops_at_the_first_page_that_is_known():
    source = FakeSource(list(range(60, 0, -1)))
    listed = fetch.list_sittings(source, 19, known=lambda session: int(session[2:]) <= 40)
    assert source.requests == 2                                     # page 2 holds 40 to 21, all known
    assert sorted(listed)[0] == "190021" and sorted(listed)[-1] == "190060"
    whole = FakeSource(list(range(60, 0, -1)))
    assert len(fetch.list_sittings(whole, 19)) == 60 and whole.requests == 3


def test_page_and_record_are_checked_against_the_sitting():
    assert fetch.page_sitting(page_bytes()) == (19, 900)
    assert fetch.page_sitting(b"<html>another page</html>") is None
    assert fetch.is_record_of(record_bytes(), 19, 900)
    assert not fetch.is_record_of(record_bytes(), 19, 901)
    assert not fetch.is_record_of(b"<html>not found</html>", 19, 900)
    assert not fetch.is_record_of(b'<seduta legislatura="19" numero="900"/>', 19, 900)


class PageSource:
    def __init__(self, body: bytes):
        self.body = body
        self.requests = 0

    def get(self, url):
        self.requests += 1
        return self.body


def test_fetch_sitting_writes_page_record_and_entry(tmp_path):
    config = Config(tmp_path)
    entry = {k: v for k, v in ARCHIVE_ENTRY.items() if k not in ("session", "record", "recordLinked")}
    assert not fetch.is_complete(config, SESSION)
    assert fetch.fetch_sitting(config, SESSION, entry, PageSource(page_bytes()), PageSource(record_bytes()))
    assert config.raw_event(SESSION).read_bytes() == page_bytes()
    assert config.raw_record(SESSION).read_bytes() == record_bytes()
    stored = json.loads(config.raw_archive_entry(SESSION).read_text())
    assert stored["session"] == SESSION and stored["record"] and stored["recordLinked"]
    assert fetch.is_complete(config, SESSION)


def test_fetch_sitting_without_a_record_stays_incomplete(tmp_path):
    config = Config(tmp_path)
    entry = {k: v for k, v in ARCHIVE_ENTRY.items() if k not in ("session", "record", "recordLinked")}
    assert fetch.fetch_sitting(config, SESSION, entry, PageSource(page_bytes()), PageSource(b"<html/>"))
    assert not config.raw_record(SESSION).exists()
    assert json.loads(config.raw_archive_entry(SESSION).read_text())["record"] is False
    assert not fetch.is_complete(config, SESSION)


def test_fetch_sitting_skips_a_page_of_another_sitting_and_a_live_one(tmp_path):
    config = Config(tmp_path)
    entry = {k: v for k, v in ARCHIVE_ENTRY.items() if k not in ("session", "record", "recordLinked")}
    documenti = PageSource(record_bytes())
    assert not fetch.fetch_sitting(config, "190901", entry, PageSource(page_bytes()), documenti)
    live = page_bytes().replace(b"var is_live = 0;", b"var is_live = 1;")
    assert not fetch.fetch_sitting(config, SESSION, entry, PageSource(live), documenti)
    assert documenti.requests == 0
    assert not config.raw_event(SESSION).exists()


# ---- media parser ----

def test_media_meta(media):
    meta = media["meta"]
    assert (meta["session"], meta["legislature"], meta["sitting"], meta["event"]) == (SESSION, 19, 900, 99001)
    assert meta["dateStart"] == "2027-03-02T10:00:00+01:00"
    assert meta["dateEnd"] == "2027-03-02T10:21:45+01:00"
    assert meta["sourcePage"] == "https://webtv.camera.it/evento/99001"
    assert meta["parts"] == [
        {"id": "70001", "seq": 0, "url": "https://video.example.org/assemblea19/L19_S0900_parte1.mp4"},
        {"id": "70002", "seq": 1, "url": "https://video.example.org/assemblea19/L19_S0900_parte2.mp4"}]


def test_every_intervention_of_the_page_is_an_entry(media):
    assert [e["id"] for e in media["data"]] == [str(n) for n in range(500001, 500012)]
    assert _entry(media, "500006") == {
        "id": "500006", "recordId": "tit00020.sub00010.int00020", "part": "70002", "start": 30, "end": 60,
        "clock": "2027-03-02T10:19:50+01:00", "name": "DE LUCA Anna", "role": "Deputata", "group": "PARTITO UNO",
        "personId": "300003", "titleId": "800007"}


def test_entries_without_a_link_to_the_record_and_without_an_end(media):
    assert _entry(media, "500008")["recordId"] is None
    assert _entry(media, "500011")["end"] is None


def test_role_and_group_come_from_the_link_text(media):
    verdi = _entry(media, "500007")
    assert (verdi["role"], verdi["group"]) == ("Deputato", "NOI MODERATI (NOI CON L'ITALIA, CORAGGIO ITALIA)-CENTRO")
    neri = _entry(media, "500010")
    assert (neri["role"], neri["group"], neri["personId"]) == ("Sottosegretaria di Stato per la Giustizia", None, None)
    assert m2j.split_link_text("Vice Presidente ROSSI Maria", "ROSSI Maria") == ("Vice Presidente", None)
    assert m2j.split_link_text("Qualcosa", "ROSSI Maria") == (None, None)


def test_titles_form_a_tree(media):
    titles = media["meta"]["titles"]
    assert titles["800007"] == {"title": "Dichiarazioni di voto finale - A.C. 100", "parent": "800005"}
    assert titles["800005"]["parent"] == "800004"
    assert titles["800004"]["parent"] is None


def test_local_time_follows_rome_summer_time():
    assert m2j.local_time("2026-10-22 09:56:17") == "2026-10-22T09:56:17+02:00"
    assert m2j.local_time("2026-11-13 09:32:11") == "2026-11-13T09:32:11+01:00"
    assert m2j.local_time("") is None and m2j.local_time("yesterday") is None


def test_record_id_is_written_the_records_way():
    assert m2j.record_id("tit00040_sub00030_int00180") == "tit00040.sub00030.int00180"
    assert m2j.record_id("") is None and m2j.record_id(None) is None


# ---- record parser ----

def _runs(markup: str) -> list[p2j.Run]:
    return p2j._runs(etree.fromstring(f"<p>{markup}</p>"))


def test_a_parenthesis_in_italics_is_a_comment():
    pieces = p2j.split_comments(_runs('La Camera approva <emphasis type="italic">(Vedi votazione n. 1)</emphasis>.'))
    assert pieces == [("speech", "La Camera approva."), ("comment", "(Vedi votazione n. 1)")]


def test_a_comment_runs_to_its_closing_parenthesis_across_formatting():
    pieces = p2j.split_comments(_runs(
        'prosegua <emphasis type="italic">(Commenti di deputati </emphasis>- <emphasis type="italic">Una voce: “</emphasis>'
        'Basta<emphasis type="italic">”)</emphasis>… come dicevo.'))
    assert pieces == [("speech", "prosegua"),
                      ("comment", "(Commenti di deputati - Una voce: “Basta”)"),
                      ("speech", "… come dicevo.")]


def test_punctuation_behind_a_comment_returns_to_the_speech_before_it():
    pieces = p2j.split_comments(_runs('è esplosa <emphasis type="italic">(Applausi)</emphasis>, perché nessuno ha vigilato.'))
    assert pieces == [("speech", "è esplosa,"), ("comment", "(Applausi)"), ("speech", "perché nessuno ha vigilato.")]
    assert p2j.split_comments(_runs('<emphasis type="italic">(È approvato)</emphasis>.')) == [("comment", "(È approvato)")]


def test_italics_without_parentheses_and_plain_parentheses_stay_speech():
    pieces = p2j.split_comments(_runs('il <emphasis type="italic">vulnus</emphasis> che c\'era (e lo sapevamo) andava sanato.'))
    assert pieces == [("speech", "il vulnus che c'era (e lo sapevamo) andava sanato.")]


@pytest.mark.parametrize("markup,role,group,narrated,text", [
    (". La seduta è aperta.", None, None, False, "La seduta è aperta."),
    (" (P1). Grazie, Presidente.", None, "P1", False, "Grazie, Presidente."),
    (" (NM(N-C-U-I)M-CP). Signor Presidente.", None, "NM(N-C-U-I)M-CP", False, "Signor Presidente."),
    (', <emphasis type="italic">Relatrice</emphasis>. Parere favorevole.', "Relatrice", None, False, "Parere favorevole."),
    (',<emphasis type="italic"> Sottosegretario di Stato</emphasis>. Grazie.', "Sottosegretario di Stato", None, False, "Grazie."),
    (', <emphasis type="italic">Relatore</emphasis> (P1). Grazie.', "Relatore", "P1", False, "Grazie."),
    (', <emphasis type="italic">Segretario</emphasis>, legge il processo verbale.', "Segretario", None, True,
     "legge il processo verbale."),
])
def test_split_prefix(markup, role, group, narrated, text):
    got_role, got_group, got_narrated, rest = p2j.split_prefix(_runs(markup))
    assert (got_role, got_group, got_narrated) == (role, group, narrated)
    assert " ".join("".join(r.text for r in rest).split()) == text


def test_split_prefix_returns_a_form_it_does_not_know_unchanged():
    runs = _runs(" dice qualcosa senza punto")
    assert p2j.split_prefix(runs)[3] is runs


def test_clean_title_drops_the_times_and_the_full_stop():
    assert p2j.clean_title("Missioni (ore 10,03).  (ore 10,03) (ore 10,03)") == "Missioni"
    assert p2j.clean_title("Sull'ordine dei lavori.") == "Sull'ordine dei lavori"
    assert p2j.clean_title("Disposizioni in materia di esempi (A.C. 100) (ore 10,19).") == "Disposizioni in materia di esempi (A.C. 100)"


def test_sentences_end_where_the_text_ends_one():
    nlp = StubNLP()
    doc = next(nlp.pipe(["… sul terreno della guerra. Presidente, noi vogliamo la verità"]))
    assert p2j.sentence_spans(doc) == ["… sul terreno della guerra.", "Presidente, noi vogliamo la verità"]

    class Doc:
        def __init__(self, parts):
            self.sents = [type("S", (), {"text_with_ws": p, "text": p.strip()}) for p in parts]

    assert p2j.sentence_spans(Doc(["Altrettanto positiva", ", devo riscontrare, è la modifica. ", "Bene. ", "…"])) == [
        "Altrettanto positiva, devo riscontrare, è la modifica.", "Bene. …"]
    assert p2j.sentence_spans(Doc(["…"])) == ["…"]


def test_proceedings_meta(proceedings):
    meta = proceedings["meta"]
    assert (meta["session"], meta["legislature"], meta["sitting"], meta["date"]) == (SESSION, 19, 900, "2027-03-02")
    assert meta["sourceURI"] == "https://documenti.camera.it/leg19/resoconti/assemblea/html/sed0900/stenografico.htm"


def test_the_annex_is_left_out(proceedings):
    ids = [i["id"] for i in proceedings["data"]]
    assert len(ids) == 17
    assert ids[0] == "tit00000.int00010" and ids[-1] == "tit00040.int00010"
    assert not any(i.startswith("tit00050") for i in ids)


def test_debate_and_phase_of_an_intervention(proceedings):
    item = _intervention(proceedings, "tit00020.sub00010.int00020")
    assert item["debateId"] == "tit00020"
    assert item["debateTitle"] == "Seguito della discussione del disegno di legge: Disposizioni in materia di esempi (A.C. 100)"
    assert (item["phaseId"], item["phaseTitle"]) == ("tit00020.sub00010", "Dichiarazioni di voto finale - A.C. 100")
    opening = _intervention(proceedings, "tit00000.int00010")
    assert opening["debateId"] is None and opening["phaseId"] is None


def test_speakers_of_the_record(proceedings):
    assert _intervention(proceedings, "tit00000.int00010")["speaker"] == {
        "name": "ROSSI Maria", "personId": "300001", "chair": True, "role": None, "group": None,
        "chairContext": "vice-president"}
    assert _intervention(proceedings, "tit00020.sub00010.int00060")["speaker"] == {
        "name": "VERDI Paolo Emilio", "personId": "300004", "chair": False, "role": None, "group": "NM(N-C-U-I)M-CP"}
    assert _intervention(proceedings, "tit00030.sub00010.int00010")["speaker"]["role"] == "Sottosegretaria di Stato per la Giustizia"


def test_chair_context_follows_the_presidency_line():
    assert p2j.chair_context("PRESIDENZA DEL VICEPRESIDENTE FABIO RAMPELLI") == "vice-president"
    assert p2j.chair_context("PRESIDENZA DELLA VICEPRESIDENTE ANNA ASCANI") == "vice-president"
    assert p2j.chair_context("PRESIDENZA DEL PRESIDENTE LORENZO FONTANA") == "president"


def test_the_speaker_line_is_not_part_of_the_text(proceedings):
    item = _intervention(proceedings, "tit00020.sub00010.int00020")
    spoken = _sentences(item, "speech")
    assert spoken[0] == "Grazie, Presidente."
    assert not any("ANNA DE LUCA" in s or "(P1)" in s for s in spoken)
    assert _sentences(item, "comment") == ["(Applausi dei deputati del gruppo Partito Uno)"]
    assert any("il vulnus che c'era" in s for s in spoken)


def test_a_narrated_intervention_is_a_comment_as_a_whole(proceedings):
    item = _intervention(proceedings, "tit00000.int00020")
    assert item["narrated"] and item["chars"] == 0
    assert item["blocks"] == [{"kind": "comment", "sentences": [
        "LUCA BIANCHI, Segretario, legge il processo verbale della seduta di ieri."]}]


def test_a_vote_keeps_its_stage_directions_as_comments(proceedings):
    item = _intervention(proceedings, "tit00020.sub00020.int00010")
    assert [(b["kind"], b["sentences"]) for b in item["blocks"]][2:] == [
        ("speech", ["Dichiaro aperta la votazione."]),
        ("comment", ["(Segue la votazione)"]),
        ("speech", ["Dichiaro chiusa la votazione."]),
        ("speech", ["La Camera approva."]),
        ("comment", ["(Vedi votazione n. 1)"]),
    ]


def test_notices_stay_with_the_intervention_they_precede(proceedings):
    assert _intervention(proceedings, "tit00000.int00010")["notesBefore"] == [
        "PRESIDENZA DELLA VICEPRESIDENTE MARIA ROSSI", "La seduta comincia alle 10."]
    assert _intervention(proceedings, "tit00020.int00010")["notesBefore"] == [
        "La seduta, sospesa alle 10,04, è ripresa alle 10,19."]
    assert _intervention(proceedings, "tit00040.int00010")["notesAfter"] == ["La seduta termina alle 10,21."]


def test_a_calendar_title_holds_interventions_like_a_debate():
    xml = (b'<seduta legislatura="19" numero="900"><resoconto>'
           b'<titoloCalendario id="tit00030"><titolo>Sui lavori dell\'Assemblea.</titolo>'
           b'<intervento id="tit00030.int00010"><testoXHTML><nominativo id="1" cognomeNome="ROSSI Maria">PRESIDENTE</nominativo>'
           b'. Avverto che il calendario cambia.</testoXHTML></intervento></titoloCalendario></resoconto></seduta>')
    _, interventions = p2j.read_record(xml)
    assert [(i["id"], i["debateTitle"]) for i in interventions] == [("tit00030.int00010", "Sui lavori dell'Assemblea")]


def test_a_record_of_another_sitting_is_refused():
    with pytest.raises(ValueError):
        p2j.parse_record(record_bytes(), "190901", StubNLP())
    with pytest.raises(ValueError):
        p2j.read_record(b"<html/>")
