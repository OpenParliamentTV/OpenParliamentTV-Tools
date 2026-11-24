"""IT merger on the synthetic fixtures: every intervention of the WebTV page becomes a speech, with the record text that lies in its time range."""

from __future__ import annotations

import copy
import json

import pytest

from optv.parliaments.IT.common import Config
from optv.parliaments.IT.merger import merge_session as M
from optv.shared.validators import validate_stage2

from .it_fixtures import SESSION, parsed_media, parsed_proceedings

PART_1 = "https://video.example.org/assemblea19/L19_S0900_parte1.mp4"
PART_2 = "https://video.example.org/assemblea19/L19_S0900_parte2.mp4"
RECORD_PAGE = "https://documenti.camera.it/leg19/resoconti/assemblea/html/sed0900/stenografico.htm"


@pytest.fixture(scope="module")
def media() -> dict:
    return parsed_media()


@pytest.fixture(scope="module")
def proceedings() -> dict:
    return parsed_proceedings()


@pytest.fixture(scope="module")
def merged(media, proceedings) -> dict:
    return M.merge_documents(copy.deepcopy(media), copy.deepcopy(proceedings), SESSION)


def _speech(merged: dict, entry_id: str) -> dict:
    return next(s for s in merged["data"] if s["media"]["originMediaID"] == entry_id)


def _texts(speech: dict, block_type: str | None = None) -> list[str]:
    return [s["text"] for tc in speech["textContents"] for b in tc["textBody"]
            if block_type in (None, b["type"]) for s in b["sentences"]]


# ---- distributing the record over the page ----

def _entry(entry_id: str, name: str, record_id: str | None = None, person_id: str | None = None) -> dict:
    return {"id": entry_id, "name": name, "recordId": record_id, "personId": person_id}


def _item(item_id: str, name: str, chars: int, person_id: str | None = None) -> dict:
    return {"id": item_id, "chars": chars, "speaker": {"name": name, "personId": person_id}}


def test_anchors_rise_in_both_orders():
    entries = [_entry("1", "A", "x2"), _entry("2", "B", "x1"), _entry("3", "C", "x3"), _entry("4", "D", "gone")]
    items = [_item("x1", "B", 10), _item("x2", "A", 10), _item("x3", "C", 10)]
    assert M.find_anchors(entries, items) == [(0, 1), (2, 2)]       # the link back to x1 and the unknown id are ignored


def test_what_lies_between_two_anchors_stays_with_the_earlier_speech():
    entries = [_entry("1", "ROSSI Anna", "a1"), _entry("2", "VERDI Paolo", "a5")]
    items = [_item("a1", "ROSSI Anna", 900), _item("a2", "CHAIR", 9), _item("a3", "ROSSI Anna", 200),
             _item("a4", "CHAIR", 60), _item("a5", "VERDI Paolo", 800)]
    assert M.distribute(entries, items) == ([[0, 1, 2, 3], [4]], [])


def test_an_unlinked_entry_opens_with_its_own_speakers_text():
    entries = [_entry("1", "ROSSI Anna", "a1"), _entry("2", "CHAIR"), _entry("3", "VERDI Paolo", "a5")]
    items = [_item("a1", "ROSSI Anna", 900), _item("a2", "CHAIR", 9), _item("a3", "ROSSI Anna", 200),
             _item("a4", "CHAIR", 600), _item("a5", "VERDI Paolo", 800)]
    # The chair's "Concluda" stays with the speaker it interrupts; the vote opens the chair's entry.
    assert M.distribute(entries, items) == ([[0, 1, 2], [3], [4]], [])


def test_an_unlinked_entry_takes_the_earliest_fitting_text_when_nothing_else_lies_between():
    entries = [_entry("1", "ROSSI Anna", "a1"), _entry("2", "CHAIR"), _entry("3", "VERDI Paolo", "a4")]
    items = [_item("a1", "ROSSI Anna", 900), _item("a2", "CHAIR", 300), _item("a3", "CHAIR", 300), _item("a4", "VERDI Paolo", 800)]
    assert M.distribute(entries, items) == ([[0], [1, 2], [3]], [])


def test_an_unlinked_entry_without_text_of_its_speaker_stays_empty():
    entries = [_entry("1", "ROSSI Anna", "a1"), _entry("2", "CHAIR"), _entry("3", "VERDI Paolo", "a3")]
    items = [_item("a1", "ROSSI Anna", 900), _item("a2", "ROSSI Anna", 200), _item("a3", "VERDI Paolo", 800)]
    assert M.distribute(entries, items) == ([[0, 1], [], [2]], [])


def test_entries_before_the_first_anchor_share_the_opening():
    entries = [_entry("1", "CHAIR"), _entry("2", "BIANCHI Luca"), _entry("3", "CHAIR", "b1")]
    items = [_item("o1", "CHAIR", 100), _item("o2", "BIANCHI Luca", 0), _item("o3", "CHAIR", 70), _item("b1", "CHAIR", 150)]
    assert M.distribute(entries, items) == ([[0], [1, 2], [3]], [])


def test_text_before_the_first_entry_has_no_clip():
    entries = [_entry("1", "ROSSI Anna", "a2")]
    items = [_item("a1", "CHAIR", 100), _item("a2", "ROSSI Anna", 900), _item("a3", "CHAIR", 50)]
    assert M.distribute(entries, items) == ([[1, 2]], [0])


def test_people_are_compared_by_id_before_name():
    assert M.same_person(_entry("1", "ROSSI Anna", person_id="7"), _item("a", "ROSSI ANNA MARIA", 1, person_id="7"))
    assert not M.same_person(_entry("1", "ROSSI Anna", person_id="7"), _item("a", "ROSSI Anna", 1, person_id="8"))
    assert M.same_person(_entry("1", "Rossi  Anna"), _item("a", "ROSSI Anna", 1, person_id="8"))
    assert not M.same_person(_entry("1", ""), _item("a", "", 1))


def test_without_a_record_every_entry_is_empty(media):
    carried, uncovered = M.distribute(media["data"], [])
    assert carried == [[] for _ in media["data"]] and uncovered == []


# ---- the merged document ----

def test_every_intervention_but_the_suspension_is_a_speech(merged):
    speeches = merged["data"]
    assert [s["media"]["originMediaID"] for s in speeches] == [
        "500001", "500002", "500003", "500005", "500006", "500007", "500008", "500009", "500010", "500011"]
    assert [s["speechIndex"] for s in speeches] == list(range(1, 11))
    assert all(s["textContents"] for s in speeches)


def test_merged_document_validates(merged):
    errors = [f for f in validate_stage2(merged, schema="full", semantic=True) if f["severity"] == "error"]
    assert errors == []


def test_period_session_and_meta(merged):
    speech = merged["data"][0]
    assert speech["parliament"] == "IT"
    assert speech["electoralPeriod"] == {"number": 19}
    assert speech["session"] == {"number": 900, "dateStart": "2027-03-02T10:01:40+01:00",
                                 "dateEnd": "2027-03-02T10:21:45+01:00"}
    assert speech["originalLanguage"] == "it"
    meta = merged["meta"]
    assert meta["session"] == SESSION and meta["lastProcessing"] == "merge"
    assert (meta["dateStart"], meta["dateEnd"]) == ("2027-03-02T10:01:40+01:00", "2027-03-02T10:21:45+01:00")
    assert meta["sourceURI"] == "https://webtv.camera.it/evento/99001"
    assert set(meta["processing"]) == {"parse_media", "parse_proceedings", "merge"}
    assert "parliament" not in meta and "electoralPeriod" not in meta


def test_a_speech_is_a_time_range_of_its_part_of_the_recording(merged):
    first = _speech(merged, "500001")["media"]
    assert first["videoFileURI"] == PART_1 + "#t=100,110"
    assert first["duration"] == 10.0
    assert first["additionalInformation"] == {"startOffset": 100, "endOffset": 110}
    assert _speech(merged, "500006")["media"]["videoFileURI"] == PART_2 + "#t=30,60"


def test_the_last_speech_runs_to_the_end_of_the_sitting(merged):
    last = _speech(merged, "500011")
    assert last["media"]["videoFileURI"] == PART_2 + "#t=130,145"
    assert (last["dateStart"], last["dateEnd"]) == ("2027-03-02T10:21:30+01:00", "2027-03-02T10:21:45+01:00")


def test_times_are_the_pages_clock_plus_the_range(merged):
    speech = _speech(merged, "500006")
    assert (speech["dateStart"], speech["dateEnd"]) == ("2027-03-02T10:19:50+01:00", "2027-03-02T10:20:20+01:00")


def test_source_pages_open_the_player_at_the_speech_and_are_unique(merged):
    pages = [s["media"]["sourcePage"] for s in merged["data"]]
    assert pages[0] == "https://webtv.camera.it/evento/99001/500001"
    assert len(set(pages)) == len(pages)


def test_rights_come_from_the_manifest(merged):
    speech = _speech(merged, "500006")
    assert speech["media"]["creator"] == "Camera dei deputati" and speech["media"]["license"]
    text = speech["textContents"][0]
    assert text["creator"] == "Camera dei deputati" and text["license"]


def test_text_is_linked_to_its_place_in_the_record(merged):
    text = _speech(merged, "500006")["textContents"][0]
    assert (text["type"], text["language"]) == ("proceedings", "it")
    assert text["originTextID"] == "tit00020.sub00010.int00020"
    assert text["sourceURI"] == RECORD_PAGE + "#sed0900.stenografico.tit00020.sub00010.int00020"
    assert "originID" not in _speech(merged, "500006")


def test_an_interrupted_speech_keeps_the_chairs_words_and_its_own_end(merged):
    speech = _speech(merged, "500006")
    assert speech["debug"]["recordInterventions"] == [
        "tit00020.sub00010.int00020", "tit00020.sub00010.int00030", "tit00020.sub00010.int00040",
        "tit00020.sub00010.int00050"]
    body = speech["textContents"][0]["textBody"]
    assert [(b["speaker"], b["speakerstatus"]) for b in body if b["type"] == "speech"][0] == ("Anna De Luca", "main-speaker")
    chair = [b for b in body if b["speaker"] == "Maria Rossi"]
    assert [b["speakerstatus"] for b in chair] == ["vice-president", "vice-president"]
    assert chair[0]["sentences"] == [{"text": "Concluda."}]
    assert "…e per queste ragioni il nostro gruppo voterà a favore." in _texts(speech, "speech")
    assert _texts(speech, "comment") == ["(Applausi dei deputati del gruppo Partito Uno)"]


def test_people_of_a_speech(merged):
    people = _speech(merged, "500006")["people"]
    assert people[0] == {"type": "memberOfParliament", "label": "Anna De Luca", "firstname": "Anna",
                         "lastname": "De Luca", "role": "Deputata", "faction": {"label": "PARTITO UNO"},
                         "originPersonID": "300003", "context": "main-speaker"}
    assert [(p["label"], p["context"], p["role"]) for p in people[1:]] == [("Maria Rossi", "vice-president", "Vice Presidente")]


def test_a_person_the_page_shows_without_portrait_is_matched_by_name(merged):
    speech = _speech(merged, "500010")
    assert [(p["label"], p["context"]) for p in speech["people"]] == [("Giulia Neri", "main-speaker")]
    assert speech["people"][0]["role"] == "Sottosegretaria di Stato per la Giustizia"
    assert "type" not in speech["people"][0] and "faction" not in speech["people"][0]
    assert speech["debug"]["confidence"] == 1.0


def test_the_chair_is_the_main_speaker_of_its_own_speeches(merged):
    speech = _speech(merged, "500003")
    assert [(p["label"], p["context"]) for p in speech["people"]] == [("Maria Rossi", "vice-president")]
    assert {b["speakerstatus"] for b in speech["textContents"][0]["textBody"] if b["type"] == "speech"} == {"vice-president"}


def test_the_vote_the_page_does_not_link_gets_its_text(merged):
    speech = _speech(merged, "500008")
    assert speech["debug"]["anchored"] is False
    assert speech["debug"]["recordInterventions"] == ["tit00020.sub00020.int00010"]
    assert "Dichiaro aperta la votazione." in _texts(speech, "speech")
    assert _texts(speech, "comment") == ["(Segue la votazione)", "(Vedi votazione n. 1)"]
    assert _speech(merged, "500007")["debug"]["recordInterventions"][-1] == "tit00020.sub00010.int00080"


def test_agenda_items_follow_the_pages_title_tree(merged):
    declaration = _speech(merged, "500006")
    assert declaration["agendaItem"] == {
        "id": "800005",
        "officialTitle": "Disegno di legge: Disposizioni in materia di esempi (A.C. 100) (Seguito della discussione ed approvazione)",
        "title": "Disposizioni in materia di esempi. (C. 100)",
        "type": "regular"}
    assert declaration["debug"]["phase"] == "Dichiarazioni di voto finale - A.C. 100"
    vote = _speech(merged, "500008")["agendaItem"]
    assert (vote["id"], vote["type"], vote["nativeType"]) == ("800005", "voting", "IT-votazione")
    answer = _speech(merged, "500010")["agendaItem"]
    assert (answer["officialTitle"], answer["title"], answer["type"]) == (
        "Interpellanze urgenti (Svolgimento)", "Iniziative in materia di esempi - n. 2-00001", "qa")
    opening = _speech(merged, "500001")["agendaItem"]
    assert opening == {"id": "800001", "officialTitle": "Lettura Verbale", "title": "Lettura Verbale",
                       "type": "opening", "nativeType": "IT-lettura_verbale"}
    assert "phase" not in _speech(merged, "500001")["debug"]


def test_notices_of_the_record_are_comments(merged):
    assert _texts(_speech(merged, "500001"), "comment")[:2] == [
        "PRESIDENZA DELLA VICEPRESIDENTE MARIA ROSSI", "La seduta comincia alle 10."]
    assert _texts(_speech(merged, "500005"), "comment")[0] == "La seduta, sospesa alle 10,04, è ripresa alle 10,19."
    assert _texts(_speech(merged, "500011"), "comment")[-1] == "La seduta termina alle 10,21."


def test_the_reading_of_the_minutes_is_marked_as_doubtful(merged):
    speech = _speech(merged, "500002")
    assert speech["people"][0]["label"] == "Luca Bianchi"
    assert (speech["debug"]["confidence"], speech["debug"]["confidenceReason"]) == (0.5, "no-own-text")
    assert speech["debug"]["linkedMediaIndexes"] == [2]
    others = [s for s in merged["data"] if s is not speech]
    assert all(s["debug"]["confidence"] == 1.0 and "confidenceReason" not in s["debug"] for s in others)
    assert all(s["debug"]["linkedMediaIndexes"] == [s["speechIndex"]] for s in others)
    assert all(s["debug"]["proceedingIndex"] is not None for s in merged["data"])


@pytest.mark.parametrize("chars,own,seconds,expected", [
    (900, 900, 60, (1.0, None)),
    (900, 0, 60, (0.5, "no-own-text")),
    (4000, 4000, 100, (0.5, "text-too-long")),
    (399, 399, 5, (1.0, None)),                 # a few words of the chair in a short range
    (150, 150, 300, (0.5, "text-too-short")),
    (150, 150, 100, (1.0, None)),
    (150, 150, None, (1.0, None)),
])
def test_text_confidence(chars, own, seconds, expected):
    assert M.text_confidence(chars, own, seconds) == expected


def test_chair_context_reads_the_role_of_the_page():
    assert M.chair_context("Vice Presidente") == "vice-president"
    assert M.chair_context("Presidente") == "president"
    assert M.chair_context("Presidente del Consiglio dei ministri") is None
    assert M.chair_context(None) is None


def test_a_member_speaking_in_an_office_gets_the_group_named_elsewhere(media, proceedings):
    media = copy.deepcopy(media)
    # The secretary also speaks as a member later on, where the page names the group.
    extra = {**media["data"][5], "id": "500099", "name": "BIANCHI Luca", "personId": "300002",
             "role": "Deputato", "group": "PARTITO DUE", "recordId": None}
    media["data"].insert(6, extra)
    merged = M.merge_documents(media, copy.deepcopy(proceedings), SESSION)
    secretary = _speech(merged, "500002")["people"][0]
    assert (secretary["role"], secretary["faction"]) == ("Segretario", {"label": "PARTITO DUE"})


def test_without_a_record_the_speeches_are_clips_without_text(media):
    merged = M.merge_documents(copy.deepcopy(media), None, SESSION)
    assert len(merged["data"]) == 10                              # the suspension is left out all the same
    assert all(s["textContents"] == [] for s in merged["data"])
    assert all("confidence" not in s["debug"] and "proceedingIndex" not in s["debug"] for s in merged["data"])
    errors = [f for f in validate_stage2(merged, schema="full", semantic=True) if f["severity"] == "error"]
    assert errors == []


def test_merge_session_writes_the_merged_file_once(tmp_path, media, proceedings):
    config = Config(tmp_path)
    for stage, doc in (("media", media), ("proceedings", proceedings)):
        config.file(SESSION, stage, create=True).write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    out = M.merge_session(SESSION, config)
    assert out == config.file(SESSION, "merged")
    first = out.read_text(encoding="utf-8")
    assert len(json.loads(first)["data"]) == 10
    stamp = out.stat().st_mtime_ns
    M.merge_session(SESSION, config)
    assert out.stat().st_mtime_ns == stamp and out.read_text(encoding="utf-8") == first


def test_merge_session_needs_the_parsed_media_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        M.merge_session(SESSION, Config(tmp_path))
