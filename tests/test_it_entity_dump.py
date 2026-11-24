"""IT entity list: Wikidata query responses turned into the document the entity linking reads. No network."""

from __future__ import annotations

from optv.parliaments.IT.scraper import build_entity_dump as B
from optv.shared.nel import cleanup, link_entities, PersonIndex


def _row(qid, *, it=None, mul=None, en=None, aliases="", chamber_ids="", latest=None):
    row = {"person": {"value": f"http://www.wikidata.org/entity/{qid}"},
           "aliases": {"value": aliases}, "chamber_ids": {"value": chamber_ids}}
    for key, value in (("label_it", it), ("label_mul", mul), ("label_en", en), ("latest", latest)):
        if value is not None:
            row[key] = {"value": value}
    return row


def _payload(*rows):
    return {"results": {"bindings": list(rows)}}


def test_label_is_italian_then_language_independent_then_english():
    persons = B.parse_persons(_payload(
        _row("Q1", it="Anna De Luca", en="Anna De Luca (politician)"),
        _row("Q2", mul="Paolo Emilio Verdi", en="Paolo Verdi"),
        _row("Q3", en="Giulia Neri"),
        _row("Q4"),                                  # no label at all: left out
    ))
    assert [p["label"] for p in persons.values()] == ["Anna De Luca", "Paolo Emilio Verdi", "Giulia Neri"]
    assert persons["Q1"]["aliases"] == ["Anna De Luca (politician)"]
    assert "Q4" not in persons


def test_aliases_of_one_word_are_left_out():
    person = B.parse_persons(_payload(_row("Q1", it="Maria Rossi", aliases="Rossi|Maria Teresa Rossi")))["Q1"]
    assert person["aliases"] == ["Maria Teresa Rossi"]


def test_the_chambers_person_id_goes_into_the_entity():
    deputies = B.parse_persons(_payload(
        _row("Q1", it="Anna De Luca", chamber_ids="300003"),
        _row("Q2", it="Paolo Emilio Verdi", chamber_ids="300004|300044"),
        _row("Q3", it="Luca Bianchi"),
    ))
    entities = {e["id"]: e for e in B.build_entities(deputies, {})["data"] if e["type"] == "person"}
    assert entities["Q1"]["additionalInformation"] == {"originPersonID": "300003"}
    assert entities["Q2"]["additionalInformation"] == {"originPersonID": ["300004", "300044"]}
    assert "additionalInformation" not in entities["Q3"]


def test_deputies_are_written_oldest_term_first_so_the_latest_namesake_wins():
    deputies = B.parse_persons(_payload(
        _row("Q20", it="Mario Rossi", latest="2022-10-13T00:00:00Z"),
        _row("Q10", it="Mario Rossi", latest="1948-05-08T00:00:00Z"),
        _row("Q30", it="Piero Bianchi"),             # no dated term: first
    ))
    persons = [e for e in B.build_entities(deputies, {})["data"] if e["type"] == "person"]
    assert [e["id"] for e in persons] == ["Q30", "Q10", "Q20"]
    by_name = {}
    for e in persons:
        by_name[cleanup(e["label"])] = e["id"]
    assert by_name["mario rossi"] == "Q20"


def test_senators_and_ministers_who_sat_in_the_chamber_stay_deputies():
    deputies = B.parse_persons(_payload(_row("Q1", it="Anna De Luca", chamber_ids="300003")))
    others = B.parse_persons(_payload(
        _row("Q1", it="Anna De Luca", latest="2022-10-22T00:00:00Z"),
        _row("Q2", it="Giulia Neri", latest="2022-10-13T00:00:00Z"),
    ))
    sub_types = {e["id"]: e["subType"] for e in B.build_entities(deputies, others)["data"] if e["type"] == "person"}
    assert sub_types == {"Q1": "memberOfParliament", "Q2": "person"}


def test_groups_are_keyed_by_the_names_of_the_page_and_the_abbreviations_of_the_record():
    doc = B.build_entities({}, {})
    groups = [e for e in doc["data"] if e["subType"] == "faction"]
    assert all(e["type"] == "organisation" and e["id"].startswith("Q") for e in groups)
    assert len({e["id"] for e in groups}) == len(groups)
    keys = {cleanup(name): e["id"] for e in groups for name in [e["label"], *e["labelAlternative"]]}
    assert keys[cleanup("FRATELLI D'ITALIA")] == keys[cleanup("FDI")]
    assert keys[cleanup("PARTITO DEMOCRATICO - ITALIA DEMOCRATICA E PROGRESSISTA")] == keys[cleanup("PD-IDP")]
    assert cleanup("NM(N-C-U-I)M-CP") in keys and cleanup("MISTO") in keys


def test_a_speaker_is_linked_by_the_chambers_id_before_the_name():
    deputies = B.parse_persons(_payload(
        _row("Q1", it="Anna De Luca", chamber_ids="300003"),
        _row("Q2", it="Anna De Luca", chamber_ids="300099", latest="2022-10-13T00:00:00Z"),
    ))
    doc = B.build_entities(deputies, {})
    persons = PersonIndex()
    for entity in doc["data"]:
        if entity["type"] == "person":
            persons.entities.append(entity)
            persons[cleanup(entity["label"])] = entity
    speeches = [{"people": [{"label": "Anna De Luca", "context": "main-speaker", "originPersonID": "300003"}]}]
    link_entities(speeches, persons, {})
    assert speeches[0]["people"][0]["wid"] == "Q1"      # the name alone would give the namesake Q2
