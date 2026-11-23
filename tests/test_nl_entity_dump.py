"""NL entity list: Wikidata query responses turned into the document the entity linking reads. No network."""

from __future__ import annotations

from optv.parliaments.NL.scraper import build_entity_dump as B
from optv.shared.nel import cleanup


def _row(qid, *, nl=None, mul=None, en=None, aliases="", latest=None):
    row = {"person": {"value": f"http://www.wikidata.org/entity/{qid}"}, "aliases": {"value": aliases}}
    for key, value in (("label_nl", nl), ("label_mul", mul), ("label_en", en), ("latest", latest)):
        if value is not None:
            row[key] = {"value": value}
    return row


def _payload(*rows):
    return {"results": {"bindings": list(rows)}}


def test_label_is_dutch_then_language_independent_then_english():
    persons = B.parse_persons(_payload(
        _row("Q1", nl="Anna Jansen", en="Anna Jansen (politician)"),
        _row("Q2", mul="Bram de Vries", en="Bram De Vries"),
        _row("Q3", en="Carla Bakker"),
        _row("Q4"),                                  # no label at all: left out
    ))
    assert [p["label"] for p in persons.values()] == ["Anna Jansen", "Bram de Vries", "Carla Bakker"]
    assert persons["Q1"]["aliases"] == ["Anna Jansen (politician)"]
    assert "Q4" not in persons


def test_aliases_of_one_word_are_left_out():
    person = B.parse_persons(_payload(
        _row("Q1", nl="Femke Wiersma", aliases="Wiersma|F.M. Wiersma|Femke Marije Wiersma")))["Q1"]
    assert person["aliases"] == ["F.M. Wiersma", "Femke Marije Wiersma"]


def test_members_are_written_oldest_term_first_so_the_latest_namesake_wins():
    members = B.parse_persons(_payload(
        _row("Q20", nl="Jan de Boer", latest="2023-12-06T00:00:00Z"),
        _row("Q10", nl="Jan de Boer", latest="1977-06-08T00:00:00Z"),
        _row("Q30", nl="Piet Smit"),                 # no dated term: first
    ))
    doc = B.build_entities(members, {})
    persons = [e for e in doc["data"] if e["type"] == "person"]
    assert [e["id"] for e in persons] == ["Q30", "Q10", "Q20"]
    # The entity linking keeps the last member it reads for a name.
    by_name = {}
    for e in persons:
        by_name[cleanup(e["label"])] = e["id"]
    assert by_name["jan de boer"] == "Q20"


def test_cabinet_members_who_sat_in_the_house_stay_members():
    members = B.parse_persons(_payload(_row("Q1", nl="Sophie Hermans", latest="2017-03-23T00:00:00Z")))
    cabinet = B.parse_persons(_payload(
        _row("Q1", nl="Sophie Hermans", latest="2026-02-23T00:00:00Z"),
        _row("Q2", nl="Rianne Letschert", latest="2026-02-23T00:00:00Z"),
    ))
    doc = B.build_entities(members, cabinet)
    sub_types = {e["id"]: e["subType"] for e in doc["data"] if e["type"] == "person"}
    assert sub_types == {"Q1": "memberOfParliament", "Q2": "person"}


def test_factions_are_keyed_by_the_names_the_report_uses():
    doc = B.build_entities({}, {})
    factions = {e["label"]: e for e in doc["data"] if e["subType"] == "faction"}
    assert {"VVD", "D66", "PRO", "ChristenUnie", "Groep Markuszower"} <= set(factions)
    assert all(e["type"] == "organisation" and e["id"].startswith("Q") for e in factions.values())
    ids = [e["id"] for e in factions.values()]
    assert len(set(ids)) == len(ids)
    assert "Volkspartij voor Vrijheid en Democratie" in factions["VVD"]["labelAlternative"]


def test_queries_name_the_position_and_the_cabinet_offices():
    assert "wd:Q18887908" in B.SPARQL_MEMBERS
    cabinet = B.SPARQL_CABINET % {"since": B.OFFICE_SINCE}
    assert "wd:Q55" in cabinet and "wd:Q83307" in cabinet and "wd:Q1847103" in cabinet
    assert f'"{B.OFFICE_SINCE}T00:00:00Z"' in cabinet
