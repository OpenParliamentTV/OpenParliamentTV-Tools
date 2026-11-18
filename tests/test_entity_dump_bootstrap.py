"""The shared Wikidata entity-dump builder and the manifest blocks that configure it."""

import json

import pytest

from optv.parliaments import list_parliaments, load_manifest
from optv.shared import entity_dump_bootstrap as bootstrap

LANDTAGE = [pid for pid in list_parliaments() if load_manifest(pid).get("entity_bootstrap")]


def _binding(qid, de=None, en=None, mul=None, aliases=None):
    b = {"person": {"value": f"http://www.wikidata.org/entity/{qid}"}}
    for key, value in (("label_de", de), ("label_en", en), ("label_mul", mul), ("aliases_de", aliases)):
        if value is not None:
            b[key] = {"value": value}
    return b


@pytest.fixture
def wikidata(monkeypatch):
    """Answer the next SPARQL query with these bindings; records the queries sent."""
    state = {"bindings": [], "queries": []}

    def sparql_get(query, **kwargs):
        state["queries"].append(query)
        return {"results": {"bindings": state["bindings"]}}

    monkeypatch.setattr(bootstrap, "sparql_get", sparql_get)
    return state


def test_label_falls_back_from_german_to_english_to_mul_to_qid(wikidata):
    wikidata["bindings"] = [
        _binding("Q1", de="Anna Beispiel", en="Anna Example", mul="Anna Beispiel"),
        _binding("Q2", en="Only English"),
        _binding("Q3", mul="Nur Mul"),
        _binding("Q4"),
        {"label_de": {"value": "no person"}},
    ]
    members = bootstrap.fetch_p39_members("Q17481175")
    assert [(m["id"], m["label"], m["labelAlternative"]) for m in members] == [
        ("Q1", "Anna Beispiel", ["Anna Example"]),
        ("Q2", "Only English", []),
        ("Q3", "Nur Mul", []),
        ("Q4", "Q4", []),
    ]
    assert all(m["subType"] == "memberOfParliament" for m in members)
    assert "ps:P39 wd:Q17481175" in wikidata["queries"][0]
    assert "altLabel" not in wikidata["queries"][0]


def test_aliases_are_taken_only_when_asked_and_only_multi_word(wikidata):
    wikidata["bindings"] = [_binding("Q1", de="Ines Saborowski-Richter", en="Ines Saborowski-Richter",
                                     aliases="Saborowski|Ines Saborowski|Ines Saborowski-Richter")]
    assert bootstrap.fetch_p39_members("Q17334379")[0]["labelAlternative"] == []
    with_aliases = bootstrap.fetch_p39_members("Q17334379", with_aliases=True)
    assert with_aliases[0]["labelAlternative"] == ["Ines Saborowski"]
    assert "altLabel" in wikidata["queries"][1]


def test_extra_members_fill_gaps_without_replacing_query_results(wikidata):
    extra = [{"id": "Q9", "label": "Kristina Herbst"}, {"id": "Q1", "label": "Stale Label"}]
    wikidata["bindings"] = [_binding("Q1", de="From Wikidata")]
    members = bootstrap.fetch_p39_members("Q18130496", extra_members=extra)
    assert [(m["id"], m["label"]) for m in members] == [("Q1", "From Wikidata"), ("Q9", "Kristina Herbst")]
    assert members[1] == {"id": "Q9", "label": "Kristina Herbst", "subType": "memberOfParliament",
                          "labelAlternative": []}


def test_dump_lists_persons_then_factions(wikidata, tmp_path):
    wikidata["bindings"] = [_binding("Q1", de="Anna Beispiel")]
    out = bootstrap.build_p39_dump(
        tmp_path / "metadata", position="Q17481175",
        parties=[{"id": "Q49762", "label": "CDU", "aliases": ["CDU/CSU"]}, {"id": "Q49768", "label": "SPD"}])
    text = out.read_text()
    doc = json.loads(text)
    assert out == tmp_path / "metadata" / "entities.json"
    assert text == json.dumps(doc, indent=2, ensure_ascii=False)
    assert doc["data"][1:] == [
        {"id": "Q49762", "label": "CDU", "subType": "faction", "labelAlternative": ["CDU/CSU"]},
        {"id": "Q49768", "label": "SPD", "subType": "faction", "labelAlternative": []},
    ]


def test_the_german_landtage_are_configured():
    assert LANDTAGE == ["DE-BW", "DE-BY", "DE-HH", "DE-NI", "DE-NW", "DE-SH", "DE-SN", "DE-ST"]


@pytest.mark.parametrize("parliament", LANDTAGE)
def test_manifest_block_builds_a_dump(parliament, wikidata, tmp_path):
    config = load_manifest(parliament)["entity_bootstrap"]
    out = bootstrap.build_from_manifest(parliament, tmp_path)
    factions = [e for e in json.loads(out.read_text())["data"] if e["subType"] == "faction"]
    assert f"ps:P39 wd:{config['wikidata_position']}" in wikidata["queries"][0]
    assert [f["label"] for f in factions] == [p["label"] for p in config["parties"]]
    for faction in factions:
        assert faction["id"].startswith("Q") and faction["id"][1:].isdigit()
        assert faction["label"] not in faction["labelAlternative"]


def test_parliament_without_a_block_is_refused(tmp_path):
    with pytest.raises(KeyError):
        bootstrap.build_from_manifest("DE", tmp_path)


def test_party_table_names_each_item_once():
    ids = list(bootstrap.GERMAN_PARTIES.values())
    assert len(set(ids)) == len(ids)


@pytest.mark.parametrize("parliament", LANDTAGE)
def test_landtag_parties_come_from_the_party_table(parliament):
    for party in load_manifest(parliament)["entity_bootstrap"]["parties"]:
        assert "id" not in party
        assert party["party"] in bootstrap.GERMAN_PARTIES


@pytest.mark.parametrize("parliament, label, qid", [
    ("DE-BW", "FDP/DVP", "Q13124"),
    ("DE-BY", "CSU", "Q49763"),
    ("DE-SH", "SSW", "Q161545"),
    ("DE-SN", "BSW", "Q123121346"),
    ("DE-SN", "Die Linke", "Q49764"),
    ("DE-SN", "BÜNDNISGRÜNE", "Q49766"),
    ("DE-ST", "FDP", "Q13124"),
    ("DE-ST", "AfD", "Q6721203"),
    ("DE-ST", "Die Linke", "Q49764"),
    ("DE-ST", "BÜNDNIS 90/DIE GRÜNEN", "Q49766"),
    ("DE-ST", "BSW", "Q123121346"),
])
def test_faction_labels_resolve_to_the_party_item(parliament, label, qid, wikidata, tmp_path):
    out = bootstrap.build_from_manifest(parliament, tmp_path)
    factions = {e["label"]: e["id"] for e in json.loads(out.read_text())["data"] if e["subType"] == "faction"}
    assert factions[label] == qid


def test_a_party_outside_the_table_names_its_item_directly(wikidata, tmp_path, monkeypatch):
    block = {"wikidata_position": "Q1", "parties": [{"id": "Q42", "label": "Regionalpartei"},
                                                    {"party": "spd", "label": "SPD"}]}
    monkeypatch.setattr("optv.parliaments.load_manifest", lambda pid: {"entity_bootstrap": block})
    out = bootstrap.build_from_manifest("XX", tmp_path)
    assert [(e["label"], e["id"]) for e in json.loads(out.read_text())["data"]] == [
        ("Regionalpartei", "Q42"), ("SPD", "Q49768")]
