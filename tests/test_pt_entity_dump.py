"""PT entity dump: namesakes stay apart, carry the Assembly deputy ID, and the most recent one wins a name lookup."""

from __future__ import annotations

from pathlib import Path

from optv.parliaments.PT.scraper import build_entity_dump as dump
from optv.shared.nel import get_nel_data, link_entities


def _row(qid, label, latest="", arids=""):
    row = {"person": {"value": f"http://www.wikidata.org/entity/{qid}"},
           "label_pt": {"value": label}}
    if latest:
        row["latest"] = {"value": latest}
    if arids:
        row["arids"] = {"value": arids}
    return row


PAYLOAD = {"results": {"bindings": [
    _row("Q110759678", "Pedro Pinto", "2022-03-29T00:00:00Z", "7336"),
    _row("Q10347324", "Pedro Pinto", "1995-10-27T00:00:00Z", "629"),
    _row("Q25440961", "Paulo Núncio", "2024-03-26T00:00:00Z", "8723|8723b"),
]}}


def test_namesakes_are_kept_and_ordered_by_latest_term():
    persons = dump.parse_members(PAYLOAD)
    assert list(persons) == ["Q10347324", "Q110759678", "Q25440961"]
    assert persons["Q110759678"]["arids"] == ["7336"]
    assert persons["Q25440961"]["arids"] == ["8723", "8723b"]


def test_nel_links_by_deputy_id_then_by_most_recent_name(tmp_path, monkeypatch):
    monkeypatch.setattr(dump, "fetch_members", lambda: dump.parse_members(PAYLOAD))
    monkeypatch.setattr(dump, "fetch_parties", lambda: [
        {"qid": "Q63645885", "label": "CHEGA", "label_en": "Chega", "abbrev": "", "n": 60}])
    entities = dump.build()
    (tmp_path / "entities.json").write_text(__import__("json").dumps(entities))
    pinto = [e for e in entities["data"] if e["id"] == "Q10347324"][0]
    assert pinto["additionalInformation"] == {"originPersonID": ["629"]}
    ch = [e for e in entities["data"] if e["label"] == "CH"][0]
    assert ch["id"] == "Q63645885"
    assert ch["labelAlternative"] == ["CHEGA", "Chega"]     # names of the pinned party without an abbreviation

    persons, factions = get_nel_data(Path(tmp_path))
    speeches = [
        {"people": [{"label": "Pedro Pinto", "originPersonID": "629"}]},   # by ID: the older namesake
        {"people": [{"label": "Pedro Pinto"}]},                            # by name: the most recent one
    ]
    link_entities(speeches, persons, factions)
    assert [s["people"][0]["wid"] for s in speeches] == ["Q10347324", "Q110759678"]
