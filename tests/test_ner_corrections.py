"""NER corrections (optv/shared/ner_corrections.py) and their use in the NER stage.

No spaCy, no entity-fishing: mentions are plain dicts, the KB class lookup is a stub or a patched ``requests.get``, and the pipeline test uses a stand-in ``nlp``.
"""

from __future__ import annotations

import argparse
import collections
import json

import requests

from optv.shared import ner_corrections
from optv.shared.ner import _run_pipeline_on
from optv.shared.ner_corrections import ClassLookup, NerCorrections, load_stop_mentions
from optv.shared.workflow import _apply_manifest_defaults

CLASSES = {"Q26943907": {"Q22808320"}, "Q9927": {"Q19776628", "Q3841820"},
           "Q2270565": {"Q2695280"}, "Q45": {"Q3624078"}}


def _corrections(**kw):
    defaults = dict(stop_mentions=load_stop_mentions("pt"),
                    factions={"ch": "Q112301427", "psd": "Q595575", "l": "Q16947563"},
                    class_lookup=lambda qid: frozenset(CLASSES.get(qid, ())),
                    excluded_classes=ner_corrections.load_excluded_classes())
    defaults.update(kw)
    return NerCorrections(**defaults)


def _m(label, wid, score=0.4, wtype="ORG"):
    return {"label": label, "wid": wid, "wtype": wtype, "score": score}


def test_stop_mentions_are_normalised_like_nel_labels():
    stop = load_stop_mentions("pt")
    assert {"srs", "sr", "sr a", "deputados"} <= stop
    assert load_stop_mentions("xx") == frozenset()


def test_stop_mentions_are_dropped():
    stats = collections.Counter()
    kept = _corrections().apply([_m("Srs", "Q2270565"), _m("Sr.ª", None), _m("Portugal", "Q45")],
                            stats=stats)
    assert [m["label"] for m in kept] == ["Portugal"]
    assert stats["stop"] == 2


def test_faction_abbreviations_take_the_entity_list_wid():
    stats = collections.Counter()
    kept = _corrections().apply([_m("CH", "Q1137694", 0.15), _m("PSD", None), _m("CH", "Q112301427", 0.35)],
                            stats=stats)
    assert [(m["label"], m["wid"], m["score"]) for m in kept] == [
        ("CH", "Q112301427", None),      # relinked: not a model score any more
        ("PSD", "Q595575", None),        # rescued: entity-fishing linked nothing
        ("CH", "Q112301427", 0.35),      # already right: score kept
    ]
    assert (stats["relinked"], stats["rescued"]) == (1, 1)


def test_persons_come_only_from_the_sittings_linked_speakers():
    speeches = [{"people": [{"label": "Pedro Pinto", "wid": "Q10347324"}, {"label": "Sem Wid"}]}]
    persons = NerCorrections.session_persons(speeches)
    assert persons == {"pedro pinto": "Q10347324"}
    kept = _corrections().apply([_m("Pedro Pinto", "Q26943907", wtype="PER")], persons)
    assert kept[0]["wid"] == "Q10347324"
    # not a speaker in this sitting: entity-fishing's disambiguation page is dropped, no namesake guessed
    stats = collections.Counter()
    assert _corrections().apply([_m("Pedro Pinto", "Q26943907", wtype="PER")], {}, stats) == []
    assert stats["class"] == 1


def test_excluded_classes_and_unlinked_mentions_are_dropped():
    corrections = _corrections(factions={})
    kept = corrections.apply([_m("L", "Q9927"), _m("Fulano", None), _m("Portugal", "Q45")])
    assert [m["label"] for m in kept] == ["Portugal"]


def test_class_lookup_reads_p31_once_per_qid(monkeypatch):
    calls = []

    class _Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"statements": [{"propertyId": "P31", "value": "Q22808320"},
                                   {"propertyId": "P27", "value": "Q45"}]}

    def fake_get(url, timeout):
        calls.append(url)
        return _Response()

    monkeypatch.setattr(ner_corrections.requests, "get", fake_get)
    lookup = ClassLookup("http://ef:8090/service/")
    assert lookup("Q26943907") == frozenset({"Q22808320"})
    assert lookup("Q26943907") == frozenset({"Q22808320"})
    assert calls == ["http://ef:8090/service/kb/concept/Q26943907"]


def test_class_lookup_fails_open(monkeypatch, caplog):
    def down(url, timeout):
        raise requests.ConnectionError("refused")

    monkeypatch.setattr(ner_corrections.requests, "get", down)
    corrections = _corrections(class_lookup=ClassLookup("http://ef:8090/service"))
    kept = corrections.apply([_m("Pinto", "Q1"), _m("Outro", "Q2")])
    assert [m["wid"] for m in kept] == ["Q1", "Q2"]
    assert sum("Cannot read entity classes" in r.message for r in caplog.records) == 1


def test_build_reads_factions_from_the_entity_dump(tmp_path):
    (tmp_path / "entities.json").write_text(json.dumps({"meta": {}, "data": [
        {"id": "Q112301427", "label": "CH", "labelAlternative": ["Chega"],
         "type": "organisation", "subType": "faction"},
        {"id": "", "label": "XYZ", "labelAlternative": [], "type": "organisation", "subType": "faction"},
        {"id": "Q10347324", "label": "Pedro Pinto", "labelAlternative": [],
         "type": "person", "subType": "memberOfParliament"},
    ]}))
    corrections = NerCorrections.build(language="pt", nel_data_dir=tmp_path, api_base="http://ef/service")
    assert corrections.factions == {"ch": "Q112301427", "chega": "Q112301427"}
    assert "srs" in corrections.stop_mentions and "Q4167410" in corrections.excluded_classes
    assert NerCorrections.build(language="pt", nel_data_dir=None, api_base="http://ef/service").factions == {}


class _Ext:
    def __init__(self, qid, score):
        self.kb_qid, self.nerd_score = qid, score


class _Ent:
    def __init__(self, text, qid, score=0.5, label="ORG"):
        self.text, self.label_, self._ = text, label, _Ext(qid, score)


class _Doc:
    def __init__(self, ents):
        self.ents = ents


def test_pipeline_applies_corrections_per_sitting():
    ents = {"Sr. Presidente, Srs. Deputados: o CH votou.": [_Ent("Srs", "Q2270565"), _Ent("CH", "Q1137694")]}
    nlp = lambda text: _Doc(ents.get(text, []))   # noqa: E731
    speeches = [{"people": [], "textContents": [{"textBody": [{"type": "speech", "sentences": [
        {"text": "Sr. Presidente, Srs. Deputados: o CH votou."}]}]}]}]
    stats = _run_pipeline_on(nlp, speeches, _corrections())
    assert speeches[0]["textContents"][0]["textBody"][0]["sentences"][0]["entities"] == [
        {"label": "CH", "wid": "Q112301427", "wtype": "ORG", "score": None}]
    assert (stats["stop"], stats["relinked"]) == (1, 1)
    # without corrections: every linked span is kept, as before
    _run_pipeline_on(nlp, speeches)
    assert [e["wid"] for e in speeches[0]["textContents"][0]["textBody"][0]["sentences"][0]["entities"]] == \
        ["Q2270565", "Q1137694"]


def test_manifest_switch():
    pt = argparse.Namespace()
    _apply_manifest_defaults(pt, "PT")
    assert pt.ner_corrections is True
    de = argparse.Namespace()
    _apply_manifest_defaults(de, "DE")
    assert de.ner_corrections is False
