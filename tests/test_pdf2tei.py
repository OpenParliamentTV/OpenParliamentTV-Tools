"""Unit tests for the shared PDF-tier components (optv/shared/pdf2tei + lang/de).

These cover the pure logic that does not need a real PDF: the surname match key,
the rede-merge granularity rule, the TEI→turns reader, and the spine-join.
"""

from __future__ import annotations

from lxml import etree

from optv.shared.lang.de import match_key_surname
from optv.shared.pdf2tei.merge import merge_turns
from optv.shared.pdf2tei.tei2json import tei_to_turns
from optv.shared.pdf2tei.spine_join import (
    join_text_to_spine, attach_text_by_index, assign_join_confidence)

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"


# --------------------------------------------------------------------------- #
# match_key_surname
# --------------------------------------------------------------------------- #

def test_match_key_surname_variants():
    assert match_key_surname("Katharina Schulze (GRÜNE)") == "schulze"
    assert match_key_surname("Abg. Thomas Wansch, SPD") == "wansch"
    assert match_key_surname("Präsidentin Ilse Aigner") == "aigner"
    assert match_key_surname("Staatsminister Alexander Schweitzer") == "schweitzer"
    assert match_key_surname("Dr. Danyal Bayaz") == "bayaz"
    assert match_key_surname("Katrin Ebner-Steiner") == "ebner-steiner"
    assert match_key_surname("") == ""


# --------------------------------------------------------------------------- #
# merge_turns (rede granularity)
# --------------------------------------------------------------------------- #

def _t(key, chair=False, text="x"):
    return {"matchKey": key, "isChair": chair, "speaker": key,
            "agendaTitle": "A", "originTextID": "u1", "sentences": [{"text": text}]}


def test_merge_turns_no_chain_closes_each_speaker():
    # chair intro folds into the following speaker; new speaker -> new rede (BY)
    turns = [_t("chair", chair=True), _t("schulze"), _t("flierl"), _t("flierl")]
    redes = merge_turns(turns, chain=False)
    # schulze (with chair text), flierl, flierl(new because no chain)
    assert [r["matchKey"] for r in redes] == ["schulze", "flierl", "flierl"]
    # chair text folded into first rede
    assert len(redes[0]["sentences"]) == 2


def test_merge_turns_chain_absorbs_zwischenfrage():
    # main resumes within K -> the interjector is absorbed into one rede (BW)
    turns = [_t("mack"), _t("aras"), _t("mack")]
    redes = merge_turns(turns, chain=True, K=2)
    assert [r["matchKey"] for r in redes] == ["mack"]
    assert len(redes[0]["sentences"]) == 3


# --------------------------------------------------------------------------- #
# tei_to_turns
# --------------------------------------------------------------------------- #

def _tei():
    root = etree.Element(f"{{{TEI_NS}}}text", nsmap={None: TEI_NS, "xml": XML_NS})
    body = etree.SubElement(root, f"{{{TEI_NS}}}body")
    div = etree.SubElement(body, f"{{{TEI_NS}}}div", type="debateSection")
    etree.SubElement(div, f"{{{TEI_NS}}}head").text = "Mein Thema"
    note = etree.SubElement(div, f"{{{TEI_NS}}}note", type="speaker")
    note.text = "Anna Müller (SPD)"
    u = etree.SubElement(div, f"{{{TEI_NS}}}u", who="#p_anna_mueller", ana="#regular")
    u.set(f"{{{XML_NS}}}id", "u1")
    etree.SubElement(u, f"{{{TEI_NS}}}seg").text = "Erster Satz. Zweiter Satz."

    persons = etree.Element(f"{{{TEI_NS}}}listPerson", nsmap={None: TEI_NS, "xml": XML_NS})
    p = etree.SubElement(persons, f"{{{TEI_NS}}}person")
    p.set(f"{{{XML_NS}}}id", "p_anna_mueller")
    pn = etree.SubElement(p, f"{{{TEI_NS}}}persName")
    etree.SubElement(pn, f"{{{TEI_NS}}}forename").text = "Anna"
    etree.SubElement(pn, f"{{{TEI_NS}}}surname").text = "Müller"
    etree.SubElement(p, f"{{{TEI_NS}}}affiliation", ref="#parliamentaryGroup.SPD", role="member")

    orgs = etree.Element(f"{{{TEI_NS}}}listOrg", nsmap={None: TEI_NS, "xml": XML_NS})
    org = etree.SubElement(orgs, f"{{{TEI_NS}}}org")
    org.set(f"{{{XML_NS}}}id", "parliamentaryGroup.SPD")
    etree.SubElement(org, f"{{{TEI_NS}}}orgName", full="yes").text = "SPD"
    return root, persons, orgs


def test_tei_to_turns_reads_speaker_text_and_agenda():
    root, persons, orgs = _tei()
    turns = tei_to_turns(root, persons, orgs)
    assert len(turns) == 1
    t = turns[0]
    assert t["matchKey"] == "muller"          # accent-folded surname
    assert t["party"] == "SPD"
    assert t["agendaTitle"] == "Mein Thema"
    assert t["originTextID"] == "u1"
    assert [s["text"] for s in t["sentences"]] == ["Erster Satz.", "Zweiter Satz."]


# --------------------------------------------------------------------------- #
# spine joins
# --------------------------------------------------------------------------- #

def _rec(idx, label):
    return {"speechIndex": idx, "people": [{"label": label}], "textContents": [], "debug": {}}


def test_join_text_to_spine_matches_by_surname():
    merged = [_rec(1, "Anna Müller"), _rec(2, "Ben Klein")]
    spine_keys = ["muller", "klein"]
    turns = [{"matchKey": "klein", "index": 7, "sentences": [{"text": "Hallo."}]}]
    n = join_text_to_spine(merged, spine_keys, turns, creator="C", license="L")
    assert n == 1
    assert merged[0]["textContents"] == []          # no match for Müller
    assert merged[1]["textContents"][0]["textBody"][0]["sentences"][0]["text"] == "Hallo."
    assert merged[1]["debug"]["proceedingIndex"] == 7


def test_attach_text_by_index():
    merged = [_rec(1, "A"), _rec(2, "B")]
    turns = [{"speechIndex": 2, "sentences": [{"text": "Text."}]}]
    n = attach_text_by_index(merged, turns, creator="C", license="L")
    assert n == 1
    assert merged[0]["textContents"] == []
    assert merged[1]["textContents"][0]["textBody"][0]["sentences"][0]["text"] == "Text."


def _txt_rec(idx, agenda_type, text, start=0.0, end=100.0):
    tc = ([{"textBody": [{"sentences": [{"text": text}]}]}] if text else [])
    return {
        "speechIndex": idx,
        "people": [{"label": f"S{idx}"}],
        "agendaItem": {"type": agenda_type},
        "media": {"additionalInformation": {"startOffset": start, "endOffset": end}},
        "textContents": tc,
        "debug": {},
    }


def test_assign_join_confidence_gates_qa_and_cps():
    qa = _txt_rec(1, "qa", "x" * 50)                       # qa type -> gated
    reg_ok = _txt_rec(2, "regular", "x" * 50)              # 0.5 cps, clean
    reg_cps = _txt_rec(3, "regular", "x" * 600, end=5)     # 120 cps, >=floor -> gated
    reg_short = _txt_rec(4, "regular", "x" * 200, end=1)   # 200 cps but <500 floor -> clean
    notext = _txt_rec(5, "regular", "")                    # no text -> untouched
    gated = assign_join_confidence([qa, reg_ok, reg_cps, reg_short, notext])
    assert gated == 2
    assert qa["debug"]["confidence"] == 0.5 and qa["debug"]["confidence_reason"] == "qa-agenda-type"
    assert reg_cps["debug"]["confidence"] == 0.5 and reg_cps["debug"]["confidence_reason"] == "cps-cap"
    assert reg_ok["debug"]["confidence"] == 1.0 and "confidence_reason" not in reg_ok["debug"]
    assert reg_short["debug"]["confidence"] == 1.0
    assert "confidence" not in notext["debug"]             # video-only speech left alone


def test_join_text_to_spine_stamps_clean_confidence():
    merged = [_rec(1, "Anna Müller")]
    merged[0]["agendaItem"] = {"type": "regular"}
    merged[0]["media"] = {"additionalInformation": {"startOffset": 0.0, "endOffset": 100.0}}
    turns = [{"matchKey": "muller", "index": 1, "sentences": [{"text": "Kurz."}]}]
    join_text_to_spine(merged, ["muller"], turns, creator="C", license="L")
    assert merged[0]["debug"]["confidence"] == 1.0
