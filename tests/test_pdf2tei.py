"""Unit tests for the shared PDF-tier components (optv/shared/pdf2tei + lang/de).

These cover the pure logic that does not need a real PDF: the surname match key,
the rede-merge granularity rule, the TEI→turns reader, and the spine-join.
"""

from __future__ import annotations

from lxml import etree

from optv.shared.lang.de import (
    match_key_surname, is_running_header, regex_sentencize, spacy_sentencize,
    is_non_speech, join_segments)
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


def test_tei_to_turns_emits_comment_bodies():
    root = etree.Element(f"{{{TEI_NS}}}text", nsmap={None: TEI_NS, "xml": XML_NS})
    body = etree.SubElement(root, f"{{{TEI_NS}}}body")
    div = etree.SubElement(body, f"{{{TEI_NS}}}div", type="debateSection")
    etree.SubElement(div, f"{{{TEI_NS}}}head").text = "Thema"
    note = etree.SubElement(div, f"{{{TEI_NS}}}note", type="speaker")
    note.text = "Anna Müller"
    u = etree.SubElement(div, f"{{{TEI_NS}}}u", who="#x", ana="#regular")
    u.set(f"{{{XML_NS}}}id", "u1")
    etree.SubElement(u, f"{{{TEI_NS}}}seg").text = "Erster Satz."
    inc = etree.SubElement(u, f"{{{TEI_NS}}}incident")
    etree.SubElement(inc, f"{{{TEI_NS}}}desc").text = "Beifall bei der SPD"
    etree.SubElement(u, f"{{{TEI_NS}}}seg").text = "Zweiter Satz."
    t = tei_to_turns(root)[0]
    # interleaved typed bodies (mirrors DE); the interjection is kept as a comment
    assert [b["type"] for b in t["bodies"]] == ["speech", "comment", "speech"]
    assert t["bodies"][1]["sentences"][0]["text"] == "(Beifall bei der SPD)"
    # speech-only sentences (for the join / metrics) exclude the comment
    assert [s["text"] for s in t["sentences"]] == ["Erster Satz.", "Zweiter Satz."]


def test_join_text_to_spine_emits_comment_textbody():
    merged = [_rec(1, "Anna Müller")]
    turns = [{"matchKey": "muller", "index": 1, "sentences": [{"text": "Hallo."}],
              "bodies": [{"type": "speech", "sentences": [{"text": "Hallo."}]},
                         {"type": "comment", "sentences": [{"text": "(Beifall)"}]}]}]
    join_text_to_spine(merged, ["muller"], turns, creator="C", license="L")
    tb = merged[0]["textContents"][0]["textBody"]
    assert [b["type"] for b in tb] == ["speech", "comment"]
    assert tb[0]["speaker"] == "Anna Müller"
    assert tb[0]["speakerstatus"] == "main-speaker"      # untagged body: spine speaker
    assert tb[1]["speaker"] is None and tb[1]["sentences"][0]["text"] == "(Beifall)"
    assert tb[1]["speakerstatus"] is None
    assert merged[0]["people"] == [{"label": "Anna Müller"}]   # no chairs: unchanged


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
    assert qa["debug"]["confidence"] == 0.5 and qa["debug"]["confidenceReason"] == "qa-agenda-type"
    assert reg_cps["debug"]["confidence"] == 0.5 and reg_cps["debug"]["confidenceReason"] == "cps-cap"
    assert reg_ok["debug"]["confidence"] == 1.0 and "confidenceReason" not in reg_ok["debug"]
    assert reg_short["debug"]["confidence"] == 1.0
    assert "confidence" not in notext["debug"]             # video-only speech left alone


def test_regex_sentencize_protects_abbrev_ordinal_initial():
    # ordinals/dates and abbreviations must NOT split
    assert regex_sentencize("Vom 11. August 1919 an gilt das.") == ["Vom 11. August 1919 an gilt das."]
    assert regex_sentencize("In der 17. Wahlperiode, 119. Sitzung.") == ["In der 17. Wahlperiode, 119. Sitzung."]
    assert regex_sentencize("Das ist z. B. gut. Der Rest folgt.") == ["Das ist z. B. gut.", "Der Rest folgt."]
    assert regex_sentencize("Dr. Müller sprach. Dann Pause.") == ["Dr. Müller sprach.", "Dann Pause."]
    # a year at a real sentence end must STILL split (no under-splitting)
    assert regex_sentencize("Im Jahr 2026. Der nächste Satz.") == ["Im Jahr 2026.", "Der nächste Satz."]
    # placeholder never leaks
    assert "\x00" not in regex_sentencize("z. B. Test.")[0]


def test_spacy_sentencize_splits_and_protects():
    out = spacy_sentencize("Das ist gut. Der Rest folgt.")
    assert out == ["Das ist gut.", "Der Rest folgt."]
    # abbreviation / ordinal not split (German tokenizer)
    assert spacy_sentencize("Vom 11. August 1919 gilt das.") == ["Vom 11. August 1919 gilt das."]


def test_is_non_speech_vote_list_and_appendix():
    names = ", ".join(f"Max Muster{i}" for i in range(30))
    assert is_non_speech("Mit Ja haben gestimmt: GRÜNE: " + names)
    assert is_non_speech("Anlage 1 Vorschlag der Fraktion GRÜNE Umbesetzungen in verschiedenen Ausschüssen " + names)
    # a long unpunctuated name list (table)
    assert is_non_speech(names + ", " + names)
    # normal speech (even long) is kept
    assert not is_non_speech("Sehr geehrter Herr Präsident, ich möchte heute über die Lage der Apotheken sprechen.")
    assert not is_non_speech("Kurz.")


def test_join_segments_cross_block_dehyphenation():
    # word split across a block boundary -> rejoined
    assert join_segments(["Das ist eine kontinuier-", "liche Verbesserung."]) == "Das ist eine kontinuierliche Verbesserung."
    # elided compound -> hyphen + space kept
    assert join_segments(["die Ein-", "und Ausgänge"]) == "die Ein- und Ausgänge"
    # normal segments -> space-joined
    assert join_segments(["Satz eins.", "Satz zwei."]) == "Satz eins. Satz zwei."


def test_is_running_header_landtag_formats():
    # real running headers -> dropped
    assert is_running_header("Landtag von Baden-Württemberg – 17. Wahlperiode – 119. Sitzung – Mittwoch, 2. April 2025")
    assert is_running_header("Sächsischer Landtag 8. Wahlperiode – 24. Sitzung 4. Februar 2026")
    assert is_running_header("Landtag 29.01.2026 Nordrhein-Westfalen 36 Plenarprotokoll 18/116")
    assert is_running_header("7")
    # real speech naming the parliament -> NOT dropped
    assert not is_running_header("Ich eröffne die 119. Sitzung des 17. Landtags von Baden-Württemberg.")
    assert not is_running_header("In der 17. Wahlperiode haben wir viel erreicht.")
    assert not is_running_header("Der Landtag von Baden-Württemberg hat entschieden.")


def test_join_text_to_spine_stamps_clean_confidence():
    merged = [_rec(1, "Anna Müller")]
    merged[0]["agendaItem"] = {"type": "regular"}
    merged[0]["media"] = {"additionalInformation": {"startOffset": 0.0, "endOffset": 100.0}}
    turns = [{"matchKey": "muller", "index": 1, "sentences": [{"text": "Kurz."}]}]
    join_text_to_spine(merged, ["muller"], turns, creator="C", license="L")
    assert merged[0]["debug"]["confidence"] == 1.0


# --------------------------------------------------------------------------- #
# member id from the speaker label's link (DE-BY profile links)
# --------------------------------------------------------------------------- #

_PROFIL = "https://www.bayern.landtag.de/abgeordnete/abgeordnete-von-a-z/profil/{}"


def test_lead_links_keep_only_links_starting_on_the_first_line():
    import fitz
    from optv.shared.pdf2tei.backends.pymupdf_backend import _lead_links
    block = fitz.Rect(50, 100, 300, 140)                         # three lines
    page_links = [(fitz.Rect(50, 101, 200, 111), _PROFIL.format(5171)),   # label, line 1
                  (fitz.Rect(50, 113, 90, 123), _PROFIL.format(5171)),    # wrapped label
                  (fitz.Rect(120, 125, 200, 135), _PROFIL.format(9999)),  # in the body
                  (fitz.Rect(400, 101, 450, 111), _PROFIL.format(1))]     # other column
    assert _lead_links(block, page_links) == [_PROFIL.format(5171)]


def test_person_link_flows_from_label_block_to_merged_turn():
    import importlib
    from optv.shared.pdf2tei.pdf2tei import build_tei, build_registries
    cfg = importlib.import_module("optv.parliaments.DE-BY.parsers.pdf_config").CONFIG
    blocks = [
        {"text": "Präsidentin Ilse Aigner: Das Wort hat Frau Post.", "page": 0,
         "links": [_PROFIL.format(95209)]},
        {"text": "Julia Post (GRÜNE): Sehr geehrte Damen und Herren.", "page": 0,
         "links": ["https://www.bayern.landtag.de/drucksachen?dknr=1", _PROFIL.format(5354)]},
        {"text": "Staatsminister Albert Füracker (Finanzen): Vielen Dank.", "page": 0},
    ]
    root, persons, factions, has_gov, *_ = build_tei("19054", cfg, blocks, agenda_mode="regex")
    us = list(root.iter(f"{{{TEI_NS}}}u"))
    assert [u.get("corresp") for u in us] == [_PROFIL.format(95209), _PROFIL.format(5354), None]
    person_root, org_root = build_registries(persons, factions, has_gov)
    turns = tei_to_turns(root, person_root, org_root, sentencize=regex_sentencize)
    assert [t["personId"] for t in turns] == ["95209", "5354", ""]
    redes = merge_turns(turns, chain=False)
    # The chair's intro joins Post's rede; the rede's id is the main speaker's.
    assert [(r["matchKey"], r["personId"]) for r in redes] == [("post", "5354"), ("furacker", "")]


def test_join_prefers_person_id_over_surname():
    turns = [{"matchKey": "muller", "personId": "2", "index": 1, "sentences": [{"text": "Eins."}]},
             {"matchKey": "freiherr", "personId": "7", "index": 2, "sentences": [{"text": "Zwei."}]},
             {"matchKey": "klein", "personId": "", "index": 3, "sentences": [{"text": "Drei."}]}]
    merged = [_rec(1, "Anna Müller"), _rec(2, "Bert Müller"), _rec(3, "Felix von Zobel"),
              _rec(4, "Staatsminister Klein")]
    keys, ids = ["muller", "muller", "zobel", "klein"], ["1", "2", "7", ""]
    assert join_text_to_spine(merged, keys, turns, creator="C", license="L",
                              spine_person_ids=ids) == 3
    # Namesakes stay apart, a label variant matches on the id, no id: surname.
    assert [m["debug"].get("proceedingIndex") for m in merged] == [None, 1, 2, 3]


# --------------------------------------------------------------------------- #
# presiding officer: named from the protocol's chair lines
# --------------------------------------------------------------------------- #

def _u(key, name, surname, text, *, chair=None):
    """A ``tei_to_turns`` utterance; ``chair`` is its context for a chair turn."""
    return {"matchKey": key, "name": name, "surname": surname, "speaker": name,
            "isChair": bool(chair), "context": chair, "agendaTitle": "A",
            "originTextID": "u", "sentences": [{"text": text}],
            "bodies": [{"type": "speech", "sentences": [{"text": text}]}]}


def test_tei_to_turns_names_chair_and_context():
    root = etree.Element(f"{{{TEI_NS}}}text", nsmap={None: TEI_NS, "xml": XML_NS})
    div = etree.SubElement(etree.SubElement(root, f"{{{TEI_NS}}}body"),
                           f"{{{TEI_NS}}}div", type="debateSection")
    for label, who, ana in (("Erste Vizepräsidentin Ines Saborowski", "#p_ines", "#chair"),
                            ("Ines Saborowski, CDU", "#p_ines", "#regular")):
        etree.SubElement(div, f"{{{TEI_NS}}}note", type="speaker").text = label
        u = etree.SubElement(div, f"{{{TEI_NS}}}u", who=who, ana=ana)
        etree.SubElement(u, f"{{{TEI_NS}}}seg").text = "Text."
    persons = etree.Element(f"{{{TEI_NS}}}listPerson", nsmap={None: TEI_NS, "xml": XML_NS})
    person = etree.SubElement(persons, f"{{{TEI_NS}}}person")
    person.set(f"{{{XML_NS}}}id", "p_ines")
    pn = etree.SubElement(person, f"{{{TEI_NS}}}persName")
    etree.SubElement(pn, f"{{{TEI_NS}}}forename").text = "Ines"
    etree.SubElement(pn, f"{{{TEI_NS}}}surname").text = "Saborowski"
    chair, member = tei_to_turns(root, persons, None, sentencize=lambda t: [t])
    assert (chair["name"], chair["isChair"], chair["context"]) == \
        ("Ines Saborowski", True, "vice-president")
    # the context belongs to the utterance: the same person speaking as a member
    assert (member["name"], member["isChair"], member["context"]) == \
        ("Ines Saborowski", False, None)


def test_merge_turns_tags_bodies_and_collects_chairs():
    turns = [_u("dierks", "Alexander Dierks", "Dierks", "Ich eröffne.", chair="president"),
             _u("nguyen", "Nam Duy Nguyen", "Nguyen", "Rede."),
             _u("saborowski", "Ines Saborowski", "Saborowski", "Danke.", chair="vice-president"),
             _u("dierks", "Alexander Dierks", "Dierks", "Weiter.", chair="president"),
             _u("kromer", "Frank Kromer", "Kromer", "Zweite Rede.")]
    first, second = merge_turns(turns, chain=False)
    assert first["chairs"] == [
        {"name": "Alexander Dierks", "firstname": "Alexander", "lastname": "Dierks",
         "context": "president"},
        {"name": "Ines Saborowski", "firstname": "Ines", "lastname": "Saborowski",
         "context": "vice-president"}]
    assert [(b["speakerName"], b.get("chairContext")) for b in first["bodies"]] == [
        ("Alexander Dierks", "president"), ("Nam Duy Nguyen", None),
        ("Ines Saborowski", "vice-president"), ("Alexander Dierks", "president")]
    assert second["chairs"] == []


def test_join_text_to_spine_names_chair_in_people_and_textbody():
    turns = merge_turns([
        _u("nguyen", "Nam Duy Nguyen", "Nguyen", "Rede."),
        _u("dierks", "Alexander Dierks", "Dierks", "Danke.", chair="president")],
        chain=False)
    merged = [{"speechIndex": 1, "textContents": [], "debug": {},
               "people": [{"label": "Nam Duy Nguyen", "context": "main-speaker"}]}]
    join_text_to_spine(merged, ["nguyen"], turns, creator="C", license="L")
    assert merged[0]["people"] == [
        {"label": "Nam Duy Nguyen", "context": "main-speaker"},
        {"label": "Alexander Dierks", "context": "president",
         "firstname": "Alexander", "lastname": "Dierks"}]
    tb = merged[0]["textContents"][0]["textBody"]
    assert [(b["speaker"], b["speakerstatus"]) for b in tb] == [
        ("Nam Duy Nguyen", "main-speaker"), ("Alexander Dierks", "president")]


def test_join_text_to_spine_labels_absorbed_interjection():
    # chain mode (DE-BW): the interjector's text stays in the rede, as "speaker"
    turns = merge_turns([_u("mack", "Winfried Mack", "Mack", "Frage?"),
                         _u("aras", "Muhterem Aras", "Aras", "Bitte.", chair="president"),
                         _u("jung", "Christian Jung", "Jung", "Zwischenfrage."),
                         _u("mack", "Winfried Mack", "Mack", "Antwort.")],
                        chain=True, K=2)
    assert len(turns) == 1
    merged = [{"speechIndex": 1, "textContents": [], "debug": {},
               "people": [{"label": "Winfried Mack", "context": "main-speaker"}]}]
    join_text_to_spine(merged, ["mack"], turns, creator="C", license="L")
    tb = merged[0]["textContents"][0]["textBody"]
    assert [(b["speaker"], b["speakerstatus"]) for b in tb] == [
        ("Winfried Mack", "main-speaker"), ("Muhterem Aras", "president"),
        ("Christian Jung", "speaker"), ("Winfried Mack", "main-speaker")]
    assert [p["label"] for p in merged[0]["people"]] == ["Winfried Mack", "Muhterem Aras"]


# --------------------------------------------------------------------------- #
# toc: PDF-bookmark agenda titles (toc_layout="outline")
# --------------------------------------------------------------------------- #

def _bookmarked_pdf(path, outline, pages=4):
    import fitz
    doc = fitz.open()
    for _ in range(pages):
        doc.new_page()
    if outline:
        doc.set_toc(outline)
    doc.save(str(path))
    return path


def test_parse_toc_outline_groups_joint_debates(tmp_path):
    import importlib
    from optv.shared.pdf2tei import toc
    cfg = importlib.import_module("optv.parliaments.DE-SH.parsers.pdf_config").CONFIG
    pdf = _bookmarked_pdf(tmp_path / "p.pdf", [
        [1, "20/131 Plenarprotokoll Schleswig-Holstein", 1],
        [2, "Gemeinsame Beratung", 2],
        [2, "a) Einführung eines Spritpreisdeckels", 2],
        [2, "Mobilität muss bezahlbar sein", 2],
        [2, "b) Dauerhafte Entlastungen für die Mitte der Gesellschaft", 2],
        [3, "Serpil Midyatli [SPD]:", 2],
        [3, "Vizepräsidentin Beate Raudies:", 3],
        [2, "Häusliche Versorgung junger Menschen stärken", 3],
        [2, "Kinder- und Jugendhospizarbeit weiter stärken", 3],
        [3, "Dr. Heiner Garg [FDP]:", 3],
        [2, "Tierschutz stärken", 4],
        [3, "Präsidentin Kristina Herbst:", 4],
    ])
    assert cfg.toc_layout == "outline"
    entries, offset = toc.parse_toc(pdf, cfg)
    assert offset == 0
    assert entries == [("Einführung eines Spritpreisdeckels", 1),
                       ("Häusliche Versorgung junger Menschen stärken", 2),
                       ("Tierschutz stärken", 3)]
    assert toc.title_for_printed_page(entries, 2 + offset) == \
        "Häusliche Versorgung junger Menschen stärken"


def test_parse_toc_outline_falls_back_without_bookmarks(tmp_path, monkeypatch):
    import importlib
    from optv.shared.pdf2tei import toc
    cfg = importlib.import_module("optv.parliaments.DE-SH.parsers.pdf_config").CONFIG
    sentinel = ([("TOC title", 5)], 1)
    monkeypatch.setattr(toc, "_parse_toc_page_tail", lambda *_: sentinel)
    assert toc.parse_toc(_bookmarked_pdf(tmp_path / "none.pdf", []), cfg) == sentinel
    # Bookmarks without contribution entries (no level 3) are not an agenda outline.
    flat = _bookmarked_pdf(tmp_path / "flat.pdf", [[1, "Protokoll", 1], [2, "Anlage", 2]])
    assert toc.parse_toc(flat, cfg) == sentinel
