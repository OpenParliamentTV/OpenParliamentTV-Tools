"""DE proceedings document extraction (Drucksachen).

Regression for the period-21 markup change: the Bundestag moved the Drucksache
number into an <a> link, so the old doc.text-only regex extracted nothing and
all of period 21 published with empty documents. parse_documents must read the
full element text (children included) and keep building the per-number PDF URI.
"""

from lxml import etree

from optv.parliaments.DE.parsers.proceedings2json import parse_documents
from optv.shared.merge_format import dedupe_documents


def _doc(ref):
    s, n = ref.split("/")
    padded = n.rjust(5, "0")
    return {"type": "officialDocument", "label": f"Drucksache {ref}",
            "sourceURI": f"https://dserver.bundestag.de/btd/{s}/{padded[:3]}/{s}{padded}.pdf"}


def _op(inner_xml: str):
    return etree.fromstring(f"<tagesordnungspunkt>{inner_xml}</tagesordnungspunkt>")


def test_inline_number_pre_period21_shape():
    op = _op('<p klasse="T_Drs">Drucksache 20/26</p>')
    docs = list(parse_documents(op))
    assert docs == [{
        "type": "officialDocument",
        "label": "Drucksache 20/26",
        "sourceURI": "https://dserver.bundestag.de/btd/20/000/2000026.pdf",
    }]


def test_linked_number_period21_shape():
    # Number wrapped in an <a> link -> doc.text alone is "Drucksache " (no digits).
    op = _op('<p klasse="T_Drs">Drucksache '
             '<a href="https://dserver.bundestag.de/btd/21/011/2101100.pdf">21/1100</a></p>')
    docs = list(parse_documents(op))
    assert docs == [{
        "type": "officialDocument",
        "label": "Drucksache 21/1100",
        "sourceURI": "https://dserver.bundestag.de/btd/21/011/2101100.pdf",
    }]


def test_constructed_uri_matches_the_href_when_linked():
    # The per-number constructed URI must equal the URL the Bundestag links to,
    # so the backfill / live parse agree.
    op = _op('<p klasse="T_Drs">Drucksache '
             '<a href="https://dserver.bundestag.de/btd/21/011/2101100.pdf">21/1100</a></p>')
    href = op.find('.//a').get('href')
    assert list(parse_documents(op))[0]["sourceURI"] == href


def test_multiple_drucksachen_in_one_paragraph():
    # Only the first ref is linked; the rest are plain text. All are extracted,
    # each with its own constructed per-number URI (not the single shared href).
    op = _op('<p klasse="T_Drs">Drucksachen '
             '<a href="https://dserver.bundestag.de/btd/21/020/2102033.pdf">21/2033</a>, '
             '21/2037</p>')
    docs = list(parse_documents(op))
    assert [d["label"] for d in docs] == ["Drucksache 21/2033", "Drucksache 21/2037"]
    assert docs[1]["sourceURI"] == "https://dserver.bundestag.de/btd/21/020/2102037.pdf"


def test_href_url_does_not_produce_spurious_matches():
    # itertext() excludes attribute values, so the digits inside the href URL
    # must not be picked up as extra Drucksachen.
    op = _op('<p klasse="T_Drs">Drucksache '
             '<a href="https://dserver.bundestag.de/btd/21/011/2101100.pdf">21/1100</a></p>')
    assert len(list(parse_documents(op))) == 1


# dedupe_documents -- the merger and the backfill share this so their output
# stays byte-identical (otherwise a re-publish would churn the backfill).


def test_dedupe_documents_drops_repeated_sourceuri():
    docs = [_doc("20/188"), _doc("20/250"), _doc("20/250"), _doc("20/186"), _doc("20/250")]
    out = dedupe_documents(docs)
    assert [d["label"] for d in out] == [
        "Drucksache 20/188", "Drucksache 20/250", "Drucksache 20/186"]


def test_dedupe_documents_preserves_first_seen_order():
    docs = [_doc("21/2"), _doc("21/1"), _doc("21/2")]
    assert [d["label"] for d in dedupe_documents(docs)] == ["Drucksache 21/2", "Drucksache 21/1"]


def test_dedupe_documents_falls_back_to_label_without_uri():
    a = {"type": "officialDocument", "label": "Antrag X"}
    out = dedupe_documents([a, dict(a), a])
    assert out == [a]


def test_dedupe_documents_empty_is_empty():
    assert dedupe_documents([]) == []
