"""The synthetic NL fixtures, parsed through the NL parser functions.

``tests/fixtures/NL/verslag-sample.xml`` is an invented report in the House's report schema: an opening, a debate with a motion, interruptions, a suspension and a seventy-minute answer of a minister, an order of business with a debate request, two votes, and the closing. ``debatdirect-sample.json`` is the matching file the scraper would write, with debate records for the debate, the order of business and the votes, and none for the opening.
"""

import json
import re
from pathlib import Path

from optv.parliaments.NL.parsers.media2json import parse_debates
from optv.parliaments.NL.parsers.proceedings2json import parse_report

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "NL"
SESSION = "2025-050"


class StubNLP:
    """Stands in for the spaCy model: splits at sentence-final punctuation."""

    def pipe(self, texts):
        for text in texts:
            yield _Doc(text)


class _Sentence:
    def __init__(self, text: str):
        self.text = text


class _Doc:
    def __init__(self, text: str):
        self.text = text
        self.sents = [_Sentence(p) for p in re.split(r"(?<=[.!?])\s+", text) if p.strip()]


def report_bytes() -> bytes:
    return (FIXTURES / "verslag-sample.xml").read_bytes()


def raw_debates() -> dict:
    return json.loads((FIXTURES / "debatdirect-sample.json").read_text(encoding="utf-8"))


def parsed_proceedings() -> dict:
    return parse_report(report_bytes(), SESSION, StubNLP())


def parsed_media() -> dict:
    return parse_debates(raw_debates(), SESSION)
