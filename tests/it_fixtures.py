"""The synthetic IT fixtures, parsed through the IT parser functions.

``tests/fixtures/IT/evento-sample.html`` is an invented sitting in the structure of a WebTV sitting page: eleven interventions in two parts of the recording, among them the opening, the reading of the minutes, a suspension, two declarations of vote with interruptions, a vote the page does not link to the record, an answer of an undersecretary, and the closing. ``stenografico-sample.xml`` is the matching record in the Chamber's record schema, with an annex of a text that was not spoken. ``archivio-sample.xml`` is an answer of the WebTV archive service that lists the sitting.
"""

import re
from pathlib import Path

from optv.parliaments.IT.parsers.media2json import parse_page
from optv.parliaments.IT.parsers.proceedings2json import parse_record

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "IT"
SESSION = "190900"

ARCHIVE_ENTRY = {
    "session": SESSION,
    "event": 99001,
    "legislature": 19,
    "sitting": 900,
    "title": "AULA, Seduta 900 - Esempio di una seduta",
    "dateStart": "2027-03-02 10:00:00",
    "dateEnd": "2027-03-02 10:21:45",
    "record": True,
    "recordLinked": True,
}


class StubNLP:
    """Stands in for the spaCy model: splits behind sentence-final punctuation and keeps the whitespace, as spaCy's sentences do."""

    def pipe(self, texts):
        for text in texts:
            yield _Doc(text)


class _Sentence:
    def __init__(self, text_with_ws: str):
        self.text_with_ws = text_with_ws
        self.text = text_with_ws.strip()


class _Doc:
    def __init__(self, text: str):
        self.text = text
        self.sents = [_Sentence(p) for p in re.findall(r"[^.!?]*[.!?]+\s*|[^.!?]+$", text) if p]


def page_bytes() -> bytes:
    return (FIXTURES / "evento-sample.html").read_bytes()


def record_bytes() -> bytes:
    return (FIXTURES / "stenografico-sample.xml").read_bytes()


def archive_bytes() -> bytes:
    return (FIXTURES / "archivio-sample.xml").read_bytes()


def parsed_media() -> dict:
    return parse_page(page_bytes(), dict(ARCHIVE_ENTRY), SESSION)


def parsed_proceedings() -> dict:
    return parse_record(record_bytes(), SESSION, StubNLP())
