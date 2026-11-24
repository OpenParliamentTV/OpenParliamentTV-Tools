"""Turn the stenographic record of an IT sitting into ``original/proceedings/{session}-proceedings.json``.

One record per intervention (``intervento``) of the sitting, in the record's order: its id, the debate and phase it stands in, its speaker, and its text as blocks of sentences. The id is the one the WebTV page links, which is what the merger joins by.

The record marks what was not spoken in italics and parentheses: ``(Applausi)``, ``(Segue la votazione)``, ``(Vedi votazione n. 1)``. Such a passage becomes a ``comment`` block; italics without parentheses are emphasis or foreign words and stay in the speech. An intervention the record only narrates (``…, Segretario, legge il processo verbale``) is a comment as a whole. The notices between interventions (the sitting's start, suspension and end, a change of the chair) are kept with the intervention they precede. The annex of texts handed in but not spoken (``allegati``) is left out.

Run on its own: ``python -m optv.parliaments.IT.parsers.proceedings2json <data_dir> --session 190731``.
"""

import argparse
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from lxml import etree

from optv.shared.cli import load_config, setup_logging
from optv.shared.meta import now_iso
from optv.shared.sentence_split import split_long_sentences

from ..common import in_scope, parse_session_key

logger = logging.getLogger(__name__)

PARLIAMENT_ID = Path(__file__).resolve().parents[1].name

# The record as a page, where each intervention has an anchor.
RECORD_PAGE = "https://documenti.camera.it/leg{period}/resoconti/assemblea/html/sed{number:04d}/stenografico.htm"

# Elements that hold interventions under a title of their own.
_DEBATES = ("dibattito", "titoloCalendario")
_NOTICES = ("avviso", "presidenza")

_WS_RE = re.compile(r"\s+")
_HAS_WORD_RE = re.compile(r"\w")
_LEADING_PUNCTUATION_RE = re.compile(r"^[\s.,;:!?]*")
# The end of a sentence, closing quotation marks and brackets included.
_SENTENCE_END_RE = re.compile(r"[.!?…:;][\"'»”’)\]]*$")
# A time the record adds to a title: "(ore 9,38)".
_TITLE_TIME_RE = re.compile(r"\s*\(ore \d{1,2}(?:[,.:]\d{2})?\)\.?")


@dataclass
class Run:
    """A stretch of text and whether the record sets it in italics."""
    text: str
    italic: bool


def _clean(text: str) -> str:
    return _WS_RE.sub(" ", text).strip()


def _runs(element, after=None) -> list[Run]:
    """The text of ``element`` as runs, starting behind its child ``after`` when given."""
    runs: list[Run] = []
    started = after is None
    if started and element.text:
        runs.append(Run(element.text, False))
    for child in element:
        if not started:
            if child is after:
                started = True
                if child.tail:
                    runs.append(Run(child.tail, False))
            continue
        if isinstance(child.tag, str):
            text = "".join(child.itertext())
            if text:
                runs.append(Run(text, child.tag == "emphasis" and child.get("type") == "italic"))
        if child.tail:
            runs.append(Run(child.tail, False))
    return runs


def split_comments(runs: list[Run]) -> list[tuple[str, str]]:
    """A paragraph as ``("speech" | "comment", text)`` pieces in order.

    A parenthesis that opens in italics starts a comment, which runs to the matching closing parenthesis whatever the formatting in between. Punctuation that follows a comment goes back to the speech before it.
    """
    pieces: list[tuple[str, str]] = []
    buffer: list[str] = []
    depth = 0

    def flush(kind: str) -> None:
        text = _clean("".join(buffer))
        buffer.clear()
        if not text:
            return
        if kind == "speech" and pieces and pieces[-1][0] == "comment":
            # "… esplosa (Applausi), perché …": the comma belongs to the words before the comment.
            lead = _LEADING_PUNCTUATION_RE.match(text).group()
            text = text[len(lead):].strip()
            lead = lead.strip()
            previous = next((i for i in range(len(pieces) - 1, -1, -1) if pieces[i][0] == "speech"), None)
            if lead and previous is not None and not pieces[previous][1].endswith(lead[0]):
                pieces[previous] = ("speech", pieces[previous][1] + lead)
            if not text:
                return
        pieces.append((kind, text))

    for run in runs:
        for char in run.text:
            if depth == 0:
                if char == "(" and run.italic:
                    flush("speech")
                    depth = 1
                buffer.append(char)
            else:
                buffer.append(char)
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                    if depth == 0:
                        flush("comment")
    flush("comment" if depth else "speech")
    return pieces


def _closing_parenthesis(text: str) -> int:
    """The index of the parenthesis that closes the one ``text`` starts with, ``-1`` when it stays open."""
    depth = 0
    for i, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return i
    return -1


def split_prefix(runs: list[Run]) -> tuple[Optional[str], Optional[str], bool, list[Run]]:
    """What follows a speaker's name in the record: ``(role, group, narrated, text runs)``.

    The record writes ``NAME. text`` for the chair, ``NAME (GROUP). text`` for a member, ``NAME, role. text`` for a rapporteur or a member of the government, and ``NAME, role, does something.`` where it narrates instead of quoting. A form it does not know is returned unchanged as text.
    """
    rest = [Run(r.text, r.italic) for r in runs]
    role = group = None

    def head() -> str:
        return rest[0].text.lstrip() if rest and not rest[0].italic else ""

    def consume(count: int) -> None:
        rest[0] = Run(rest[0].text.lstrip()[count:], False)

    if head().startswith(","):
        consume(1)
        if not head().strip() and len(rest) > 1 and rest[1].italic:
            role = _clean(rest[1].text) or None
            del rest[:2]
        else:
            return None, None, False, runs
    if head().startswith("("):
        end = _closing_parenthesis(head())
        if end > 0:
            group = _clean(head()[1:end]) or None
            consume(end + 1)
    if head().startswith("."):
        consume(1)
        return role, group, False, rest
    if role and head().startswith(","):
        consume(1)
        return role, group, True, rest
    return None, None, False, runs


def clean_title(text: str) -> str:
    """A title of the record without the times it repeats behind it and without the closing full stop."""
    return _clean(_TITLE_TIME_RE.sub("", text)).rstrip(".").strip()


def _title(element) -> Optional[str]:
    title = element.find("titolo")
    return clean_title("".join(title.itertext())) if title is not None else None


def _strip_parentheses(title: Optional[str]) -> Optional[str]:
    if title and title.startswith("(") and title.endswith(")"):
        return title[1:-1].strip()
    return title


def chair_context(presidency: str) -> str:
    """``president`` or ``vice-president`` from the record's ``PRESIDENZA DEL VICEPRESIDENTE …`` line."""
    return "vice-president" if "VICEPRESIDENT" in presidency.upper() else "president"


def intervention_record(element, *, debate, phase, chair_role: str) -> dict:
    """One intervention with its speaker and its text as kind-tagged pieces, not yet split into sentences."""
    body = element.find("testoXHTML")
    name = body.find("nominativo") if body is not None else None
    speaker = None
    role = group = None
    narrated = False
    first = _runs(body, after=name) if body is not None else []
    if name is not None:
        after_name = first
        role, group, narrated, first = split_prefix(after_name)
        if first is after_name:
            logger.warning(f"{element.get('id')}: speaker prefix in a form not known, kept as text: "
                           f"{_clean(''.join(r.text for r in after_name))[:60]!r}")
        written = _clean("".join(name.itertext()))
        is_chair = written.upper() == "PRESIDENTE"
        speaker = {
            "name": _clean(name.get("cognomeNome") or "") or written,
            "personId": name.get("id") or None,
            "chair": is_chair,
            "role": role,
            "group": group,
        }
        if is_chair:
            speaker["chairContext"] = chair_role
    paragraphs = [first] + [_runs(p) for p in element.iterfind("interventoVirtuale")]
    if narrated:
        # The record tells what the person did; nothing of it is a quotation.
        told = _clean("".join(body.itertext()))
        pieces = [[("comment", told)]] if told else []
    else:
        pieces = [split_comments(p) for p in paragraphs]
    return {
        "id": element.get("id"),
        "debateId": debate.get("id") if debate is not None else None,
        "debateTitle": _title(debate) if debate is not None else None,
        "phaseId": phase.get("id") if phase is not None else None,
        "phaseTitle": _strip_parentheses(_title(phase)) if phase is not None else None,
        "speaker": speaker,
        "narrated": narrated,
        "notesBefore": [],
        "notesAfter": [],
        "paragraphs": [p for p in pieces if p],
    }


def read_record(xml: bytes) -> tuple[dict, list[dict]]:
    """The sitting's attributes and its interventions in order, the annex left out."""
    root = etree.fromstring(xml)
    record = root.find("resoconto")
    if root.tag != "seduta" or record is None:
        raise ValueError("not a stenographic record: no seduta/resoconto")
    interventions: list[dict] = []
    pending_notes: list[str] = []
    chair_role = "president"

    def walk(element, debate, phase) -> None:
        nonlocal chair_role
        for child in element:
            if not isinstance(child.tag, str) or child.tag == "allegati":
                continue
            if child.tag in _NOTICES:
                note = _clean("".join(child.itertext()))
                if child.tag == "presidenza":
                    chair_role = chair_context(note)
                if note:
                    pending_notes.append(note)
            elif child.tag == "intervento":
                item = intervention_record(child, debate=debate, phase=phase, chair_role=chair_role)
                item["notesBefore"] = list(pending_notes)
                pending_notes.clear()
                interventions.append(item)
            elif child.tag in _DEBATES:
                walk(child, child, None)
            elif child.tag == "fase":
                walk(child, debate, child)

    walk(record, None, None)
    if pending_notes and interventions:
        interventions[-1]["notesAfter"] = list(pending_notes)
    return dict(root.attrib), interventions


def sentence_spans(doc) -> list[str]:
    """The sentences of a parsed text.

    The model's boundaries are taken where the text before them ends a sentence (``. ! ? … : ;``). A piece that ends without such a mark, or that has no word in it (a lone ``…`` before a resumed speech), joins the piece after it. What is left at the end is a sentence of its own when it has a word, and joins the sentence before it otherwise.
    """
    sentences: list[str] = []
    pending = ""
    for sent in doc.sents:
        pending += sent.text_with_ws
        text = pending.strip()
        if _HAS_WORD_RE.search(text) and _SENTENCE_END_RE.search(text):
            sentences.append(pending)
            pending = ""
    if pending.strip():
        if _HAS_WORD_RE.search(pending) or not sentences:
            sentences.append(pending)
        else:
            sentences[-1] += pending
    return [text for raw in sentences if (text := raw.strip())]


def _sentencize(nlp, texts: list[str]) -> list[list[str]]:
    return [split_long_sentences(sentence_spans(doc)) or [doc.text.strip()] for doc in nlp.pipe(texts)]


def to_blocks(interventions: list[dict], nlp) -> None:
    """Replace each intervention's ``paragraphs`` by ``blocks`` of sentences, splitting all spoken text in one pass over the model. A comment stays one sentence."""
    spoken = [text for item in interventions for paragraph in item["paragraphs"]
              for kind, text in paragraph if kind == "speech"]
    sentences = iter(_sentencize(nlp, spoken))
    for item in interventions:
        blocks = []
        chars = 0
        for paragraph in item.pop("paragraphs"):
            for kind, text in paragraph:
                if kind == "speech":
                    chars += len(text)
                    blocks.append({"kind": "speech", "sentences": next(sentences)})
                else:
                    blocks.append({"kind": "comment", "sentences": [text]})
        item["blocks"] = blocks
        item["chars"] = chars


def load_nlp(spacy_model: str):
    """The sentence splitter: the Italian model without the parts sentence boundaries do not need."""
    import spacy
    logger.info(f"Loading spaCy model {spacy_model}")
    return spacy.load(spacy_model, exclude=["ner", "lemmatizer"])


def parse_record(xml: bytes, session: str, nlp) -> dict:
    """The proceedings document of one sitting from its record XML."""
    period, number = parse_session_key(session)
    attributes, interventions = read_record(xml)
    if (attributes.get("legislatura"), attributes.get("numero")) != (str(period), str(number)):
        raise ValueError(f"[{session}] the record is of sitting {attributes.get('numero')} "
                         f"of legislature {attributes.get('legislatura')}")
    to_blocks(interventions, nlp)
    date = None
    if all(attributes.get(k, "").isdigit() for k in ("anno", "mese", "giorno")):
        date = f"{int(attributes['anno']):04d}-{int(attributes['mese']):02d}-{int(attributes['giorno']):02d}"
    logger.info(f"[{session}] {len(interventions)} interventions in the record, "
                f"{sum(1 for i in interventions if i['chars'])} with spoken text")
    return {
        "meta": {
            "session": session,
            "legislature": period,
            "sitting": number,
            "date": date,
            "sourceURI": RECORD_PAGE.format(period=period, number=number),
            "processing": {"parse_proceedings": now_iso()},
        },
        "data": interventions,
    }


def parse_session(config, session: str, nlp) -> Path:
    doc = parse_record(config.raw_record(session).read_bytes(), session, nlp)
    out = config.file(session, "proceedings", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def parse_proceedings(config, args) -> None:
    """Parse the record of every session in scope whose parsed file is missing or older than the record."""
    nlp = None
    for session in config.sessions():
        raw = config.raw_record(session)
        out = config.file(session, "proceedings")
        if not in_scope(args, session) or not raw.exists():
            continue
        if out.exists() and out.stat().st_mtime >= raw.stat().st_mtime and not getattr(args, "force", False):
            continue
        if nlp is None:
            from optv.parliaments import get_locale
            nlp = load_nlp(get_locale(PARLIAMENT_ID)["spacy_model"])
        parse_session(config, session, nlp)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", action="append", default=[], help="session key, e.g. 190731 (repeatable); default: all")
    parser.add_argument("--force", action="store_true", help="parse again what is already parsed")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    setup_logging(args.debug)
    args.limit_to_period = False
    args.limit_session = "^(" + "|".join(re.escape(s) for s in args.session) + ")$" if args.session else ""
    parse_proceedings(load_config(PARLIAMENT_ID, args.data_dir), args)


if __name__ == "__main__":
    main()
