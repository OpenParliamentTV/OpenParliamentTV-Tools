#! /usr/bin/env python3
"""Parse DE-NW Plenarprotokolle (HTML version) into proceedings text turns.

Input per session: ``original/proceedings/{sid}.html`` — the Word-export HTML of
the Plenarprotokoll. Output: ``original/proceedings/{sid}-proceedings.json``, one
**rede** per Mediathek spine entry, which the merger joins onto the spine per
agenda item (``join_text_to_spine``).

The protocol body runs from the first speaker line to the first ``sSchluss``
paragraph ("Schluss: … Uhr"); the appendix after it holds speeches given only to
the record ("zu Protokoll gegebene Reden"), which have no video. Paragraph
classes:

- ``rRednerkopf`` — a speaker line ("Klaus Esser*) (AfD): …", "Präsident André
  Kuper: …", "Verena Schäffer, Ministerin für …: …"); the text after the first
  colon is the first speech paragraph;
- ``fZwischenfrage`` — the same for an interposed question or a short
  intervention (Kurzintervention);
- ``aStandardabsatz`` / ``zZitat`` / ``t1AbsatznachTOP`` — speech text (the last
  is the chair's text right after an agenda heading);
- ``kKlammer`` — an interjection, kept as a ``comment`` body;
- ``t-N-O…`` — a numbered agenda heading ("1 Stauland NRW: …"); other ``t-…``
  classes are sub-titles of the same item.

The Mediathek speech list has one entry per speech: interposed questions and
short interventions fall within the range of the speech they interrupt, and the
chair has an entry of its own only for the opening of the sitting. The protocol's
speaker lines are grouped accordingly (``group_redes``):

1. A numbered agenda heading closes the current rede.
2. Chair lines before the first numbered heading form the opening rede, keyed to
   the chair.
3. Other chair lines join the current rede; with no open rede (between a heading
   and the item's first speaker) they are dropped.
4. ``fZwischenfrage`` lines join the current rede, and their speaker is
   remembered as a questioner of that rede.
5. A ``rRednerkopf`` line by the rede's main speaker or one of its questioners
   joins the rede (a speech interrupted by the chair, an answer to a question, a
   short intervention continued after the chair).
6. Any other ``rRednerkopf`` line opens a new rede.

Bodies use the shape of :mod:`optv.shared.pdf2tei.merge`: each speech body is
tagged with ``speakerName``/``speakerKey`` (plus ``chairContext`` for a chair
line), and each rede lists its presiding officers in ``chairs``, so
``join_text_to_spine`` attributes chair and question text to the right person
and adds the chair to ``people[]``.
"""
from __future__ import annotations

import argparse
import datetime
import logging
import re
import sys
from pathlib import Path
from typing import Callable, Optional

from lxml import html as lxml_html

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-NW.parsers"

from optv.shared.lang.de import (
    is_non_speech,
    join_segments,
    match_key_surname,
    pdf_chair_context,
    spacy_sentencize,
    strip_honorifics,
)

logger = logging.getLogger(__name__)

PARLIAMENT_ID = "DE-NW"

SPEAKER_CLASS = "rRednerkopf"
QUESTION_CLASS = "fZwischenfrage"
SPEECH_CLASSES = ("aStandardabsatz", "zZitat", "t1AbsatznachTOP")
COMMENT_CLASS = "kKlammer"
END_CLASS = "sSchluss"
NUMBERED_HEADING_PREFIX = "t-N-O"

_CHAIR_RE = re.compile(r"^(?P<role>(?:Alters|Vize)?[Pp]räsident(?:in)?)\s+(?P<name>.+)$")
_PARTY_RE = re.compile(r"^(?P<name>.+?)\s*\((?P<party>[^()]+)\)$")
_TOP_NR_RE = re.compile(r"^(?P<nr>\d+[a-z]?)\b\s*(?P<title>.*)$")


def _clean(text: str) -> str:
    text = (text or "").replace("\xad", "").replace("‑", "-")
    return re.sub(r"\s+", " ", text).strip()


def _classes(el) -> list[str]:
    return (el.get("class") or "").split()


def parse_speaker_head(head: str) -> dict:
    """Classify a speaker line's head (the text before the first colon).

    Returns ``{kind, name, party, role, key}`` with ``kind`` one of ``chair``,
    ``government``, ``member``, ``other``. ``name`` is the natural-order name
    without footnote marker, office or party.
    """
    head = _clean(head.replace("*)", ""))
    m = _CHAIR_RE.match(head)
    if m:
        name = m.group("name").strip()
        return {"kind": "chair", "name": name, "party": "", "role": m.group("role"),
                "key": match_key_surname(name)}
    m = _PARTY_RE.match(head)
    if m:
        name = m.group("name").strip()
        return {"kind": "member", "name": name, "party": m.group("party").strip(),
                "role": "", "key": match_key_surname(name)}
    if "," in head:
        # "Verena Schäffer, Ministerin für Kinder, Jugend, …" — the office may
        # itself contain commas, so cut at the first one.
        name, role = (p.strip() for p in head.split(",", 1))
        return {"kind": "government", "name": name, "party": "", "role": role,
                "key": match_key_surname(name)}
    return {"kind": "other", "name": head, "party": "", "role": "",
            "key": match_key_surname(head)}


def parse_protocol(html_bytes: bytes) -> list[dict]:
    """Read the protocol body into utterances (one per speaker line).

    Each utterance: ``{kind, isQuestion, name, party, role, key, top,
    agendaTitle, line, parts: [(type, text)]}`` where ``line`` is the running
    number of the speaker line and ``parts`` the ordered ``speech``/``comment``
    paragraphs.
    """
    doc = lxml_html.fromstring(html_bytes)
    utterances: list[dict] = []
    started = False
    top, agenda = "", ""
    cur: dict | None = None
    line = 0
    for p in doc.iter("p"):
        classes = _classes(p)
        if not classes:
            continue
        cls = classes[0]
        if cls in (SPEAKER_CLASS, QUESTION_CLASS):
            started = True
        if not started:
            continue
        if cls == END_CLASS:
            break
        text = _clean(p.text_content())
        if cls.startswith(NUMBERED_HEADING_PREFIX):
            m = _TOP_NR_RE.match(text)
            if m:
                top, agenda = m.group("nr").lower(), m.group("title")
                utterances.append({"kind": "heading", "top": top, "agendaTitle": agenda})
                cur = None
            continue
        if cls in (SPEAKER_CLASS, QUESTION_CLASS):
            line += 1
            head, _, rest = text.partition(":")
            cur = {**parse_speaker_head(head), "isQuestion": cls == QUESTION_CLASS,
                   "top": top, "agendaTitle": agenda, "line": line, "parts": []}
            utterances.append(cur)
            if rest.strip():
                cur["parts"].append(("speech", rest.strip()))
            continue
        if cur is None or not text:
            continue
        if cls.startswith(SPEECH_CLASSES):
            cur["parts"].append(("speech", text))
        elif cls == COMMENT_CLASS:
            cur["parts"].append(("comment", text))
    return utterances


def group_redes(utterances: list[dict]) -> list[dict]:
    """Group utterances into one rede per spine entry (rules in the module
    docstring). Each rede: ``{main, top, agendaTitle, line, members}`` where
    ``main`` is the utterance that opened it and ``members`` all its utterances
    in order."""
    redes: list[dict] = []
    cur: dict | None = None
    seen_heading = False

    def close():
        nonlocal cur
        if cur is not None:
            redes.append(cur)
        cur = None

    def open_rede(u):
        nonlocal cur
        close()
        cur = {"main": u, "top": u["top"], "agendaTitle": u["agendaTitle"],
               "line": u["line"], "questioners": set(), "members": [u]}

    for u in utterances:
        if u["kind"] == "heading":
            close()
            seen_heading = True
            continue
        if u["kind"] == "chair" and not u["isQuestion"]:
            if cur is not None:
                cur["members"].append(u)
            elif not seen_heading:
                open_rede(u)
            continue
        if u["isQuestion"]:
            if cur is not None:
                cur["members"].append(u)
                cur["questioners"].add(u["key"])
            continue
        if cur is not None and (u["key"] == cur["main"]["key"]
                                or u["key"] in cur["questioners"]):
            cur["members"].append(u)
            continue
        open_rede(u)
    close()
    return redes


def _person_name(u: dict) -> str:
    return strip_honorifics(u["name"])


def _chair(u: dict) -> dict:
    """``{name, firstname, lastname, context}`` for a chair utterance."""
    name = _person_name(u)
    parts = name.split()
    chair = {"name": name, "context": pdf_chair_context(u["role"])}
    if len(parts) > 1:
        chair["firstname"] = " ".join(parts[:-1])
    if parts:
        chair["lastname"] = parts[-1]
    return chair


def _bodies(u: dict, sentencize) -> list[dict]:
    """Typed textBody entries for one utterance, tagged with its speaker."""
    tag = {"speakerName": _person_name(u), "speakerKey": u["key"]}
    if u["kind"] == "chair":
        tag["chairContext"] = pdf_chair_context(u["role"])
    out: list[dict] = []
    buf: list[str] = []

    def flush():
        if not buf:
            return
        sents = [{"text": s} for s in sentencize(join_segments(buf))
                 if not is_non_speech(s)]
        buf.clear()
        if sents:
            out.append({"type": "speech", "sentences": sents, **tag})

    for kind, text in u["parts"]:
        if kind == "speech":
            buf.append(text)
        else:
            flush()
            out.append({"type": "comment", "sentences": [{"text": text}]})
    flush()
    return out


def protocol_to_turns(html_bytes: bytes, sid: str, *,
                      sentencize: Optional[Callable[[str], list[str]]] = None
                      ) -> list[dict]:
    """Parse one HTML protocol into spine-granularity proceedings turns."""
    sentencize = sentencize or spacy_sentencize
    turns: list[dict] = []
    for i, rede in enumerate(group_redes(parse_protocol(html_bytes)), start=1):
        main = rede["main"]
        bodies = [b for u in rede["members"] for b in _bodies(u, sentencize)]
        chairs: list[dict] = []
        for u in rede["members"]:
            if u["kind"] == "chair" and all(c["name"] != _person_name(u) for c in chairs):
                chairs.append(_chair(u))
        label = _person_name(main)
        if main["party"]:
            label = f"{label} ({main['party']})"
        turns.append({
            "index": i,
            "matchKey": main["key"],
            "speaker": label,
            "isChair": main["kind"] == "chair",
            "topNumber": rede["top"],
            "agendaTitle": rede["agendaTitle"],
            "originTextID": f"{sid}-{rede['line']}",
            "sentences": [s for b in bodies if b["type"] == "speech"
                          for s in b["sentences"]],
            "bodies": bodies,
            "chairs": chairs,
        })
    return turns


def protocol_to_proceedings_doc(html_bytes: bytes, sid: str, **kw) -> dict:
    return {
        "meta": {
            "session": sid,
            "parliament": PARLIAMENT_ID,
            "processing": {
                "parse_proceedings":
                    datetime.datetime.now(datetime.timezone.utc)
                    .replace(tzinfo=None).isoformat(timespec="seconds"),
            },
        },
        "data": protocol_to_turns(html_bytes, sid, **kw),
    }


def parse_proceedings_for_session(config, session: str, *, force: bool = False):
    src = config.dir('proceedings') / f"{session}.html"
    if not src.exists():
        logger.info(f"[{session}] no protocol at {src} — skipping proceedings parse")
        return None
    out = config.file(session, 'proceedings')
    if not force and out.exists() and out.stat().st_mtime >= src.stat().st_mtime:
        logger.debug(f"[{session}] {out.name} is up to date")
        return out
    doc = protocol_to_proceedings_doc(src.read_bytes(), session)
    out = config.save_data(doc, session, 'proceedings')
    logger.info(f"[{session}] wrote {out.name} ({len(doc['data'])} turns)")
    return out


def parse_proceedings_directory(config, args=None):
    proc_dir = config.dir('proceedings')
    sources = sorted(proc_dir.glob("*.html")) if proc_dir.is_dir() else []
    if not sources:
        logger.info("No Plenarprotokoll HTML files to parse.")
        return
    force = bool(getattr(args, "force", False))
    session_filter = getattr(args, "limit_session", None)
    for src in sources:
        if session_filter and not re.match(session_filter, src.stem):
            continue
        parse_proceedings_for_session(config, src.stem, force=force)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", help="single session id (else all protocols)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s %(message)s")
    from ..common import Config
    config = Config(args.data_dir)
    if args.session:
        parse_proceedings_for_session(config, args.session, force=args.force)
    else:
        parse_proceedings_directory(config, args)
