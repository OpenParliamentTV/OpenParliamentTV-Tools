#! /usr/bin/env python3
"""Parse AT Nationalrat stenographic-protocol HTML into per-speech text records.

The scraper saves one Word-generated protocol HTML per on-camera speech at
``original/proceedings/{session}/{std_id}.html``. Each file is a flat run of
``<p>`` paragraphs; a speaker turn opens with a leading bold header of the form

    <b>Präsident <A HREF="/WWER/PAD_88386/index.shtml">Mag. Wolfgang Sobotka</A>:</b> …
    <b>Abgeordnete <A HREF="/WWER/PAD_01980/…">Mag. Johanna Jachs</A></b> (ÖVP): …

The ``PAD_<n>`` in the href is the person's stable parliament id (it matches the
Mediathek ``redner.pad_intern``), so it doubles as ``originPersonID`` and as the
key that lets the merger mark the on-camera speaker. Plain bold/italic inside a
paragraph (``<b><i>eröffne</i></b>``) is emphasis, not a header — only a leading
bold block carrying a ``PAD`` anchor and a trailing colon qualifies.

Output (intermediate, consumed by the merger), one record per ``std_id``::

    {"stdId": 261958, "originTextID": "261958",
     "people": [{type, label, firstname, lastname, context, role, originPersonID, faction?}, …],
     "textContents": [{type, sourceURI, creator, license, language, originTextID, textBody:[…]}]}
"""

from __future__ import annotations

import argparse
import html as _html
import json
import logging
import os
import re
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent))
    __package__ = "optv.parliaments.AT.parsers"

from optv.parliaments import get_language, get_rights
from optv.parliaments.AT.common import Config
from optv.shared.lang import de as lang_de
from optv.shared.merge_format import split_first_last

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

BASE_URL = "https://www.parlament.gv.at"

# Academic title tokens stripped from a speaker's display name (leading) and
# degree tokens stripped (trailing). Kept conservative so we never eat a name.
_LEADING_TITLES = re.compile(
    r"^(?:(?:MMMag|MMag|Mag|DDDr|DDr|Dr|Dipl\.-Ing|Dipl\.-Kfm|Ing|DI|Prof|"
    r"Univ\.-Prof|Priv\.-Doz|Doz)\.?(?:\s+h\.?\s?c\.?)?\s+)+", re.IGNORECASE)
_TRAILING_DEGREES = re.compile(
    r"[\s,]+(?:BSc|BA|BEd|MA|MAS|MBA|MBL|MSc|MMSc|PhD|LL\.?M\.?|LL\.?B\.?|Bakk\.?|"
    r"MEd|MPH|MIM|CMC|CSE)\b\.?", re.IGNORECASE)

# Leading speaker header. Operates on the *simplified* paragraph HTML (only
# <b>/<strong>/<a> kept). Two colon placements: inside the bold run (chair /
# minister) or after it with a faction in parens (MP).
_HEADER_RE = re.compile(
    r"^\s*<(?:b|strong)>\s*"
    r"(?P<role>[^<]*?)\s*"
    r'<a\s+href="[^"]*PAD_(?P<pad>\d+)[^"]*"[^>]*>(?P<name>[^<]*)</a>'
    r"(?:"
    r"\s*:\s*</(?:b|strong)>"                       # colon inside bold
    r"|"
    r"\s*</(?:b|strong)>\s*(?:\((?P<fac>[^)]*)\))?\s*:"  # colon + optional faction after
    r")",
    re.IGNORECASE | re.DOTALL)

_P_RE = re.compile(r"<p\b[^>]*>(?P<inner>.*?)</p>", re.IGNORECASE | re.DOTALL)
# Tags to drop while keeping b/strong/a (Word styling: spans, italics, o:p, …).
_DROP_TAGS_RE = re.compile(r"</?(?:span|i|u|o:p|font|sub|sup|em|small|big)\b[^>]*>",
                           re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")


def clean_text(s: str) -> str:
    """Unescape entities, drop soft hyphens, normalise NBSP/whitespace."""
    s = _html.unescape(s or "")
    s = s.replace("­", "").replace(" ", " ")
    return re.sub(r"\s+", " ", s).strip()


def clean_person_name(raw: str) -> str:
    """Speaker display name with academic titles/degrees removed."""
    s = clean_text(raw)
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)   # drop a trailing "(ÖVP)" faction tag
    s = _TRAILING_DEGREES.sub("", s)
    prev = None
    while prev != s:
        prev = s
        s = _LEADING_TITLES.sub("", s).strip()
    return s.strip(" ,")


def at_speaker_context(role: str) -> str:
    """Map an Austrian chair/role label to a Stage-2 speaker ``context``.

    The Nationalrat's presiding officers are the Präsident and the Zweite/Dritte
    Präsident:in (deputy presiders) — the latter map to ``vice-president``.
    Everyone else (Abgeordnete, Bundesminister:in, Staatssekretär:in, …) is the
    one speaking on camera ⇒ ``main-speaker``.
    """
    r = (role or "").lower()
    if ("vizepräsident" in r or "zweite präsident" in r or "zweiter präsident" in r
            or "dritte präsident" in r or "dritter präsident" in r
            or "zweiten präsident" in r or "dritten präsident" in r):
        return "vice-president"
    if "schriftführer" in r:
        return "speaker"
    if "präsident" in r:
        return "president"
    return "main-speaker"


def _simplify(inner: str) -> str:
    return _DROP_TAGS_RE.sub("", inner)


def _strip_tags(s: str) -> str:
    return _TAG_RE.sub(" ", s)


def parse_protocol_html(html_text: str, std_id, period: int, *, source_uri: str) -> dict:
    people: dict[str, dict] = {}
    order: list[str] = []
    textbody: list[dict] = []
    current: dict | None = None

    for m in _P_RE.finditer(html_text):
        inner = _simplify(m.group("inner"))
        header = _HEADER_RE.match(inner)
        if header:
            role = clean_text(_strip_tags(header.group("role")))
            pad = str(int(header.group("pad")))   # canonical: strip the href's zero-padding
            label = clean_person_name(header.group("name"))
            fac = clean_text(header.group("fac")) if header.group("fac") else None
            context = at_speaker_context(role)
            if pad not in people:
                first, last = split_first_last(label)
                person = {
                    "type": "memberOfParliament",
                    "label": label,
                    "firstname": first,
                    "lastname": last,
                    "context": context,
                    "role": role or None,
                    "originPersonID": pad,
                }
                if fac and fac.strip():
                    person["faction"] = {"label": fac.strip()}
                people[pad] = person
                order.append(pad)
            current = people[pad]
            body_html = inner[header.end():]
        else:
            body_html = inner

        text = clean_text(_strip_tags(body_html))
        if not text or set(text) <= {"*", " "}:
            continue
        sentences = [{"text": s} for s in lang_de.spacy_sentencize(text)]
        textbody.append({
            "speech_id": str(std_id),
            "type": "speech",
            "speaker": current["label"] if current else None,
            "speakerstatus": current["role"] if current else None,
            "sentences": sentences,
            "text": text,
        })

    rights = get_rights("AT", period, "proceedings")
    return {
        "stdId": int(std_id),
        "originTextID": str(std_id),
        "people": [people[p] for p in order],
        "textContents": [{
            "type": "proceedings",
            "sourceURI": source_uri,
            "creator": rights.get("creator", ""),
            "license": rights.get("license", ""),
            "language": get_language("AT"),
            "originTextID": str(std_id),
            "textBody": textbody,
        }],
    }


def parse_session(config: Config, session: str, period: int) -> dict:
    """Parse every protocol HTML for ``session`` into an intermediate doc."""
    proc_dir = config.dir("proceedings") / session
    records: list[dict] = []
    if not proc_dir.is_dir():
        logger.warning(f"[{session}] no protocol dir {proc_dir}")
    else:
        # Recover each std_id's source URI from the raw Mediathek payload.
        raw_path = config.dir("media") / f"{session}-mediathek.json"
        source_by_std: dict[str, str] = {}
        if raw_path.exists():
            raw = json.loads(raw_path.read_text())
            for deb in raw.get("debatten") or []:
                for r in deb.get("redner") or []:
                    prot = r.get("protokoll") or ""
                    if r.get("std_id") is not None and prot:
                        source_by_std[str(r["std_id"])] = prot if prot.startswith("http") else BASE_URL + prot
        for html_file in sorted(proc_dir.glob("*.html")):
            std_id = html_file.stem
            html_text = html_file.read_text(encoding="latin-1")
            source_uri = source_by_std.get(std_id, "")
            records.append(parse_protocol_html(html_text, std_id, period, source_uri=source_uri))

    return {"meta": {"session": session, "period": period}, "data": records}


def main():
    parser = argparse.ArgumentParser(description="Parse AT protocol HTML into intermediate JSON.")
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", required=True, help="Session key (e.g. 27144)")
    parser.add_argument("--period", type=int, default=27)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    doc = parse_session(config, args.session, args.period)
    out = config.file(args.session, "proceedings", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} speech records)")


if __name__ == "__main__":
    main()
