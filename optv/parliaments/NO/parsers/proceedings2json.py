#! /usr/bin/env python3
"""Parse a Storting publikasjon XML (Referat) into Stage-2-shaped JSON.

A Referat (``Forhandlinger / Mote``) holds its speech-bearing elements in two
places, both walked in document order:

  - ``Startseksjon`` — the procedural opening (``Motestart``; on a constituent
    meeting also ``Folkevalgte / Sammentreden``). Emitted as one procedural
    agenda item.
  - ``Hovedseksjon / Saker`` — every child is one agenda item:
    ``Sak`` (debate item), ``Sporretime`` (question time), ``Interpellasjon``,
    ``Sakreferat`` (the Referat item), and the voting block
    (``VoteringerStart``; ``Voteringer`` directly under ``Saker`` in
    2016-2017). The agenda number is ``@saksKartNr`` or, where that attribute
    is missing, the "Sak nr. N" in ``Sakshode / Saknr``. A voting block has no
    number of its own (the ``saksKartNr`` inside it names the item voted on)
    and becomes ``votering-{k}``.

The recognised speech elements are:

  - ``Hovedinnlegg`` — main speech (``Navn personID=…``)
  - ``Replikk``      — reply (``Navn personID=…``)
  - ``Presinnlegg``  — president's remark (Navn has no personID)

Each element's opening ``<A>`` paragraph holds a ``<Navn>`` whose text is e.g.
``"Erlend Wiborg (FrP) [10:00:07]:"``; the ``[HH:MM:SS]`` clock time (Europe/
Oslo) is the anchor the merger turns into a video offset against the Qbrick
``custom.TC_in``. The spoken text is every ``<A>`` under the element in
document order — including quotations (``Sitat``) and list items (``Liste``)
— except table cells (``Tbl``) and footnotes.

**Elements without a clock time** (most president's remarks in voting blocks,
and most president's remarks in Referats before 2025-2026) cannot be placed
on the video by themselves. They are not emitted as records: their text is
appended as a further ``textBody`` block to the preceding record in document
order, whose clip spans the time in which they were spoken (or prepended to
the first record when nothing precedes them). ``debug.foldedElements`` counts
them per record. ``[00:00:00]`` is a placeholder and counts as no time.

Output: ``original/proceedings/{session}-proceedings.json`` (via Config
helper) shaped as ``{"meta": {...}, "data": [<speech>, ...]}``. Per-speech
entries follow the OPTV Stage 2 shape but **omit ``media``** — that is filled
in by the merger from the Qbrick-derived part metadata. ``dateStart`` is an
ISO timestamp with the Europe/Oslo offset.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import lxml.etree as ET

from optv.parliaments.NO.common import Config, period_to_sesjonider
from optv.shared.agenda_types import classify_no
from optv.shared.sentence_split import split_long_sentences
from optv.parliaments import get_rights as _get_rights
from optv.parliaments import get_language as _get_language

logger = logging.getLogger(__name__ if __name__ != "__main__" else os.path.basename(sys.argv[0]))

PARLIAMENT_CODE = "NO"
LANGUAGE_CODE = _get_language("NO")
SPEECH_CREATOR = _get_rights("NO", stream="proceedings")["creator"]
SPEECH_LICENSE = _get_rights("NO", stream="proceedings")["license"]
REFERAT_URL_TEMPLATE = ("https://www.stortinget.no/no/Saker-og-publikasjoner/"
                        "Publikasjoner/Referater/Stortinget/{sesjon_path}/"
                        "{referat_id}?all=true")

TZ_OSLO = ZoneInfo("Europe/Oslo")
SPEECH_TAGS = ("Hovedinnlegg", "Replikk", "Presinnlegg")
VOTING_TAGS = ("VoteringerStart", "Voteringer")
# ``<A>`` paragraphs under these are not read out (tables) or not speech.
_SKIP_TEXT_ANCESTORS = ("Tbl", "Fotnote")
CHAIR_CONTEXTS = ("president", "vice-president", "interim-president")

PROCEDURAL_AGENDA: dict[str, Any] = {
    "nr": None, "id": None, "officialTitle": "Procedural",
    "title": "Procedural intros", "fullTitle": "Procedural intros",
    "nativeType": None, "type": "procedural",
}

# `<Navn>` text shape: "First Last (Party) [HH:MM:SS]:" or
#                     "Statsminister First Last [HH:MM:SS]:" (no party for
#                     ministers — the role title takes the slot the party
#                     would otherwise occupy) or "Presidenten [HH:MM:SS]:"
_NAVN_TIME_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\]")
_NAVN_PARTY_RE = re.compile(r"\(([^)]+)\)")
_SAKNR_RE = re.compile(r"\bSak\s+nr\.?\s*(\d+)", re.I)
# Minister/role titles. We strip them off the front to recover the name.
_ROLE_PREFIXES_NO: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"^statsminister(?:en)?\s+", re.I), "statsminister", "main-speaker"),
    (re.compile(r"^utenriksminister(?:en)?\s+", re.I), "utenriksminister", "main-speaker"),
    (re.compile(r"^finansminister(?:en)?\s+", re.I), "finansminister", "main-speaker"),
    (re.compile(r"^justis-?\s*og\s*beredskapsminister(?:en)?\s+", re.I), "justis- og beredskapsminister", "main-speaker"),
    # Generic "<X>minister(en) <name>" catch-all (case-insensitive). Matches
    # "Energiministeren", "Forsvarsminister", "Klima- og miljøminister", …
    (re.compile(r"^([\wåäöæøÅÄÖÆØ\-]+?minister(?:en)?)\s+", re.I), None, "main-speaker"),
    # Storting officers
    (re.compile(r"^visepresident(?:en)?\s+", re.I), "visepresident", "vice-president"),
    (re.compile(r"^president(?:en)?\b\s*", re.I), "president", "president"),
    # Committee chairs etc. — keep as main-speaker, prefix the role onto
    # ``role`` and let NER pick up the name normally.
    (re.compile(r"^(komiteens leder|saksordfører(?:en)?|sakens ordfører(?:en)?)\s+", re.I),
     None, "main-speaker"),
]

_NLP_CACHE: dict[str, Any] = {}


def _load_nlp(model: str):
    """Load a spaCy model once per process."""
    if model not in _NLP_CACHE:
        import spacy
        logger.info(f"Loading spaCy model {model}")
        _NLP_CACHE[model] = spacy.load(model)
    return _NLP_CACHE[model]


def _collapse(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _navn_text(navn: ET._Element) -> str:
    """Plain-text content of a ``<Navn>`` element with whitespace collapsed.

    Note: ``ET.tostring(navn, method='text')`` includes the element's *tail*,
    which for ``<Navn>`` is the rest of the speech body. We use ``itertext()``
    instead — that yields only the descendant text, no tails outside.
    """
    return _collapse("".join(navn.itertext()))


def parse_navn(navn: ET._Element) -> dict:
    """Decode a ``<Navn>`` element into speaker metadata.

    Returns ``{label, person_id, party, role, context, time}`` where ``time``
    is ``HH:MM:SS`` or ``None`` (also for the ``00:00:00`` placeholder).
    ``person_id`` is the Stortinget ``personID`` attribute when present.
    """
    raw = _navn_text(navn)
    person_id = (navn.get("personID") or "").strip() or None
    # Trailing colon
    cleaned = re.sub(r":\s*$", "", raw).strip()
    # Clock time
    time_str = None
    m = _NAVN_TIME_RE.search(cleaned)
    if m:
        time_str = f"{m.group(1)}:{m.group(2)}:{m.group(3)}"
        if time_str == "00:00:00":
            time_str = None
        cleaned = (cleaned[:m.start()] + cleaned[m.end():]).strip()
    # Party — only consume bracketed party at the very end of the cleaned label.
    party = None
    party_match = re.search(r"\(([^)]+)\)\s*$", cleaned)
    if party_match:
        party = party_match.group(1).strip()
        cleaned = cleaned[:party_match.start()].strip()
    # Role prefix
    role: str | None = None
    context = "main-speaker"
    # ``Presidenten`` / ``Presidenten:`` / ``Visepresidenten ...`` etc.
    if re.fullmatch(r"(?i)president(?:en)?", cleaned):
        return {"label": "Presidenten", "person_id": person_id, "party": None,
                "role": "president", "context": "president", "time": time_str}
    if re.fullmatch(r"(?i)visepresident(?:en)?", cleaned):
        return {"label": "Visepresidenten", "person_id": person_id, "party": None,
                "role": "visepresident", "context": "vice-president", "time": time_str}
    for pat, role_label, ctx in _ROLE_PREFIXES_NO:
        m = pat.match(cleaned)
        if m:
            # If pattern captured the role group, use it; else use the literal
            # role_label provided.
            captured = (m.groups() or [None])[0]
            role = role_label or (captured.strip() if captured else None)
            cleaned = cleaned[m.end():].strip()
            context = ctx
            break
    # Sometimes the bracketed party hides inside the inner name string (e.g.
    # the time was first, then "(FrP)"). One last sweep.
    if not party:
        party_match = _NAVN_PARTY_RE.search(cleaned)
        if party_match:
            party = party_match.group(1).strip()
            cleaned = (cleaned[:party_match.start()] + cleaned[party_match.end():]).strip()
    label = cleaned or (role.title() if role else "Unknown")
    return {"label": label, "person_id": person_id, "party": party,
            "role": role, "context": context, "time": time_str}


def _paragraph_text(a: ET._Element) -> str:
    """Text of one ``<A>`` without its ``<Navn>`` and footnotes."""
    copy = ET.fromstring(ET.tostring(a))
    for el in copy.xpath(".//Navn | .//Fotnote"):
        parent = el.getparent()
        if parent is None:
            continue
        if el.tail:
            prev = el.getprevious()
            if prev is not None:
                prev.tail = (prev.tail or "") + el.tail
            else:
                parent.text = (parent.text or "") + el.tail
        parent.remove(el)
    text = ET.tostring(copy, method="text", encoding="unicode", with_tail=False)
    text = text.replace("­", "").replace(" ", " ")
    return _collapse(text)


def element_paragraphs(el: ET._Element) -> list[str]:
    """One cleaned plain-text paragraph per spoken ``<A>`` under ``el``.

    Covers direct paragraphs, quotations (``Sitat/A``) and list items
    (``Liste/Pkt/A``) in document order; skips table cells and footnotes.
    """
    paragraphs: list[str] = []
    for a in el.iter("A"):
        if _inside_skipped(a, el):
            continue
        text = _paragraph_text(a)
        if text:
            paragraphs.append(text)
    return paragraphs


def _inside_skipped(a: ET._Element, el: ET._Element) -> bool:
    """True if ``a`` sits in a table or footnote below ``el``."""
    for anc in a.iterancestors():
        if anc is el:
            return False
        if anc.tag in _SKIP_TEXT_ANCESTORS:
            return True
    return False


def _sentence_split(nlp, paragraphs: list[str]) -> list[dict]:
    sentences: list[dict] = []
    for para in paragraphs:
        doc = nlp(para)
        sents = [s.text.strip() for s in doc.sents if s.text.strip()]
        # Generic (punctuation-only) length-gated split of over-long sentences.
        sentences.extend({"text": t} for t in split_long_sentences(sents))
    return sentences


def agenda_info(container: ET._Element, voting_ordinal: int = 0) -> dict:
    """Agenda metadata for one child of ``<Saker>``.

    Returns ``{nr, id, officialTitle, title, fullTitle, nativeType, type}``.
    """
    if container.tag in VOTING_TAGS:
        native, core = classify_no("Votering")
        return {"nr": None, "id": f"votering-{voting_ordinal or 1}",
                "officialTitle": "Votering", "title": "Votering",
                "fullTitle": "Votering", "nativeType": native, "type": core}
    nr: int | None = None
    try:
        nr = int(container.get("saksKartNr") or "0") or None
    except (TypeError, ValueError):
        nr = None
    if nr is None:
        saknr = container.find("./Sakshode/Saknr")
        if saknr is not None:
            m = _SAKNR_RE.search(_collapse("".join(saknr.itertext())))
            nr = int(m.group(1)) if m else None
    title_el = container.find("./Sakshode/Saktittel")
    if title_el is None:
        title_el = container.find(".//Saktittel")
    if title_el is None:
        title_el = container.find("./Tittel")
    full = _collapse("".join(title_el.itertext())) if title_el is not None else ""
    # The bracketed "(trontaledebatt)" suffix is the most useful classification
    # signal; keep it in the full title but use the cleaner version for short.
    short = re.sub(r"\s*\([^)]*\)\s*$", "", full)
    native, core = classify_no(full)
    return {
        "nr": nr,
        "id": f"sak-{nr}" if nr else None,
        "officialTitle": f"sak nr. {nr}" if nr else (short or full or "Procedural"),
        "title": short or full or "Procedural",
        "fullTitle": full,
        "nativeType": native,
        "type": core,
    }


def _moteid_from_root(root: ET._Element) -> int | None:
    mote = root.find(".//Mote")
    if mote is None:
        return None
    try:
        return int(mote.get("moteID") or "0") or None
    except (TypeError, ValueError):
        return None


def _meeting_date_iso(meeting: dict) -> tuple[datetime.date, str]:
    """Decode ``mote_dato_tid`` (``/Date(epoch_ms+TZ)/``) to ``(date, iso)``."""
    raw = meeting["mote_dato_tid"]
    m = re.match(r"/Date\((-?\d+)([+-]\d{4})?\)/", raw)
    if not m:
        raise ValueError(f"Unexpected mote_dato_tid: {raw!r}")
    epoch_ms = int(m.group(1))
    dt = datetime.datetime.fromtimestamp(epoch_ms / 1000, tz=datetime.timezone.utc)
    local_dt = dt.astimezone(TZ_OSLO)
    return local_dt.date(), local_dt.isoformat(timespec="seconds")


def _sesjon_for_meeting(meeting: dict, config: Config) -> str | None:
    """Walk the cached meetings overviews to find which sesjonid contains
    this meeting."""
    target = meeting["id"]
    for path in config.dir("meetings").glob("*.json"):
        doc = json.loads(path.read_text())
        if any(m.get("id") == target for m in doc.get("moter_liste") or []):
            return doc.get("sesjon_id") or path.stem
    return None


class _Clock:
    """Turn ``HH:MM:SS`` clock times into Europe/Oslo datetimes.

    Rolls over to the next day when a meeting runs past midnight (a time
    before 04:00 after one at or after 20:00).
    """

    def __init__(self, meeting_date: datetime.date):
        self.day = meeting_date
        self.last: datetime.time | None = None

    def at(self, hhmmss: str) -> datetime.datetime:
        t = datetime.time.fromisoformat(hhmmss)
        if self.last is not None and self.last.hour >= 20 and t.hour < 4:
            self.day = self.day + datetime.timedelta(days=1)
        self.last = t
        return datetime.datetime.combine(self.day, t, tzinfo=TZ_OSLO)


def _speaker_for(speech_el: ET._Element) -> dict:
    navn_el = speech_el.find(".//Navn")
    if navn_el is not None:
        speaker = parse_navn(navn_el)
    else:
        speaker = {"label": "Unknown", "person_id": None, "party": None,
                   "role": None, "context": "Unknown", "time": None}
    if speech_el.tag == "Presinnlegg" and speaker.get("context") == "main-speaker":
        # The president is the speaker.
        speaker["context"] = "president"
        speaker["label"] = speaker["label"] or "Presidenten"
    return speaker


def _person(speaker: dict, context: str) -> dict:
    person: dict[str, Any] = {"label": speaker["label"], "context": context}
    if speaker.get("role"):
        person["role"] = speaker["role"]
    if speaker.get("party") and context not in CHAIR_CONTEXTS:
        person["faction"] = {"label": speaker["party"]}
    if speaker.get("person_id"):
        person["originPersonID"] = speaker["person_id"]
    return person


def _text_id(speech_el: ET._Element, referat_id: str, clock: str | None,
             fallback: str, used: set[str]) -> str:
    """A text id that is unique across Referats and survives Referat versions.

    Timed elements are keyed ``{referat_id}-{HH:MM:SS}``, the form Qbrick
    events use as ``reference_id``. The element ``Id`` is no stable key: a
    Referat gets new ``Id``s with every version until it is final
    (``Komplett``), other Referats reuse the same values, and most
    president's remarks from 2025-2026 have none. It is used (prefixed with
    the Referat id) only for elements without a clock time. Collisions within
    a meeting get a ``-2``, ``-3`` … suffix.
    """
    el_id = speech_el.get("Id")
    if referat_id and clock:
        key = f"{referat_id}-{clock}"
    elif referat_id and el_id:
        key = f"{referat_id}-{el_id}"
    else:
        key = el_id or fallback
    base, n = key, 1
    while key in used:
        n += 1
        key = f"{base}-{n}"
    used.add(key)
    return key


def _build_speech(speech_el: ET._Element,
                  speaker: dict,
                  sentences: list[dict],
                  agenda: dict,
                  date_start: datetime.datetime | None,
                  parliament_period: int,
                  session_number: int,
                  text_id: str,
                  referat_url: str) -> dict:
    speech_type = speech_el.tag
    context = speaker.get("context") or "main-speaker"
    # A reply is its own clip with the replier on camera, so the replier
    # stays ``main-speaker``; ``isReply`` marks the record as a reply.
    is_reply = speech_type == "Replikk"
    person = _person(speaker, context)

    agenda_item: dict[str, Any] = {
        "officialTitle": agenda["officialTitle"],
        "title": agenda["title"],
        "type": agenda["type"],
    }
    if agenda.get("id"):
        agenda_item["id"] = agenda["id"]
    if agenda.get("nativeType"):
        agenda_item["nativeType"] = agenda["nativeType"]

    record: dict[str, Any] = {
        "parliament": PARLIAMENT_CODE,
        "electoralPeriod": {"number": parliament_period},
        "session": {"number": session_number},
        "agendaItem": agenda_item,
        "speechIndex": 0,
        "originID": text_id,
        "isReply": is_reply,
        "people": [person],
        "textContents": [{
            "type": "proceedings",
            "language": LANGUAGE_CODE,
            "originTextID": text_id,
            "sourceURI": referat_url,
            "creator": SPEECH_CREATOR,
            "license": SPEECH_LICENSE,
            "textBody": [{
                "type": "speech",
                "speaker": speaker["label"],
                "speakerstatus": context,
                "sentences": sentences,
            }],
        }],
        "debug": {
            "sakNummer": agenda.get("nr"),
            "speechType": speech_type,
            "clockTime": speaker.get("time"),
        },
    }
    if date_start is not None:
        record["dateStart"] = date_start.isoformat(timespec="seconds")
    return record


def _fold_block(speaker: dict, sentences: list[dict]) -> dict:
    """``textBody`` block for an element without a clock time."""
    context = speaker.get("context")
    status = context if context in CHAIR_CONTEXTS else "speaker"
    return {"type": "speech", "speaker": speaker["label"],
            "speakerstatus": status, "sentences": sentences}


def _fold_into(record: dict, speaker: dict, block: dict, *, prepend: bool = False) -> None:
    pid = speaker.get("person_id")
    if pid and block["speakerstatus"] == "speaker" and \
            record["people"][0].get("originPersonID") == pid:
        block = {**block, "speakerstatus": record["people"][0]["context"]}
    body = record["textContents"][0]["textBody"]
    if prepend:
        body.insert(0, block)
    else:
        body.append(block)
    debug = record.setdefault("debug", {})
    debug["foldedElements"] = debug.get("foldedElements", 0) + 1
    # A member speaking inside someone else's clip is listed as ``speaker``.
    if pid and block["speakerstatus"] == "speaker" and not any(
            p.get("originPersonID") == pid for p in record["people"]):
        record["people"].append(_person(speaker, "speaker"))


def agenda_containers(root: ET._Element) -> list[tuple[ET._Element, dict]]:
    """``(container, agenda_info)`` for every speech-bearing section, in
    document order: ``Startseksjon`` then each child of ``Saker``."""
    out: list[tuple[ET._Element, dict]] = []
    start = root.find(".//Mote/Startseksjon")
    if start is not None:
        out.append((start, PROCEDURAL_AGENDA))
    saker = root.find(".//Mote/Hovedseksjon/Saker")
    voting = 0
    if saker is not None:
        for child in saker:
            if not isinstance(child.tag, str):
                continue  # comments / processing instructions
            if child.tag in VOTING_TAGS:
                voting += 1
            out.append((child, agenda_info(child, voting)))
    return out


def parse_referat(root: ET._Element, *, nlp, meeting_date: datetime.date,
                  period: int, session_number: int, referat_id: str,
                  referat_url: str) -> list[dict]:
    """Walk a Referat tree into Stage-2 speech records (``speechIndex`` 1…n)."""
    records: list[dict] = []
    leading: list[tuple[dict, dict]] = []   # untimed blocks before any record
    untimed_records: list[dict] = []        # only used if nothing is timed
    used_ids: set[str] = set()
    clock = _Clock(meeting_date)
    visited = 0

    for container, agenda in agenda_containers(root):
        for el in container.iter(*SPEECH_TAGS):
            visited += 1
            speaker = _speaker_for(el)
            sentences = _sentence_split(nlp, element_paragraphs(el))
            if not sentences:
                continue
            if speaker.get("time"):
                start = clock.at(speaker["time"])
                text_id = _text_id(el, referat_id, speaker["time"],
                                   f"{session_number}-{visited}", used_ids)
                record = _build_speech(el, speaker, sentences, agenda, start,
                                       period, session_number, text_id, referat_url)
                if not records:
                    for spk, block in reversed(leading):
                        _fold_into(record, spk, block, prepend=True)
                records.append(record)
                continue
            block = _fold_block(speaker, sentences)
            if records:
                _fold_into(records[-1], speaker, block)
            else:
                leading.append((speaker, block))
                text_id = _text_id(el, referat_id, None,
                                   f"{session_number}-{visited}", used_ids)
                untimed_records.append(_build_speech(
                    el, speaker, sentences, agenda, None, period,
                    session_number, text_id, referat_url))

    if not records:
        # No clock time anywhere: keep the elements as records without
        # ``dateStart`` (the merger marks them media-missing) rather than
        # losing the text.
        records = untimed_records

    total = sum(1 for _ in root.iter(*SPEECH_TAGS))
    if visited != total:
        logger.warning(f"Referat {referat_id}: walked {visited} of {total} speech elements; "
                       "a container outside Startseksjon/Saker was skipped")
    for i, s in enumerate(records, start=1):
        s["speechIndex"] = i
        # Document order in the Referat (the merger re-sorts by clock time).
        s["debug"]["proceedingIndex"] = i
    return records


def parse_proceedings_for_meeting(config: Config, period: int, moteid: int,
                                  *, spacy_model: str | None = None,
                                  nlp=None) -> dict:
    """Parse the cached ``original/proceedings/{moteid}.xml`` into Stage-2 shape."""
    xml_path = config.dir("proceedings") / f"{moteid}.xml"
    if not xml_path.exists():
        raise FileNotFoundError(f"No proceedings XML for {moteid}: {xml_path}")

    if nlp is None:
        if spacy_model is None:
            from optv.parliaments import get_locale
            spacy_model = get_locale("NO")["spacy_model"]
        nlp = _load_nlp(spacy_model)

    root = ET.parse(str(xml_path)).getroot()
    xml_moteid = _moteid_from_root(root)
    if xml_moteid and xml_moteid != moteid:
        logger.warning(f"XML moteID={xml_moteid} ≠ caller-supplied moteid={moteid}; "
                       "using caller-supplied")

    # Look up the meeting metadata.
    sesjonider = period_to_sesjonider(period)
    meeting: dict | None = None
    sesjonid_for_meeting: str | None = None
    for sesjonid in sesjonider:
        path = config.dir("meetings") / f"{sesjonid}.json"
        if not path.exists():
            continue
        doc = json.loads(path.read_text())
        for m in doc.get("moter_liste") or []:
            if m.get("id") == moteid:
                meeting = m
                sesjonid_for_meeting = sesjonid
                break
        if meeting:
            break
    if not meeting:
        raise ValueError(f"No meeting overview entry for moteid={moteid} in period {period}")

    meeting_date, _ = _meeting_date_iso(meeting)
    # session.number must be unique per meeting: it keys the per-speech audio
    # cache filename (period+session.number+speechIndex in align.cachedfile /
    # align_prep). mote_rekkefolge is always 1 across the whole session-year
    # (and dagsorden_nummer is not unique either), so all meetings collided on
    # the same clip names. moteid is the only reliable unique key (also used by
    # originID and the meta.session string).
    session_number = moteid
    referat_id = meeting.get("referat_id") or ""
    sesjon_path = sesjonid_for_meeting or ""
    referat_url = REFERAT_URL_TEMPLATE.format(sesjon_path=sesjon_path,
                                              referat_id=referat_id) if referat_id else ""

    speeches = parse_referat(root, nlp=nlp, meeting_date=meeting_date,
                             period=period, session_number=session_number,
                             referat_id=referat_id, referat_url=referat_url)

    starts = [s["dateStart"] for s in speeches if s.get("dateStart")]
    midnight = datetime.datetime.combine(meeting_date, datetime.time(0), tzinfo=TZ_OSLO)
    return {
        "meta": {
            "session": f"{period}_{moteid}",
            "schemaVersion": "1.0",
            "dateStart": min(starts) if starts else midnight.isoformat(),
            "dateEnd": max(starts) if starts else midnight.isoformat(),
            "processing": {
                "parse_proceedings": datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": speeches,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, required=True)
    parser.add_argument("--meid", type=int, required=True)
    parser.add_argument("--spacy-model", default=None)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    doc = parse_proceedings_for_meeting(config, args.period, args.meid,
                                        spacy_model=args.spacy_model)
    session = f"{args.period}_{args.meid}"
    out = config.file(session, "proceedings", create=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    logger.info(f"Wrote {out} ({len(doc['data'])} speeches)")


if __name__ == "__main__":
    main()
