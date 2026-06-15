#! /usr/bin/env python3
"""Parse a Storting publikasjon XML (Referat) into Stage-2-shaped JSON.

The Referat groups speech-bearing elements under
``Forhandlinger / Mote / Hovedseksjon / Saker / Sak[saksKartNr=N]``
(or ``Forhandlinger / Mote / Startseksjon / Motestart`` for procedural
intros that precede the formal agenda). The recognised speech elements are:

  - ``Hovedinnlegg`` — main speech (always carries ``Navn personID=…``)
  - ``Replikk``      — reply (always carries ``Navn personID=…``)
  - ``Presinnlegg``  — president's interjection (Navn has no personID)

Every speech-bearing element wraps its content in one or more ``<A>``
paragraphs. The opening paragraph contains the ``<Navn>`` element whose
text is e.g. ``"Erlend Wiborg (FrP) [10:00:07]:"`` — the ``[HH:MM:SS]``
bracket carries the clock-time anchor used downstream to compute the
video offset against the Qbrick ``custom.TC_in``.

Output: ``cache/proceedings/{moteid}-proceedings.json`` (via Config helper)
shaped as ``{"meta": {...}, "data": [<speech>, ...]}``. Per-speech entries
follow the OPTV Stage 2 shape but **omit ``media`` and final ``dateStart``**
— those are filled in by the merger from the Qbrick-derived part metadata.
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

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.NO.parsers"

import lxml.etree as ET

from optv.parliaments.NO.common import Config, period_to_sesjonider
from optv.shared.agenda_types import classify_no
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

# `<Navn>` text shape: "First Last (Party) [HH:MM:SS]:" or
#                     "Statsminister First Last [HH:MM:SS]:" (no party for
#                     ministers — the role title takes the slot the party
#                     would otherwise occupy) or "Presidenten [HH:MM:SS]:"
_NAVN_TIME_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\]")
_NAVN_PARTY_RE = re.compile(r"\(([^)]+)\)")
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


def _navn_text(navn: ET._Element) -> str:
    """Plain-text content of a ``<Navn>`` element with whitespace collapsed.

    Note: ``ET.tostring(navn, method='text')`` includes the element's *tail*,
    which for ``<Navn>`` is the rest of the speech body. We use ``itertext()``
    instead — that yields only the descendant text, no tails outside.
    """
    text = "".join(navn.itertext())
    return re.sub(r"\s+", " ", text).strip()


def parse_navn(navn: ET._Element) -> dict:
    """Decode a ``<Navn>`` element into speaker metadata.

    Returns ``{label, person_id, party, role, context, time}`` where ``time``
    is ``HH:MM:SS`` or ``None``. ``person_id`` is the Stortinget ``personID``
    attribute when present.
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


def _strip_navn(el: ET._Element) -> str:
    """Return the speech-bearing element's text content with all <Navn> spans
    removed (so it doesn't repeat the speaker line as speech body)."""
    # Make a working copy so we don't mutate the original tree.
    copy = ET.fromstring(ET.tostring(el))
    for navn in copy.findall(".//Navn"):
        # Append the tail to the parent's text so we don't lose content that
        # follows the </Navn>.
        parent = navn.getparent()
        if parent is None:
            continue
        if navn.tail:
            # Glue tail to the previous sibling's tail or the parent's text.
            prev = navn.getprevious()
            if prev is not None:
                prev.tail = (prev.tail or "") + navn.tail
            else:
                parent.text = (parent.text or "") + navn.tail
        parent.remove(navn)
    text = ET.tostring(copy, method="text", encoding="unicode")
    # Replace soft hyphens & non-breaking spaces, collapse whitespace.
    text = text.replace("­", "").replace(" ", " ")
    return text.strip()


def _element_paragraphs(el: ET._Element) -> list[str]:
    """Return one cleaned plain-text paragraph per ``<A>`` child."""
    paragraphs: list[str] = []
    for a in el.findall("./A"):
        # Strip out the leading <Navn> if this is the first <A>.
        copy = ET.fromstring(ET.tostring(a))
        for navn in copy.findall(".//Navn"):
            if navn.tail:
                prev = navn.getprevious()
                if prev is not None:
                    prev.tail = (prev.tail or "") + navn.tail
                else:
                    copy.text = (copy.text or "") + navn.tail
            copy.remove(navn)
        text = ET.tostring(copy, method="text", encoding="unicode")
        text = text.replace("­", "").replace(" ", " ")
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            paragraphs.append(text)
    return paragraphs


def _sentence_split(nlp, paragraphs: list[str]) -> list[dict]:
    sentences: list[dict] = []
    for para in paragraphs:
        doc = nlp(para)
        for s in doc.sents:
            t = s.text.strip()
            if t:
                sentences.append({"text": t})
    return sentences


def _sak_title(sak: ET._Element) -> tuple[int | None, str, str]:
    """Return ``(saksKartNr, full_title, short_title)`` for a ``<Sak>``."""
    try:
        nr = int(sak.get("saksKartNr") or "0") or None
    except (TypeError, ValueError):
        nr = None
    saktittel_el = sak.find(".//Saktittel")
    if saktittel_el is None:
        return nr, "", ""
    text = ET.tostring(saktittel_el, method="text", encoding="unicode")
    text = re.sub(r"\s+", " ", text).strip()
    # The bracketed "(trontaledebatt)" suffix is the most useful classification
    # signal; keep it in the full title but use the cleaner version for short.
    short = re.sub(r"\s*\([^)]*\)\s*$", "", text)
    return nr, text, short


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
    # The TZ suffix in the source is the Oslo offset, but the ms value is UTC.
    # We want the local *date* (Norway), so adjust by +0200/+0100 to get the
    # session's local calendar date.
    offset = m.group(2) or "+0000"
    sign = 1 if offset[0] == "+" else -1
    minutes = int(offset[1:3]) * 60 + int(offset[3:5])
    local_dt = dt + datetime.timedelta(minutes=sign * minutes)
    return local_dt.date(), local_dt.replace(tzinfo=None).isoformat(timespec="seconds")


def _sesjon_for_meeting(meeting: dict, config: Config) -> str | None:
    """Walk the cached meetings overviews to find which sesjonid contains
    this meeting."""
    target = meeting["id"]
    for path in config.dir("meetings").glob("*.json"):
        doc = json.loads(path.read_text())
        if any(m.get("id") == target for m in doc.get("moter_liste") or []):
            return doc.get("sesjon_id") or path.stem
    return None


def _build_speech(speech_el: ET._Element,
                  speech_index: int,
                  speech_type: str,
                  sak_nr: int | None,
                  sak_title_full: str,
                  sak_title_short: str,
                  agenda_native: str | None,
                  agenda_core: str,
                  meeting_date: datetime.date,
                  meeting: dict,
                  parliament_period: int,
                  session_number: int,
                  referat_id: str,
                  referat_url: str,
                  nlp) -> dict:
    navn_el = speech_el.find(".//Navn")
    speaker = parse_navn(navn_el) if navn_el is not None else {
        "label": "Unknown", "person_id": None, "party": None,
        "role": None, "context": "Unknown", "time": None,
    }
    paragraphs = _element_paragraphs(speech_el)
    sentences = _sentence_split(nlp, paragraphs)

    if speech_type == "Replikk":
        is_reply = True
        # Replikk speakers are members responding to a Hovedinnlegg, normal context.
        if speaker.get("context") == "main-speaker":
            speaker["context"] = "speaker"
    elif speech_type == "Presinnlegg":
        # President is the speaker.
        is_reply = False
        if speaker.get("context") == "main-speaker":
            speaker["context"] = "president"
            speaker["label"] = speaker["label"] or "Presidenten"
    else:
        is_reply = False

    # Build start ISO timestamp from the meeting date + speech time-of-day.
    date_start = None
    if speaker.get("time"):
        date_start = f"{meeting_date.isoformat()}T{speaker['time']}"

    person: dict[str, Any] = {
        "label": speaker["label"],
        "context": speaker.get("context") or "main-speaker",
    }
    if speaker.get("role"):
        person["role"] = speaker["role"]
    if speaker.get("party") and speaker.get("context") not in ("president", "vice-president"):
        person["faction"] = {"label": speaker["party"]}
    if speaker.get("person_id"):
        person["originPersonID"] = speaker["person_id"]

    agenda_item: dict[str, Any] = {
        "officialTitle": (f"sak nr. {sak_nr}" if sak_nr else "Procedural"),
        "title": sak_title_short or sak_title_full or "Procedural",
        "type": agenda_core,
    }
    if sak_nr:
        agenda_item["id"] = f"sak-{sak_nr}"
    if agenda_native:
        agenda_item["nativeType"] = agenda_native

    record: dict[str, Any] = {
        "parliament": PARLIAMENT_CODE,
        "electoralPeriod": {"number": parliament_period},
        "session": {"number": session_number},
        "agendaItem": agenda_item,
        "speechIndex": speech_index,
        "originID": speech_el.get("Id") or f"{meeting['id']}-{speech_index}",
        "isReply": is_reply,
        "people": [person],
        "textContents": [{
            "type": "proceedings",
            "language": LANGUAGE_CODE,
            "originTextID": speech_el.get("Id") or "",
            "sourceURI": referat_url,
            "creator": SPEECH_CREATOR,
            "license": SPEECH_LICENSE,
            "textBody": [{
                "type": "speech" if speech_type != "Presinnlegg" else "comment",
                "speaker": speaker["label"],
                "speakerstatus": speaker.get("role"),
                "sentences": sentences,
            }],
        }],
        "debug": {
            "sakNummer": sak_nr,
            "speechType": speech_type,
            "clockTime": speaker.get("time"),
        },
    }
    if date_start:
        record["dateStart"] = date_start
    return record


def parse_proceedings_for_meeting(config: Config, period: int, moteid: int,
                                  *, spacy_model: str | None = None) -> dict:
    """Parse the cached ``original/proceedings/{moteid}.xml`` into Stage-2 shape."""
    xml_path = config.dir("proceedings") / f"{moteid}.xml"
    if not xml_path.exists():
        raise FileNotFoundError(f"No proceedings XML for {moteid}: {xml_path}")

    if spacy_model is None:
        from optv.parliaments import get_locale
        spacy_model = get_locale("NO")["spacy_model"]
    import spacy
    logger.info(f"Loading spaCy model {spacy_model}")
    nlp = spacy.load(spacy_model)

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

    meeting_date, meeting_date_iso = _meeting_date_iso(meeting)
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

    speeches: list[dict] = []
    speech_index = 0

    # Phase 1: procedural intros under Startseksjon/Motestart (sak_nr = None).
    motestart = root.find(".//Mote/Startseksjon/Motestart")
    if motestart is not None:
        agenda_native_proc, agenda_core_proc = (None, "procedural")
        for child in motestart.iterchildren():
            if child.tag not in ("Hovedinnlegg", "Replikk", "Presinnlegg"):
                continue
            speech_index += 1
            speeches.append(_build_speech(
                child, speech_index, child.tag,
                None, "Procedural intros", "Procedural intros",
                agenda_native_proc, agenda_core_proc,
                meeting_date, meeting, period, session_number,
                referat_id, referat_url, nlp,
            ))

    # Phase 2: Sak-by-Sak walk through Hovedseksjon.
    for sak in root.findall(".//Mote/Hovedseksjon/Saker/Sak"):
        sak_nr, sak_title_full, sak_title_short = _sak_title(sak)
        agenda_native, agenda_core = classify_no(sak_title_full)
        # Walk every speech-bearing descendant in document order.
        for desc in sak.iter():
            if desc is sak:
                continue
            if desc.tag not in ("Hovedinnlegg", "Replikk", "Presinnlegg"):
                continue
            speech_index += 1
            speeches.append(_build_speech(
                desc, speech_index, desc.tag,
                sak_nr, sak_title_full, sak_title_short,
                agenda_native, agenda_core,
                meeting_date, meeting, period, session_number,
                referat_id, referat_url, nlp,
            ))

    # Drop speeches with empty text bodies (some <Presinnlegg> get culled
    # entirely after stripping <Navn>).
    speeches = [s for s in speeches
                if any((tc.get("textBody") or []) and tc["textBody"][0].get("sentences")
                       for tc in s.get("textContents") or [])]
    # Reindex to keep speechIndex contiguous after the drop.
    for i, s in enumerate(speeches, start=1):
        s["speechIndex"] = i

    return {
        "meta": {
            "session": f"{period}_{moteid}",
            "schemaVersion": "1.0",
            "dateStart": f"{meeting_date.isoformat()}T00:00:00",
            "dateEnd": f"{meeting_date.isoformat()}T23:59:59",
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
