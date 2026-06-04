#! /usr/bin/env python3

# Parse Landtag Rheinland-Pfalz ePP "Basisdokument" XML proceedings into
# the intermediate per-session JSON shape consumed by the merger.
#
# Schema (Dataport's ePlenarprotokoll):
#   <Plenarprotokoll wahlperiode sitzung>
#     <Sitzungstermin tagImMonat monat jahr beginn schluss/>
#     <Regierungsbank>     <-- cabinet roster (we ignore for speech extraction)
#     <Vorsitzende>        <-- presiding members roster
#     <Abwesenheit>        <-- absences
#     <Beratung>+          <-- one per agenda block
#       <Beratungsteil>+
#         <Redner name partei fraktion amt titel/>
#         <Moderation><Moderationsteil><TOP thema/></Moderationsteil>
#           <Moderationsteil>...<Rede>...</Rede></Moderationsteil>
#         </Moderation>
#         OR
#         <Rede>...</Rede>     (when the speaker speaks directly, not via chair)
#
# A speech (<Rede>) is always preceded — in document order, within the
# enclosing <Beratungsteil> — by the <Redner> describing the speaker.
# Agenda items are <TOP thema="..."/> announced inside <Moderationsteil>
# preceding the speeches they introduce.

from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
import json
from lxml import etree
from pathlib import Path
import re
from spacy.lang.de import German
import sys

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))               # for .common
    sys.path.insert(0, str(module_dir.parents[3]))           # for optv.shared.*
    __package__ = module_dir.name

from .common import STATUS_TRANSLATION, fix_faction, fix_fullname
from optv.shared.agenda_types import classify_de_rp
from optv.parliaments import get_rights as _get_rights
from optv.parliaments import get_language as _get_language

PROCEEDINGS_LICENSE = _get_rights("DE-RP", stream="proceedings")["license"]
PROCEEDINGS_LANGUAGE = _get_language("DE-RP")


def _build_agenda_item(title: str | None) -> dict:
    title = title or ""
    native_type, core_type = classify_de_rp(title)
    item = {"officialTitle": title, "title": title, "type": core_type}
    if native_type:
        item["nativeType"] = native_type
    return item

MONTHS_DE = {
    "Januar": 1, "Februar": 2, "März": 3, "April": 4, "Mai": 5, "Juni": 6,
    "Juli": 7, "August": 8, "September": 9, "Oktober": 10, "November": 11, "Dezember": 12,
}

# Module-level spaCy German for sentence splitting (rule-based, fast).
nlp = German()
nlp.add_pipe("sentencizer")


def split_sentences(paragraph: str) -> list:
    return [{"text": str(s).strip()} for s in nlp(paragraph).sents if str(s).strip()]


def parse_session_time(t: str) -> str:
    """Parse 'H.MM' or 'HH.MM' (Dataport convention) into 'HH:MM:00'."""
    if not t:
        return ""
    m = re.match(r"^\s*(\d{1,2})[\.:](\d{2})", t)
    if not m:
        logger.warning(f"Unparseable session time: {t!r}")
        return ""
    return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}:00"


def parse_session_date(termin) -> str:
    """Build ISO date from <Sitzungstermin>'s tagImMonat/monat/jahr."""
    if termin is None:
        return ""
    day = termin.attrib.get("tagImMonat", "")
    month_de = termin.attrib.get("monat", "")
    year = termin.attrib.get("jahr", "")
    if not (day and month_de and year):
        return ""
    month = MONTHS_DE.get(month_de.strip())
    if not month:
        logger.warning(f"Unknown month: {month_de!r}")
        return ""
    return f"{int(year):04d}-{month:02d}-{int(day):02d}"


def speaker_context(amt: str, is_main: bool) -> str:
    """Translate <Redner amt="..."> into a Stage 2 speaker context."""
    if amt:
        mapped = STATUS_TRANSLATION.get(amt.strip())
        if mapped:
            return mapped
    return "main-speaker" if is_main else "speaker"


def redner_to_person(redner, context: str) -> dict:
    """Build a Stage 2 people[] entry from a <Redner.../> element.

    Faction is emitted as a raw string; the NEL stage upgrades it to a
    {label, wid, wtype} dict.
    """
    name = (redner.attrib.get("name") or "").strip()
    titel = (redner.attrib.get("titel") or "").strip()
    # The label uses the bare name (no academic title) so it matches OPAL's
    # rendering, which never carries the "Dr.", "Prof. Dr.", etc. prefix.
    # The title is preserved on the `role` field if needed downstream.
    label = name
    fullname = fix_fullname(name)
    parts = fullname.split() if fullname else []
    firstname = parts[0] if parts else ""
    lastname = " ".join(parts[1:]) if len(parts) > 1 else ""
    faction = redner.attrib.get("fraktion") or redner.attrib.get("partei") or ""
    role = (redner.attrib.get("amtKurzform") or redner.attrib.get("amt") or "").strip()

    person = {
        "type": "memberOfParliament",
        "label": label,
        "firstname": firstname,
        "lastname": lastname,
        "context": context,
    }
    if faction:
        person["faction"] = fix_faction(faction)
    if role:
        person["role"] = role
    return person


def collect_inhalt_text(rede) -> tuple[list, list]:
    """Return ([(text, page)], [comment_text]) for one <Rede>."""
    speeches: list[tuple[str, str]] = []
    comments: list[str] = []
    for inh in rede.iter("Inhalt"):
        typ = (inh.attrib.get("typ") or "").strip()
        text = "".join(inh.itertext()).strip()
        if not text:
            continue
        page = inh.attrib.get("seiteNr") or ""
        if typ == "Zwischenruf":
            comments.append(text)
        else:
            speeches.append((text, page))
    return speeches, comments


def find_beratungsteil(node):
    """Walk ancestors until we find a <Beratungsteil>; None if not present."""
    n = node.getparent()
    while n is not None:
        if n.tag == "Beratungsteil":
            return n
        n = n.getparent()
    return None


def speakers_for_rede(rede) -> list:
    """Return the list of <Redner> elements that introduce this <Rede>.

    Convention: speakers are immediate <Redner> children of the enclosing
    <Beratungsteil>, listed in document order. Multiple Redner can appear
    when several speakers share a Beratungsteil (e.g. Kurzintervention).
    """
    bt = find_beratungsteil(rede)
    if bt is None:
        return []
    return [r for r in bt if r.tag == "Redner"]


def build_speech(rede, agenda_title: str, speech_id: str) -> dict | None:
    redners = speakers_for_rede(rede)
    if not redners:
        return None

    people = [
        redner_to_person(r, speaker_context(r.attrib.get("amt", ""), idx == 0))
        for idx, r in enumerate(redners)
    ]
    # Sort so main-speaker is first (merger relies on this invariant).
    people.sort(key=lambda p: 0 if p["context"] == "main-speaker" else 1)

    speech_paragraphs, comments = collect_inhalt_text(rede)
    if not speech_paragraphs and not comments:
        return None

    main_speaker = people[0]["label"]
    main_status = people[0]["context"]
    text_body: list = []
    if speech_paragraphs:
        full_text = "\n".join(t for t, _ in speech_paragraphs)
        text_body.append({
            "speech_id": speech_id,
            "type": "speech",
            "speaker": main_speaker,
            "speakerstatus": main_status,
            "text": full_text,
            "sentences": split_sentences(full_text),
        })
    for comment in comments:
        text_body.append({
            "speech_id": speech_id,
            "type": "comment",
            "speaker": None,
            "speakerstatus": None,
            "text": comment,
            "sentences": [{"text": comment}],
        })

    pages = [p for _, p in speech_paragraphs if p]
    page_range = ""
    if pages:
        page_range = pages[0] if pages[0] == pages[-1] else f"{pages[0]}-{pages[-1]}"

    return {
        "people": people,
        "text_body": text_body,
        "agenda_title": agenda_title,
        "page_range": page_range,
        "main_speaker": main_speaker,
    }


def parse_proceedings_xml(path: Path, source_uri: str = None) -> dict:
    """Parse one ePP XML into a Stage 2 intermediate JSON document."""
    tree = etree.parse(str(path))
    root = tree.getroot()

    if root.tag != "Plenarprotokoll":
        raise ValueError(
            f"{path}: expected root <Plenarprotokoll>, got <{root.tag}>"
        )

    period = root.attrib.get("wahlperiode") or ""
    session = root.attrib.get("sitzung") or ""
    if not period or not session:
        raise ValueError(f"{path}: missing wahlperiode/sitzung on <Plenarprotokoll>")

    session_id = f"{period}{str(session).zfill(3)}"

    termin = root.find("Sitzungstermin")
    date = parse_session_date(termin)
    timeStart = parse_session_time(termin.attrib.get("beginn", "")) if termin is not None else ""
    timeEnd = parse_session_time(termin.attrib.get("schluss", "")) if termin is not None else ""

    dateStart = f"{date}T{timeStart}" if date and timeStart else ""
    dateEnd = f"{date}T{timeEnd}" if date and timeEnd else ""
    if dateStart and dateEnd and timeEnd < timeStart:
        logger.warning(
            f"{path}: session ends before it starts ({timeStart} → {timeEnd}); "
            f"keeping naive timestamps."
        )

    if source_uri is None:
        source_uri = (
            f"https://dokumente.landtag.rlp.de/landtag/plenarprotokolle/"
            f"{session}-P-{period}.pdf"
        )

    speeches: list[dict] = []
    speech_index = 1
    current_agenda = ""

    # Document-order walk over speech-bearing tree (skip headers).
    for el in root.iter():
        if el.tag == "TOP":
            thema = (el.attrib.get("thema") or "").replace("\n", " ").strip()
            if thema:
                current_agenda = thema
            continue
        if el.tag != "Rede":
            continue

        # Skip <Rede> elements that are inside skipped sections (Regierungsbank,
        # Vorsitzende, Abwesenheit etc.) - those don't have a Beratungsteil ancestor.
        if find_beratungsteil(el) is None:
            continue

        speech_id = f"{session_id}-{speech_index:03d}"
        record = build_speech(el, current_agenda, speech_id)
        if record is None:
            continue

        speech = {
            "parliament": "DE-RP",
            "electoralPeriod": {"number": int(period)},
            "session": {
                "number": int(session),
                "dateStart": dateStart,
                "dateEnd": dateEnd,
            },
            "speechIndex": speech_index,
            "originID": speech_id,
            "originTextID": speech_id,
            "agendaItem": _build_agenda_item(record["agenda_title"]),
            "people": record["people"],
            "textContents": [
                {
                    "type": "proceedings",
                    "sourceURI": source_uri,
                    "creator": "Landtag Rheinland-Pfalz",
                    "license": PROCEEDINGS_LICENSE,
                    "language": PROCEEDINGS_LANGUAGE,
                    "originTextID": speech_id,
                    "textBody": record["text_body"],
                }
            ],
            "documents": [],
            "debug": {
                "proceedings-source": "ePP",
                "page-range": record["page_range"],
                "main-speaker-label": record["main_speaker"],
            },
        }
        speeches.append(speech)
        speech_index += 1

    return {
        "meta": {
            "session": session_id,
            "processing": {
                "parse_proceedings": datetime.now().isoformat("T", "seconds"),
            },
            "dateStart": dateStart,
            "dateEnd": dateEnd,
        },
        "data": speeches,
    }


def parse_proceedings_directory(directory: Path, args=None) -> None:
    """Update parsed JSON for every <session>-proceedings.xml in the directory."""
    directory = Path(directory)
    for source in sorted(directory.glob("*-proceedings.xml")):
        output = source.with_suffix(".json")
        if output.exists() and output.stat().st_mtime >= source.stat().st_mtime:
            continue
        logger.info(f"Parsing {source.name}")
        data = parse_proceedings_xml(source)
        with open(output, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse ePP XML proceedings.")
    parser.add_argument("source", type=str, help="Source XML file or directory.")
    parser.add_argument("--output", type=str, default="-",
                        help="Output JSON file or '-' for stdout (single-file mode only).")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )

    src = Path(args.source)
    if src.is_dir():
        parse_proceedings_directory(src, args)
    else:
        data = parse_proceedings_xml(src)
        if args.output == "-":
            json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
        else:
            with open(args.output, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
