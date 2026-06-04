#! /usr/bin/env python3
"""Parse EP Open Data API speech payloads into intermediate per-speech JSON.

Reads ``speeches.jsonld`` + ``meeting.jsonld`` produced by
``scraper/fetch_proceedings.py`` and emits the same intermediate shape the
merger consumes — keyed lookups (timing, person, faction, agenda) come from
the structured API fields; speech text + speaker display name + faction abbr
are pulled from the embedded ``api:xmlFragment`` (English translation).

Activity-type discriminator (``PLENARY_DEBATE_SPEECH``) is used to filter out
chair-changes and votes naturally — no regex needed.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from lxml import etree  # type: ignore

from ..scraper.ep_api import PLENARY_DEBATE_SPEECH, _activity_type, ref_to_id, strip_iri_prefix
from .common import EU_FACTION_LABELS, EU_KNOWN_ROLES, parse_speaker_line

logger = logging.getLogger(__name__)

MEPPHOTO_BASE = "https://www.europarl.europa.eu/mepphoto/"
LANG_PREFERENCE = ("en", "fr", "de", "es", "it")
_ITM_NUMBER_RE = re.compile(r"ITM-0*(\d+)", re.IGNORECASE)
# Agenda labels from the API come prefixed with their item number, e.g.
# "10. Europe's automotive future – ...". Strip it for parity with the
# baseline scraper output (which carried just the title text).
_AGENDA_PREFIX_RE = re.compile(r"^\s*\d+(?:\.\d+)*\.\s*")
_EP_VOD_URL = (
    "https://www.europarl.europa.eu/plenary/en/vod.html"
    "?mode=unit&vodLanguage=EN&playerStartTime={start}&playerEndTime={end}"
)


def _classify_agenda(title: str) -> str:
    """Crude CRE-title → coarse type mapping; the real classifier is
    optv.shared.agenda_types.classify_eu_native (called in the merger)."""
    t = (title or "").lower()
    if "voting time" in t or t.startswith("vote") or t.startswith("explanation"):
        return "voting"
    if "question time" in t or "questions to" in t:
        return "qa"
    if "opening of the sitting" in t or "resumption" in t:
        return "opening"
    if "closure of the sitting" in t:
        return "closing"
    if "formal sitting" in t or "address by" in t:
        return "government_declaration"
    if "debate" in t:
        return "regular"
    return "other"


def _pick_lang(label_or_fragment: dict | None) -> tuple[str | None, str | None]:
    """Return (lang_code, value) from a dict keyed by ISO-639-1 language codes.

    Tries English first, then a handful of well-translated fallbacks, then any
    available key. Returns (None, None) if the input is empty/missing.
    """
    if not label_or_fragment:
        return None, None
    for lang in LANG_PREFERENCE:
        v = label_or_fragment.get(lang)
        if v:
            return lang, v
    for lang, v in label_or_fragment.items():
        if v:
            return lang, v
    return None, None


def _parse_xml_fragment(fragment_str: str) -> etree._Element | None:
    """Parse an embedded XML fragment from the API into an lxml element.

    The API wraps the verbatim transcript inside ``<oralStatements>`` /
    ``<writtenStatements>`` etc. Use a tolerant recovering parser since the
    fragments occasionally contain entities that aren't in scope.
    """
    if not fragment_str:
        return None
    try:
        parser = etree.XMLParser(recover=True, resolve_entities=False)
        return etree.fromstring(fragment_str.encode("utf-8"), parser=parser)
    except etree.XMLSyntaxError as e:
        logger.warning("xml fragment parse failed: %s", e)
        return None


def _flatten(el) -> str:
    if el is None:
        return ""
    return re.sub(r"\s+", " ", " ".join(el.itertext())).strip()


def _extract_paragraphs(root: etree._Element) -> list[str]:
    """Pull paragraph text from a <speech>/<oralStatements>/<blockContainer> tree."""
    paragraphs: list[str] = []
    for p in root.iter("p"):
        text = _flatten(p)
        if text:
            paragraphs.append(text)
    return paragraphs


def _extract_from_block(root: etree._Element) -> tuple[str | None, str | None, str | None]:
    """Return (speaker_name, faction_abbr, raw_from_text) from the <from> sub-tree.

    The structured ``<person>`` / ``<organization>`` elements are the
    canonical source; only fall back to free-text parsing when one of them is
    missing.
    """
    speaker_name = None
    faction_abbr = None
    raw = None

    from_el = next(iter(root.iter("from")), None)
    if from_el is None:
        return None, None, None
    raw = _flatten(from_el)

    person_el = next(iter(from_el.iter("person")), None)
    if person_el is not None:
        speaker_name = _flatten(person_el) or None

    org_el = next(iter(from_el.iter("organization")), None)
    if org_el is not None:
        text = _flatten(org_el)
        if text in EU_FACTION_LABELS:
            faction_abbr = text
        elif text:
            # Sometimes the long form appears; reverse-lookup by canonical label.
            for abbr, label in EU_FACTION_LABELS.items():
                if text == label or text == abbr:
                    faction_abbr = abbr
                    break

    return speaker_name, faction_abbr, raw


def _parse_iso_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _to_utc_iso(value: str | None) -> str | None:
    dt = _parse_iso_dt(value)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _to_player_time(value: str | None) -> str | None:
    """Render an ISO datetime in the legacy EP VOD ``YYYYMMDD-HH:MM:SS`` format
    using Europe/Brussels local time (kept for ``debug.cre`` parity)."""
    dt = _parse_iso_dt(value)
    if dt is None:
        return None
    try:
        from zoneinfo import ZoneInfo
        local = dt.astimezone(ZoneInfo("Europe/Brussels"))
    except Exception:  # noqa: BLE001
        local = dt
    return local.strftime("%Y%m%d-%H:%M:%S")


def _vod_url(start_player: str | None, end_player: str | None) -> str | None:
    if not start_player or not end_player:
        return None
    return _EP_VOD_URL.format(start=start_player, end=end_player)


def _agenda_for_speech(speech: dict, agenda_items: dict, fallback_titles: dict) -> dict | None:
    """Resolve a speech's parent agenda item (number + English title)."""
    rec = (speech.get("recorded_in_a_realization_of") or [{}])[0]
    parent_doc = rec.get("is_part_of") or ""
    parent_id = ref_to_id(parent_doc)
    if not parent_id:
        return None

    # Map document id (CRE-10-...-ITM-NNN) → event id (MTG-PL-...-PVCRE-ITM-NNN).
    itm_number = None
    m = _ITM_NUMBER_RE.search(parent_id)
    if m:
        itm_number = int(m.group(1))

    title = None
    # First try direct lookup by full id in agenda_items (event ids).
    event_match = None
    for ev_id, ev in agenda_items.items():
        m2 = _ITM_NUMBER_RE.search(ev_id)
        if m2 and itm_number is not None and int(m2.group(1)) == itm_number:
            event_match = ev
            break
    if event_match is not None:
        _, title = _pick_lang(event_match.get("activity_label"))
    elif itm_number is not None:
        title = fallback_titles.get(itm_number)

    if not title and itm_number is None:
        return None

    title = (title or "").strip()
    title = _AGENDA_PREFIX_RE.sub("", title)
    return {
        "number": itm_number,
        "officialTitle": title or "Untitled agenda item",
        "type": _classify_agenda(title or ""),
    }


def _speaker_record_from_speech(
    speech: dict,
    xml_root: etree._Element | None,
) -> tuple[dict, str]:
    """Build the intermediate-shape ``speaker`` block + the original speaker text."""
    # epId comes from the structured had_participation; xml_fragment is a fallback.
    ep_id = None
    participation = speech.get("had_participation") or {}
    persons = participation.get("had_participant_person") or []
    if persons:
        ep_id = strip_iri_prefix(persons[0], "person/")

    speaker_name = None
    faction_abbr = None
    raw_from = ""
    if xml_root is not None:
        speaker_name, faction_abbr, raw_from = _extract_from_block(xml_root)
        if ep_id is None and xml_root is not None:
            person_el = next(iter(xml_root.iter("person")), None)
            if person_el is not None:
                ep_id = strip_iri_prefix(person_el.get("refersTo"), "person/")

    # Role parsing: if no faction, try the rest of the <from> text after the name.
    role = None
    annotation = None
    if not faction_abbr and raw_from and speaker_name:
        remainder = raw_from
        if remainder.startswith(speaker_name):
            remainder = remainder[len(speaker_name):]
        # Use the existing parse_speaker_line helper for role/annotation parsing
        parsed = parse_speaker_line((speaker_name + remainder).rstrip(" .–—-:"))
        role = parsed.get("role")
        annotation = parsed.get("annotation")
        if not role and speaker_name in EU_KNOWN_ROLES:
            role = speaker_name

    speaker = {
        "name": speaker_name or "",
        "role": role,
        "factionAbbr": faction_abbr,
        "factionLabel": EU_FACTION_LABELS.get(faction_abbr) if faction_abbr else None,
        "epId": ep_id,
        "photoURL": MEPPHOTO_BASE + f"{ep_id}.jpg" if ep_id else None,
        "annotation": annotation,
    }
    return speaker, raw_from or speaker_name or ""


def parse_speeches_payload(
    speeches_payload: dict,
    meeting_payload: dict,
    session: str,
) -> dict:
    """Build the intermediate proceedings doc from API payloads."""
    speeches_in = speeches_payload.get("data") or []
    agenda_items = meeting_payload.get("agenda_items") or {}

    # Optional supplemental mapping: ITM number → English title from
    # meeting.consists_of (some legacy meetings don't return /events/ for each ITM).
    fallback_titles: dict[int, str] = {}

    speeches_out: list[dict] = []
    skipped_other_type = 0
    skipped_no_text = 0
    fallback_lang_count = 0

    speech_index = 0
    for sp in speeches_in:
        if _activity_type(sp) != PLENARY_DEBATE_SPEECH:
            skipped_other_type += 1
            continue
        rec = (sp.get("recorded_in_a_realization_of") or [{}])[0]
        fragments = rec.get("api:xmlFragment") or {}
        lang, fragment_str = _pick_lang(fragments)
        if lang and lang != "en":
            fallback_lang_count += 1
            logger.info("speech %s missing EN — falling back to %s", rec.get("identifier"), lang)
        xml_root = _parse_xml_fragment(fragment_str) if fragment_str else None
        paragraphs = _extract_paragraphs(xml_root) if xml_root is not None else []
        if not paragraphs:
            skipped_no_text += 1
            continue
        speaker, raw_from = _speaker_record_from_speech(sp, xml_root)

        # Strip the speaker line prefix from the first paragraph if it leaked
        # in (some fragments inline <p> right after <from> without a separator).
        if raw_from and paragraphs and paragraphs[0].startswith(raw_from):
            paragraphs[0] = paragraphs[0][len(raw_from):].lstrip(" .–—-:").strip()
            if not paragraphs[0]:
                paragraphs = paragraphs[1:]

        speech_index += 1

        speech_id = (
            rec.get("notation_speechId")
            or ref_to_id(rec.get("identifier"))
            or sp.get("activity_id")
            or ""
        )
        start_iso = _to_utc_iso(sp.get("activity_start_date"))
        end_iso = _to_utc_iso(sp.get("activity_end_date"))
        player_start = _to_player_time(sp.get("activity_start_date"))
        player_end = _to_player_time(sp.get("activity_end_date"))

        agenda = _agenda_for_speech(sp, agenda_items, fallback_titles)

        original_languages = rec.get("originalLanguage") or []
        original_lang_iso = None
        if isinstance(original_languages, list) and original_languages:
            iri = original_languages[0]
            tag = iri.rsplit("/", 1)[-1].lower() if isinstance(iri, str) else ""
            if tag:
                # Tag is ISO 639-3 (e.g. "eng"); the canonical 639-1 lookup is
                # one-to-one for the EU's 24 official languages.
                original_lang_iso = _ISO3_TO_ISO1.get(tag)

        debug = {
            "vodURL": _vod_url(player_start, player_end),
            "originalSpeakerLine": raw_from or speaker.get("name") or "",
            "apiSpeechId": rec.get("identifier"),
            "originalLanguage": original_lang_iso or "",
        }
        if lang and lang != "en":
            debug["fallbackLang"] = lang

        speeches_out.append({
            "speechIndex": speech_index,
            "speechId": str(speech_id),
            "speaker": speaker,
            "playerStartTime": player_start,
            "playerEndTime": player_end,
            "dateStart": start_iso,
            "dateEnd": end_iso,
            "textParagraphs": paragraphs,
            "agendaItem": agenda,
            "debug": debug,
        })

    logger.info(
        "parsed %d speeches (skipped %d non-debate activities, %d empty)%s",
        len(speeches_out), skipped_other_type, skipped_no_text,
        f"; {fallback_lang_count} EN-fallback" if fallback_lang_count else "",
    )

    return {
        "meta": {
            "session": session,
            "date": f"{session[:4]}-{session[4:6]}-{session[6:8]}",
            "parliament": "EU",
            "processing": {
                "parse_proceedings": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            },
        },
        "data": speeches_out,
    }


# ISO 639-3 → 639-1 for the 24 official EU languages (used to normalize the
# API's ``originalLanguage`` URI-tag form to the short code the rest of the
# pipeline expects).
_ISO3_TO_ISO1 = {
    "bul": "bg", "ces": "cs", "dan": "da", "deu": "de", "ell": "el",
    "eng": "en", "spa": "es", "est": "et", "fin": "fi", "fra": "fr",
    "gle": "ga", "hrv": "hr", "hun": "hu", "ita": "it", "lit": "lt",
    "lav": "lv", "mlt": "mt", "nld": "nl", "pol": "pl", "por": "pt",
    "ron": "ro", "slk": "sk", "slv": "sl", "swe": "sv",
}


def parse_proceedings_for_session(config, session: str) -> dict:
    proc_dir = config.dir("proceedings")
    marker_path = proc_dir / f"raw-{session}-cre.json"
    if not marker_path.exists():
        raise FileNotFoundError(f"[{session}] raw marker missing: {marker_path}")
    marker = json.loads(marker_path.read_text())
    speeches_path = (proc_dir / marker["speechesPath"]).resolve()
    meeting_path = (proc_dir / marker["meetingPath"]).resolve()
    if not speeches_path.exists() or not meeting_path.exists():
        raise FileNotFoundError(
            f"[{session}] API payload(s) missing: {speeches_path} / {meeting_path}"
        )
    speeches_payload = json.loads(speeches_path.read_text(encoding="utf-8"))
    meeting_payload = json.loads(meeting_path.read_text(encoding="utf-8"))
    return parse_speeches_payload(speeches_payload, meeting_payload, session)


def parse_proceedings_directory(config, args) -> None:
    proc_dir = config.dir("proceedings")
    markers = sorted(proc_dir.glob("raw-*-cre.json"))
    if not markers:
        logger.warning(f"no raw-*-cre.json under {proc_dir}")
        return
    for marker_path in markers:
        m = re.match(r"raw-(\d{8})-cre\.json$", marker_path.name)
        if not m:
            continue
        session = m.group(1)
        if getattr(args, "limit_session", None):
            try:
                if not re.match(args.limit_session, session):
                    continue
            except re.error:
                if args.limit_session != session:
                    continue
        out_path = config.file(session, "proceedings")
        if (out_path.exists() and not args.force
                and out_path.stat().st_mtime > marker_path.stat().st_mtime):
            logger.debug(f"[{session}] proceedings intermediate cached")
            continue
        logger.info(f"[{session}] parsing API proceedings")
        doc = parse_proceedings_for_session(config, session)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info(f"[{session}] wrote {out_path.name} ({len(doc['data'])} speeches)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("speeches_jsonld", type=Path, help="Path to API speeches.jsonld")
    parser.add_argument("meeting_jsonld", type=Path, help="Path to API meeting.jsonld")
    parser.add_argument("--session", type=str, required=True, help="Session key (YYYYMMDD)")
    parser.add_argument("--output", type=Path, default=None, help="Output path (default: stdout)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    speeches_payload = json.loads(args.speeches_jsonld.read_text(encoding="utf-8"))
    meeting_payload = json.loads(args.meeting_jsonld.read_text(encoding="utf-8"))
    doc = parse_speeches_payload(speeches_payload, meeting_payload, args.session)
    out_text = json.dumps(doc, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(out_text)
        logger.info(f"wrote {args.output} ({len(doc['data'])} speeches)")
    else:
        print(out_text)


if __name__ == "__main__":
    main()
