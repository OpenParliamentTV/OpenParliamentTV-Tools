#! /usr/bin/env python3
"""Parse the downloaded DE-SN raw manifests into per-Sitzung intermediate JSON.

Input per session (written by the scraper):

- ``original/media/{session_id}-raw.json`` — the per-speech records scraped from
  the mediathek list: speaker (natural ``Firstname Lastname`` order), faction
  badge, speech-time category, TOP number + short ``thema`` text, Sitzungsnummer,
  date + wall-clock time, the daily HLS ``smil`` URL and per-speech
  ``start_offset``/``end_offset`` (seconds into that daily stream).

This pass: splits the speaker into first/last; routes the faction badge to a
party (or to a government role for "Staatsregierung"); derives the speaker
``context`` from the speech-time category; computes a **real wall-clock**
``start_datetime`` from the item's date + time and an ``end_datetime`` from the
offset span; and assigns a deterministic ``speech_index`` ordered by
(date, start_offset). Unlike DE-BW (video-relative), the source carries a real
per-speech wall-clock, so ``timesAreVideoRelative`` is False downstream. One
record per speech is emitted to ``original/media/{session_id}-media.json``.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from optv.parliaments import get_rights as _get_rights

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-SN.parsers"

logger = logging.getLogger(__name__)

MEDIA_LICENSE = _get_rights("DE-SN", stream="media")["license"]
MEDIA_CREATOR = _get_rights("DE-SN", stream="media")["creator"]

# Faction badges that are NOT a party group: the government bench and
# unattached members. These route to a role rather than a faction.
_NON_FACTION = {"staatsregierung"}
_FRAKTIONSLOS = {"fraktionslos", "fraktionslose", "parteilos"}

_HONORIFICS = ("Dr. ", "Prof. ", "Prof. Dr. ", "Dr. Dr. ", "Dr. h. c. ")

# Structural placeholder rows the mediathek lists alongside real Redebeiträge:
# session-boundary / break markers carried in the speaker field (e.g.
# "ENDE der Sitzung - Fragestunde", "Beginn der Sitzung"). Not speeches.
_MARKER_RE = re.compile(
    r'\b(ENDE|Beginn|Anfang|Fortsetzung|Unterbrechung)\s+der\s+Sitzung\b|'
    r'\bSitzungspause\b|\bMittagspause\b|\bUnterbrechung\b', re.I)
# A speaker field that tacks the interjection type onto the name, e.g.
# "Unbekannter Redner - Zwischenfrage" → ("Unbekannter Redner", "Zwischenfrage").
_SUFFIX_TYPE_RE = re.compile(
    r'^(?P<name>.*?)\s+-\s+(?P<type>Zwischenfrage|Kurzintervention|Nachfrage)\s*$', re.I)


def _is_marker(speaker_raw: str) -> bool:
    return bool(_MARKER_RE.search(speaker_raw or ""))


def _strip_honorifics(name: str) -> str:
    s = name.strip()
    changed = True
    while changed:
        changed = False
        for h in _HONORIFICS:
            if s.startswith(h):
                s = s[len(h):]
                changed = True
                break
    return s


def _split_name(speaker_raw: str) -> tuple[str, str, str]:
    """``"Nam Duy Nguyen"`` → ``("Nam Duy Nguyen", "Nam Duy", "Nguyen")``.

    The source lists speakers in natural ``Firstname Lastname`` order, so the
    label is taken as-is (minus honorifics) and the surname is the last token.
    """
    name = _strip_honorifics(speaker_raw)
    parts = name.split()
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        return parts[0], "", parts[0]
    lastname = parts[-1]
    firstname = " ".join(parts[:-1])
    return name, firstname, lastname


def _speaker_context(speech_type: str, speaker_raw: str = "") -> str:
    """Map the speech-time category to a Stage 2 ``people[].context``.

    The mediathek archives substantive speaking turns ("Debatte", "Aktuelle
    Debatte", "Sonderredezeit") as main speakers and brief follow-ups
    ("Kurzintervention", "Zwischenfrage") as secondary speakers (the type is
    sometimes carried in the speaker field rather than the category). Chair
    turns are not archived as Einzelbeiträge, so there is no president context.
    """
    t = f"{speech_type or ''} {speaker_raw or ''}".lower()
    if "kurzintervention" in t or "zwischenfrage" in t or "nachfrage" in t:
        return "speaker"
    return "main-speaker"


def parse_session(raw_path: Path) -> dict | None:
    with raw_path.open() as f:
        manifest = json.load(f)

    session_id = manifest["session_id"]
    wp = int(manifest["wp"])
    sitzungsnr = int(manifest["sitzung"])

    speeches: list[dict] = []
    skipped = 0
    for rec in manifest.get("speeches", []):
        speaker_raw = rec.get("speaker_raw", "")
        # Drop session-boundary / break placeholder rows (not Redebeiträge).
        if _is_marker(speaker_raw):
            skipped += 1
            continue
        # "Unbekannter Redner - Zwischenfrage" → name "Unbekannter Redner",
        # type signal "Zwischenfrage" (folded into the context derivation).
        m = _SUFFIX_TYPE_RE.match(speaker_raw)
        name_for_split = m.group("name").strip() if m else speaker_raw
        label, firstname, lastname = _split_name(name_for_split)
        faction_raw = (rec.get("faction_raw") or "").strip()
        fl = faction_raw.lower()
        if fl in _NON_FACTION:
            gruppe, role = "", faction_raw          # government bench
        elif fl in _FRAKTIONSLOS:
            gruppe, role = "", faction_raw
        else:
            gruppe, role = faction_raw, ""

        start = int(rec["start_offset"])
        end = rec.get("end_offset")
        end = int(end) if end is not None else None

        # Real wall-clock from the item's date + time; end = start + offset span.
        start_dt = end_dt = None
        if rec.get("date") and rec.get("time"):
            start_dt = datetime.strptime(
                f"{rec['date']}T{rec['time']}", "%Y-%m-%dT%H:%M:%S")
            span = (end - start) if (end is not None and end > start) else 0
            end_dt = start_dt + timedelta(seconds=span)

        speeches.append({
            "date": rec.get("date"),
            "wp": wp,
            "sitzung_no": sitzungsnr,
            "speech_id": f"{session_id}_{rec['id']}",
            "einzelbeitrag_id": rec["id"],
            "top_no": rec.get("top_no"),
            "top_title": rec.get("thema", "").strip(),
            "speech_type": rec.get("speech_type", ""),
            "name_raw": rec.get("speaker_raw", ""),
            "label": label,
            "firstname": firstname,
            "lastname": lastname,
            "role": role,
            "gruppe": gruppe,
            "context": _speaker_context(rec.get("speech_type", ""), speaker_raw),
            "start_offset": start,
            "end_offset": end,
            "start_clock": rec.get("time", ""),
            "start_datetime": start_dt.isoformat("T", "seconds") if start_dt else None,
            "end_datetime": end_dt.isoformat("T", "seconds") if end_dt else None,
            "smil_url": rec.get("smil_url", ""),
            "source_page": rec.get("source_page", ""),
        })

    if skipped:
        logger.info(f"{session_id}: skipped {skipped} structural placeholder row(s)")
    if not speeches:
        logger.info(f"{session_id}: no speeches with video — skipping")
        return None

    speeches.sort(key=lambda s: (s.get("date") or "", s["start_offset"]))
    for idx, s in enumerate(speeches, start=1):
        s["speech_index"] = idx

    dates = sorted({s["date"] for s in speeches if s.get("date")})
    return {
        "meta": {
            "session": session_id,
            "wp": wp,
            "date": dates[0] if dates else manifest.get("date"),
            "dates": dates,
            "sitzung": sitzungsnr,
            "processing": {
                "parse_media": datetime.now().isoformat("T", "seconds"),
            },
        },
        "data": speeches,
    }


def parse_media_directory(media_dir: Path) -> None:
    media_dir = Path(media_dir)
    raw_files = sorted(media_dir.glob("*-raw.json"))
    if not raw_files:
        logger.warning(f"No *-raw.json manifests under {media_dir} — nothing to parse.")
        return
    for raw_path in raw_files:
        doc = parse_session(raw_path)
        if doc is None:
            continue
        session_id = doc["meta"]["session"]
        out_path = media_dir / f"{session_id}-media.json"
        out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        logger.info(f"Wrote {out_path.name} ({len(doc['data'])} speeches, {doc['meta']['dates']})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    parse_media_directory(args.data_dir / "original" / "media")
