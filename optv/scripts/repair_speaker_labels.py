#! /usr/bin/env python3
"""Repair speaker/faction labels that the old DE parsers mangled, in place.

The platform reported hundreds of "Speaker not in database" / "Faction not in
database" import conflicts. The Wikidata IDs were in the entity dump all along;
the *labels* handed to NEL were broken before it ever saw them:

* proceedings, Fragestunde: a bare ``<name>Dr. Alexander S. Neu (DIE LINKE) : </name>``
  lost its closing parenthesis to a trailing-punctuation strip, leaving the
  unlinkable ``Alexander S. Neu (DIE LINKE``;
* media titles: a lazy name group stopped at the first ``(``, so
  ``Dr. h. c. (Univ Kyiv) Hans Michelbach (CDU/CSU)`` parsed to label ``h. c.``,
  role ``Univ Kyiv) Hans Michelbach (CDU``, faction ``CSU``.

Both are fixed in the parsers, but that only helps sessions parsed from now on:
the parse stage is mtime-gated (and ignores --force), and even a re-merge cannot
reach ``processed/`` -- the publish demotion guard refuses to overwrite an
aligned/NER'd session with a bare merge. Hence this backfill.

It rewrites nothing but ``people[]``. Transcript text, timings, NER annotations
and documents are left exactly as they are, so no stage has to re-run:

* the media speaker is re-derived from ``debug.originalTitle``, which every
  merged speech carries -- so the shredded name is recovered from the raw title
  rather than guessed at from its own wreckage;
* every other speaker's label is pushed back through the fixed ``fix_fullname``,
  which drops the faction/constituency parenthetical and the academic title;
* NEL then re-links the repaired labels against ``metadata/entities.json``.

Like optv/scripts/backfill_documents.py this touches every stage file
(``cache/{merged,aligned,ner}`` + ``processed/``) so a later stage cannot
resurrect a stale label from the cache, and it stamps one common mtime
afterwards so ``Config.is_newer`` does not re-run a stage over the result.

Only ``processed/`` travels between machines (it is the git-tracked artifact and
what ``richest_source`` prefers), so a single run -- committed and pushed -- is
enough to fix the platform. Running it again on another worker is idempotent and
merely cleans that worker's local cache.

    python -m optv.scripts.repair_speaker_labels --dir <data_dir>            # dry run
    python -m optv.scripts.repair_speaker_labels --dir <data_dir> --apply
"""

import argparse
import json
import logging
import os
import re
import time
from collections import Counter
from pathlib import Path

from lxml import etree

from optv.parliaments.DE.parsers.common import (fix_faction, fix_fullname, fix_role,
                                                parse_fullname, split_role_faction)
from optv.parliaments.DE.parsers.media2json import extract_title_data
from optv.parliaments.DE.parsers.proceedings2json import (faction_from_redner,
                                                          speaker_from_redner)
from optv.shared.nel import get_nel_data, link_entities
from optv.shared.publish import data_signature

logger = logging.getLogger(__name__)

_STAGE_GLOBS = ("*-merged.json", "*-aligned.json", "*-ner.json", "*-session.json")

# The speaker the media feed names for the clip. Everyone else in people[] comes
# from the proceedings and only needs the label pushed through fix_fullname.
_MEDIA_CONTEXT = "main-speaker"


def _stage_files(directory: Path) -> list[Path]:
    out: list[Path] = []
    for pattern in _STAGE_GLOBS:
        out.extend(directory.rglob(pattern))
    return sorted(set(out))


def _session_of(path: Path) -> str:
    """``21040-session.json`` → ``21040`` (session numbers carry no '-')."""
    return path.name.split("-", 1)[0]


def name_repairs_from_xml(xml_path: Path) -> dict:
    """{name the old parser produced: name the display text gives} for one session.

    A dropped name particle leaves no trace in the published label -- nothing in
    "Thomas Maizière" says a "de" is missing -- so unlike the media speaker it
    cannot be repaired from the session file alone. The proceedings XML still
    has both: the (lossy) structured fields the old parser read, and the display
    text after </redner> that the new one reads.

    Entries where one old name maps to several new ones are dropped: those are
    the corrupt <redner> records that fused two people
    ("Dirk-UlrichAlexander Mende Föhr" is both Dirk-Ulrich Mende and Alexander
    Föhr), and the published label cannot say which speech belongs to whom.
    """
    try:
        root = etree.parse(str(xml_path)).getroot()
    except Exception as exc:  # noqa: BLE001
        logger.error("%s: unparseable (%s)", xml_path.name, exc)
        return {}
    sitzungsverlauf = root.find('sitzungsverlauf')
    if sitzungsverlauf is None:
        return {}

    candidates: dict[str, set] = {}
    for redner in sitzungsverlauf.findall('.//redner'):
        old, _status = parse_fullname(
            f"{redner.findtext('.//vorname') or ''} "
            f"{redner.findtext('.//namenszusatz') or ''} "
            f"{redner.findtext('.//nachname') or ''}")
        new, _status = speaker_from_redner(redner)
        if not old or not new or old == new:
            continue
        # The corrupt records duplicate the faction as well ("SPDSPD"), and the
        # display text carries the right one, so repair them together -- keyed by
        # the name, which is what identifies the person in the published file.
        candidates.setdefault(old, set()).add((new, faction_from_redner(redner)))

    repairs = {}
    for old, news in candidates.items():
        if len(news) == 1:
            name, faction = next(iter(news))
            repairs[old] = {"label": name, "faction": faction}
        else:
            logger.warning("ambiguous label %r -> %s; left for a re-parse",
                           old, sorted(n for n, _f in news))
    return repairs


def _faction_is_broken(person: dict) -> bool:
    """A named faction that NEL could not link -- i.e. a reported conflict."""
    faction = person.get("faction")
    return (isinstance(faction, dict)
            and bool(faction.get("label"))
            and not faction.get("wid"))


def _repair_media_speaker(person: dict, title: str,
                          fix_person: bool, fix_faction_: bool) -> bool:
    """Re-derive the broken fields of the media speaker from the raw title.

    The old regex scattered the name across label, role *and* faction, so a
    broken name means role has to be re-derived too or its wreckage stays behind
    (role "Univ Kyiv) Hans Michelbach (CDU").

    Only the broken fields are touched. A faction that already carries a wid is
    left alone: the merger may have taken it from the proceedings, whose label
    ("BÜNDNIS 90/DIE GRÜNEN") is the canonical one the platform displays, while
    the media title spells the same faction "B90/GRÜNE".
    """
    metadata = extract_title_data(title)
    if not metadata:
        return False
    label = fix_fullname(metadata.get("fullname", ""))
    if not label:
        return False
    role, faction = split_role_faction(metadata.get("faction", ""))

    changed = False
    if fix_person:
        if person.get("label") != label:
            person["label"] = label
            changed = True
        role = fix_role(role) if role else None
        if role:
            if person.get("role") != role:
                person["role"] = role
                changed = True
        elif "role" in person:
            del person["role"]
            changed = True

    if fix_faction_ and faction:
        faction_label = fix_faction(faction)
        current = person.get("faction")
        current = current if isinstance(current, dict) else {}
        if current.get("label") != faction_label:
            # Drop the empty wid with it; NEL re-links from the repaired label.
            person["faction"] = {"label": faction_label}
            changed = True
    return changed


def _repair_speech(speech: dict, name_repairs: dict) -> bool:
    """Repair only the people NEL failed to link -- everything else is correct
    and must not be rewritten (re-deriving a linked speaker from the media title
    would replace good proceedings-sourced labels with the feed's spelling)."""
    changed = False
    title = (speech.get("debug") or {}).get("originalTitle") or ""
    for person in speech.get("people") or []:
        broken_person = not person.get("wid")
        broken_faction = _faction_is_broken(person)
        if not broken_person and not broken_faction:
            continue
        if person.get("context") == _MEDIA_CONTEXT and title:
            if _repair_media_speaker(person, title, broken_person, broken_faction):
                changed = True
            continue
        label = person.get("label")
        if not label:
            continue
        repair = name_repairs.get(label) or {}

        if broken_faction and repair.get("faction"):
            # The corrupt <redner> records duplicate the faction too ("SPDSPD").
            person["faction"] = {"label": repair["faction"]}
            changed = True
        if not broken_person:
            continue
        # A name the XML's display text can correct (dropped particle, doubled
        # field) first; otherwise just re-clean the label itself.
        fixed = repair.get("label") or fix_fullname(label)
        if fixed and fixed != label:
            person["label"] = fixed
            changed = True
    return changed


def repair_data(data: list, persons: dict, factions: dict,
                name_repairs: dict | None = None) -> int:
    """Repair people[] across a session's speeches and re-link. Returns #speeches."""
    name_repairs = name_repairs or {}
    touched = sum(1 for speech in data if _repair_speech(speech, name_repairs))
    if touched:
        # Fill-only: every label we fixed had no wid (that is why it was a
        # conflict), so this adds ids without overwriting good ones.
        link_entities(data, persons, factions)
    return touched


def _unlinked(data: list) -> Counter:
    out: Counter = Counter()
    for speech in data:
        for person in speech.get("people") or []:
            if not person.get("wid"):
                out[f"person:{person.get('label')}"] += 1
            faction = person.get("faction")
            if isinstance(faction, dict) and faction.get("label") and not faction.get("wid"):
                out[f"faction:{faction['label']}"] += 1
    return out


def _normalize_stage_mtimes(files: list[Path]) -> int:
    """Stamp every stage file with one common mtime so ``Config.is_newer``
    (strict ``>``) never re-runs a stage over backfilled sessions — same
    rationale as optv/scripts/backfill_documents.py."""
    now = time.time()
    for path in files:
        os.utime(path, (now, now))
    return len(files)


def run(directory: Path, apply: bool, session_re: re.Pattern,
        proceedings_dir: Path) -> int:
    persons, factions = get_nel_data(directory / "metadata")
    if not persons or not factions:
        logger.error("no entity dump under %s/metadata — cannot re-link", directory)
        return 0

    files = [f for f in _stage_files(directory) if session_re.search(_session_of(f))]
    if not files:
        logger.warning("no stage files under %s matching session /%s/",
                       directory, session_re.pattern)
        return 0

    # Parsed once per session, shared by its 4 stage files, and only for sessions
    # that actually carry an unlinked speaker -- parsing 1000+ XMLs to repair a
    # few dozen labels would be pure waste.
    repairs_cache: dict[str, dict] = {}

    def repairs_for(session: str) -> dict:
        if session not in repairs_cache:
            xml_path = proceedings_dir / f"{session}-proceedings.xml"
            repairs_cache[session] = (name_repairs_from_xml(xml_path)
                                      if xml_path.exists() else {})
        return repairs_cache[session]

    changed_files = 0
    before: Counter = Counter()
    after: Counter = Counter()
    for path in sorted(files):
        try:
            doc = json.loads(path.read_text())
        except Exception as exc:  # noqa: BLE001
            logger.error("%s: unreadable (%s)", path.name, exc)
            continue
        data = doc.get("data") or []
        old_sig = data_signature(data)
        was = _unlinked(data)
        name_repairs = repairs_for(_session_of(path)) if was else {}
        touched = repair_data(data, persons, factions, name_repairs)
        now = _unlinked(data)
        # Count every published session, not just the ones we can repair -- the
        # totals have to include what this backfill *cannot* fix, or they claim
        # a win over a corpus we never looked at.
        if path.name.endswith("-session.json"):
            before += was
            after += now
        if data_signature(data) == old_sig:
            continue
        changed_files += 1
        logger.info("%s: repaired %d speech(es), unlinked %d -> %d",
                    path.name, touched, sum(was.values()), sum(now.values()))
        if apply:
            path.write_text(json.dumps(doc, indent=2, ensure_ascii=False))

    verb = "rewrote" if apply else "would change"
    logger.info("%s %d / %d stage files", verb, changed_files, len(files))
    logger.info("processed/: unlinked entries %d -> %d",
                sum(before.values()), sum(after.values()))
    if after:
        logger.info("still unlinked after repair (top 10, needs a different fix):")
        for key, count in after.most_common(10):
            logger.info("    %4dx %s", count, key)
    if apply and changed_files:
        logger.info("normalized mtimes on %d stage files (no spurious stage re-runs)",
                    _normalize_stage_mtimes(files))
    return changed_files


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", type=Path, required=True,
                        help="data-dir root (covers cache/ + processed/)")
    parser.add_argument("--session", default="",
                        help="regex (re.search) on session number; default '' (all)")
    parser.add_argument("--proceedings-dir", type=Path, default=None,
                        help="raw proceedings XML dir "
                             "(default <dir>/original/proceedings)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", default=True,
                       help="report changes without writing (default)")
    group.add_argument("--apply", action="store_true", help="write changes in place")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    proceedings_dir = args.proceedings_dir or (args.dir / "original" / "proceedings")
    run(args.dir, apply=args.apply, session_re=re.compile(args.session),
        proceedings_dir=proceedings_dir)


if __name__ == "__main__":
    main()
