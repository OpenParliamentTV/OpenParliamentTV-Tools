#! /usr/bin/env python3
"""One-time in-place migration of already-published ``processed/*-session.json``.

Converges existing output to the unified Stage-2 shape **without re-running the
pipeline** (no aeneas/NER), because the deltas are pure metadata transforms:

  1. ``meta`` → canonical ``build_meta`` shape (adds ``schemaVersion``,
     ``parliament``, ``electoralPeriod`` as ``{"number": int}``,
     ``lastProcessing``/``lastUpdate``; keeps processing history + bespoke keys).
  2. ``originalLanguage`` filled per speech from the manifest ``language_code``
     (monolingual parliaments; multilingual EU/FI keep their per-speech value).

Rights (``creator``/``license``) are intentionally *not* touched: the manifest
values are byte-identical to what is already published (verified for DE).

Idempotent: a file is rewritten only when its ``data`` payload or its structural
``meta`` (ignoring the volatile ``lastUpdate``) actually changes, so re-running
is a no-op and untouched files keep their mtime (and the platform's mtime-diff
importer ignores them).

**Patches the cache files too**, not just ``processed/``. The publish step
(``workflow._publish_as_processed``) copies the latest cache file (``merged`` /
``aligned`` / ``ner``) verbatim into ``processed/`` and compares only the
``data`` signature — so if only ``processed/`` were patched, the next workflow
run would re-publish the un-migrated cache and silently revert the migration.
Migrating ``cache/{merged,aligned,ner}`` + ``processed`` keeps every stage's
output consistent, so a re-publish from cache carries the unified shape.

Usage (point ``--dir`` at the data-dir root to cover cache + processed)::

    python -m optv.shared.migrate_processed --dir <data_dir> --dry-run
    python -m optv.shared.migrate_processed --dir <data_dir> --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from optv.shared.meta import build_meta, fill_original_language
from optv.shared.publish import data_signature, strip_legacy_textbody_ids

logger = logging.getLogger("migrate_processed")

# debug.* keys renamed to camelCase (WS7). Applied to each speech's debug dict
# so already-published files converge to the same keys the parsers now emit.
# Source-structural keys (media.additionalInformation, processing stage names,
# textBody.speech_id) are intentionally NOT renamed.
_DEBUG_RENAME = {
    "align-duration": "alignDuration", "ner-duration": "nerDuration",
    "confidence_reason": "confidenceReason", "proceedings-source": "proceedingsSource",
    "media-source": "mediaSource", "align-error": "alignError", "align-skip": "alignSkip",
    "align-source": "alignSource", "page-range": "pageRange", "block-index": "blockIndex",
    "coarse-timing": "coarseTiming", "h3-label": "h3Label",
    "is-procedural-label": "isProceduralLabel", "sign-player-id": "signPlayerId",
    "std-player-id": "stdPlayerId", "transcript-cHash": "transcriptCHash",
    "transcript-paragraph-count": "transcriptParagraphCount",
    "transcript-speaker-id": "transcriptSpeakerId",
    "anforande_nummer": "anforandeNummer", "sak_nummer": "sakNummer",
    "speech_type": "speechType", "clock_time": "clockTime", "debatt_titel": "debattTitel",
    "has_gazette": "hasGazette", "ivod_id": "ivodId",
    "whisperx_last_end": "whisperxLastEnd", "whisperx_segments": "whisperxSegments",
}


def _rename_debug_keys(data: list) -> bool:
    """Rename non-camelCase debug keys in place. Returns True if anything changed."""
    changed = False
    for speech in data:
        debug = speech.get("debug")
        if not isinstance(debug, dict):
            continue
        for old, new in _DEBUG_RENAME.items():
            if old in debug and new not in debug:
                debug[new] = debug.pop(old)
                changed = True
    return changed


def _latest_stage(processing: dict, fallback: str = "merge") -> str:
    """The processing stage with the most recent timestamp (lastProcessing)."""
    if not processing:
        return fallback
    return max(processing, key=lambda k: processing[k])


def _rebuild_meta(meta: dict, data: list, parliament_id: str) -> dict:
    # build_meta drops any inherited parliament/electoralPeriod, so migrating an
    # already-published file removes those meta-level duplicates of the per-speech
    # fields.
    processing = meta.get("processing") or {}
    return build_meta(
        parliament_id,
        session=meta.get("session"),
        date_start=meta.get("dateStart"),
        date_end=meta.get("dateEnd"),
        processing=processing,
        last_processing=meta.get("lastProcessing") or _latest_stage(processing),
        last_update=meta.get("lastUpdate"),  # preserve if present → idempotent
        inherit=meta,
    )


def _meta_structural(meta: dict) -> dict:
    """Meta minus the volatile timestamp, for change detection."""
    return {k: v for k, v in meta.items() if k != "lastUpdate"}


def _backfill_rights(data: list, parliament_id: str) -> None:
    """Fill missing ``media``/``textContents`` ``creator``+``license`` from the
    manifest (honouring per-period overrides). Static values, so this converges a
    file whose stage emitted no rights (e.g. EU pre-fix) without a re-run."""
    from optv.parliaments import get_rights

    for speech in data:
        period = (speech.get("electoralPeriod") or {}).get("number")
        media = speech.get("media")
        if isinstance(media, dict):
            mr = get_rights(parliament_id, period=period, stream="media")
            for k in ("creator", "license"):
                if not media.get(k) and mr.get(k):
                    media[k] = mr[k]
        tcs = speech.get("textContents") or []
        if tcs:
            pr = get_rights(parliament_id, period=period, stream="proceedings")
            for tc in tcs:
                if not isinstance(tc, dict):
                    continue
                for k in ("creator", "license"):
                    if not tc.get(k) and pr.get(k):
                        tc[k] = pr[k]


def _fix_origin_media_id_placement(data: list) -> None:
    """Move a stale speech-level ``originMediaID`` to ``media.originMediaID`` (the
    canonical slot) and drop the top-level copy — it's a duplicate read by nothing
    (speech identity is ``originID`` / ``speechIndex``)."""
    for speech in data:
        if "originMediaID" not in speech:
            continue
        media = speech.get("media")
        if isinstance(media, dict) and not media.get("originMediaID"):
            media["originMediaID"] = speech["originMediaID"]
        del speech["originMediaID"]


def migrate_doc(doc: dict) -> tuple[bool, dict]:
    """Return ``(changed, new_doc)``. Does not write."""
    data = doc.get("data") or []
    meta = doc.get("meta") or {}
    parliament_id = next((sp.get("parliament") for sp in data if sp.get("parliament")), None)
    if not parliament_id:
        return False, doc

    old_sig = data_signature(data)
    fill_original_language(data, parliament_id)
    _backfill_rights(data, parliament_id)
    _rename_debug_keys(data)
    strip_legacy_textbody_ids(data)
    _fix_origin_media_id_placement(data)
    data_changed = data_signature(data) != old_sig

    new_meta = _rebuild_meta(meta, data, parliament_id)
    meta_changed = _meta_structural(new_meta) != _meta_structural(meta)

    new_doc = {"meta": new_meta, "data": data}
    return (data_changed or meta_changed), new_doc


# Every stage output that shares the {meta, data} shape. Patching all of them
# (not just processed) prevents a re-publish from a stale cache reverting the
# migration — see module docstring.
_STAGE_GLOBS = ("*-merged.json", "*-aligned.json", "*-ner.json", "*-session.json")


def _stage_files(directory: Path) -> list[Path]:
    """All stage outputs under ``directory`` (recursive), so a data-dir root
    covers ``cache/{merged,aligned,ner}`` and ``processed/`` in one pass."""
    out: list[Path] = []
    for pattern in _STAGE_GLOBS:
        out.extend(directory.rglob(pattern))
    return sorted(set(out))


def _normalize_stage_mtimes(directory: Path) -> int:
    """Stamp every stage file with one common mtime.

    The workflow re-runs a stage when its input file is newer than its output
    (``Config.is_newer``, strict ``>``). Rewriting files leaves them with their
    write-order mtimes — e.g. ``merged`` ends up newer than ``aligned`` — which
    would make the next cron needlessly re-run align/ner over already-migrated
    sessions. Setting merged/aligned/ner/processed to the *same* timestamp makes
    every ``is_newer`` comparison False (no re-run), and they all end up newer
    than the untouched (earlier-pulled) ``original/`` parsed files (so merge
    doesn't re-run either). Idempotent and runs even on a no-content-change pass,
    so re-applying repairs a previously mis-ordered migration. Returns the count.
    """
    now = time.time()
    n = 0
    for path in _stage_files(directory):
        os.utime(path, (now, now))
        n += 1
    return n


def run(directory: Path, apply: bool) -> int:
    files = _stage_files(directory)
    if not files:
        logger.warning("no stage files (%s) under %s",
                       "/".join(_STAGE_GLOBS), directory)
        return 0
    changed = 0
    sample_shown = False
    for path in files:
        try:
            doc = json.loads(path.read_text())
        except Exception as exc:  # noqa: BLE001
            logger.error("%s: unreadable (%s)", path.name, exc)
            continue
        did_change, new_doc = migrate_doc(doc)
        if not did_change:
            continue
        changed += 1
        if not sample_shown:
            logger.info("sample meta diff for %s:\n  before: %s\n  after:  %s",
                        path.name,
                        json.dumps(doc.get("meta", {}), ensure_ascii=False),
                        json.dumps(new_doc["meta"], ensure_ascii=False))
            sample_shown = True
        if apply:
            path.write_text(json.dumps(new_doc, indent=2, ensure_ascii=False))
    verb = "rewrote" if apply else "would change"
    logger.info("%s %d / %d files", verb, changed, len(files))
    if apply:
        stamped = _normalize_stage_mtimes(directory)
        logger.info("normalized mtimes on %d stage files (no spurious stage re-runs)", stamped)
    return changed


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", type=Path, required=True, help="processed/ directory")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", default=True,
                       help="report changes without writing (default)")
    group.add_argument("--apply", action="store_true", help="write changes in place")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.dir, apply=args.apply)


if __name__ == "__main__":
    main()
