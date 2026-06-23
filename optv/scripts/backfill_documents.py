#! /usr/bin/env python3
"""One-time in-place backfill of DE speech ``documents`` (Drucksachen).

A block of live sessions published with **no** document links: mid-period-20
(≈ session 20117) the Bundestag moved the Drucksache number into an ``<a>``
link, and the old ``parse_documents`` (``doc.text`` only) saw ``"Drucksache "``
with no digits — so every affected session (the tail of period 20 and all of
period 21) has empty ``documents``. The parser is now fixed (it reads the full
element text), but re-running the pipeline to recover the links would re-merge →
trigger a needless aeneas re-align + re-NER of every session.

This recovers the links **without re-running any stage**. Document refs are a
pure metadata transform: they are extracted per proceedings-speech at parse and
unioned onto each merged speech, and the proceedings speech id survives in the
published file as ``textContents[].originTextID``. So for each session we:

  1. re-parse the raw proceedings XML with the fixed parser → map
     ``proceedings-speech-id → documents`` (the source of truth, unchanged);
  2. for every speech in every stage file, union the documents of the
     proceedings ids it carries (``textContents[].originTextID``), dedupe by
     ``sourceURI``, and set ``documents`` — touching no other field.

**Patches the cache files too** (``cache/{merged,aligned,ner}``), not just
``processed/`` — otherwise the next workflow run would re-publish a
document-less cache file over the backfilled ``processed/``. (The publish guard
added alongside this — ``is_demotion`` / ``carry_forward_documents`` in
optv/shared/publish.py — already blocks that, but keeping every stage consistent
avoids relying on it and keeps a re-publish a true no-op.)

This also applies the merger's document **dedupe** (a merged speech that unions
several proceedings items previously accumulated the same Drucksache multiple
times). That changes any session with duplicate refs — including periods 18-19,
which had correct *but duplicated* links — so the default scope is **all
sessions**. It stays safe and idempotent: a session already matching the merger
output (deduped links from the fixed parser) is a byte-identical no-op, and
sessions without a raw proceedings XML (e.g. period 17 / ParlaMint) are skipped.

Deploy the Tools changes to **all** machines before applying, then run once on
one machine and commit the changed ``processed/`` files.

Usage (point ``--dir`` at the data-dir root to cover cache + processed)::

    python -m optv.scripts.backfill_documents --dir <data_dir> --dry-run
    python -m optv.scripts.backfill_documents --dir <data_dir> --apply
    python -m optv.scripts.backfill_documents --dir <data_dir> --session '^21' --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from optv.parliaments.DE.parsers.proceedings2json import parse_transcript
from optv.shared.merge_format import dedupe_documents
from optv.shared.publish import data_signature

logger = logging.getLogger("backfill_documents")

# Every stage output sharing the {meta, data} shape, keyed by session number.
_STAGE_GLOBS = ("*-merged.json", "*-aligned.json", "*-ner.json", "*-session.json")


def _stage_files(directory: Path) -> list[Path]:
    out: list[Path] = []
    for pattern in _STAGE_GLOBS:
        out.extend(directory.rglob(pattern))
    return sorted(set(out))


def _session_of(path: Path) -> str:
    """``21040-session.json`` → ``21040`` (session numbers carry no '-')."""
    return path.name.split("-", 1)[0]


def _documents_by_speech_id(xml_path: Path) -> dict[str, list]:
    """``proceedings-speech-id → documents`` from the (fixed) parser."""
    out: dict[str, list] = {}
    for speech in parse_transcript(str(xml_path)):
        sid = speech.get("originID")
        if sid is not None:
            out[sid] = speech.get("documents") or []
    return out


def _backfill_data(data: list, docs_by_id: dict[str, list]) -> int:
    """Set each speech's ``documents`` from the proceedings ids it carries.
    Returns the number of speeches whose documents changed. Mutates ``data``."""
    changed = 0
    for speech in data:
        origin_ids = [tc.get("originTextID")
                      for tc in (speech.get("textContents") or [])
                      if tc.get("originTextID")]
        # Concatenate in textContents order, then dedupe — matching the merger
        # exactly (merge_session.py unions p['documents'] across proceedingitems
        # in the same order and runs the same dedupe_documents). The backfill
        # output must equal what a re-merge produces, so a future re-publish
        # stays a no-op.
        merged: list = []
        for oid in origin_ids:
            merged.extend(docs_by_id.get(oid, []))
        merged = dedupe_documents(merged)
        if merged != (speech.get("documents") or []):
            speech["documents"] = merged
            changed += 1
    return changed


def _normalize_stage_mtimes(files: list[Path]) -> int:
    """Stamp every stage file with one common mtime so ``Config.is_newer``
    (strict ``>``) never re-runs a stage over backfilled sessions — same
    rationale as optv/scripts/migrate_processed.py."""
    now = time.time()
    for path in files:
        os.utime(path, (now, now))
    return len(files)


def run(directory: Path, apply: bool, session_re: re.Pattern,
        proceedings_dir: Path) -> int:
    files = [f for f in _stage_files(directory) if session_re.search(_session_of(f))]
    if not files:
        logger.warning("no stage files (%s) under %s matching session /%s/",
                       "/".join(_STAGE_GLOBS), directory, session_re.pattern)
        return 0

    # Parse each session's raw XML once, reuse across its 4 stage files.
    docs_cache: dict[str, dict[str, list] | None] = {}

    def docs_for(session: str) -> dict[str, list] | None:
        if session not in docs_cache:
            xml_path = proceedings_dir / f"{session}-proceedings.xml"
            if not xml_path.exists():
                logger.info("session %s: no raw proceedings XML — skipped", session)
                docs_cache[session] = None
            else:
                docs_cache[session] = _documents_by_speech_id(xml_path)
        return docs_cache[session]

    changed_files = 0
    per_session: dict[str, int] = {}
    for path in sorted(files):
        session = _session_of(path)
        docs_by_id = docs_for(session)
        if docs_by_id is None:
            continue
        try:
            doc = json.loads(path.read_text())
        except Exception as exc:  # noqa: BLE001
            logger.error("%s: unreadable (%s)", path.name, exc)
            continue
        data = doc.get("data") or []
        old_sig = data_signature(data)
        n_speeches = _backfill_data(data, docs_by_id)
        if data_signature(data) == old_sig:
            continue
        changed_files += 1
        total_docs = sum(len(s.get("documents") or []) for s in data)
        per_session[session] = total_docs
        logger.info("%s: %d speech(es) gained documents (%d doc refs in session)",
                    path.name, n_speeches, total_docs)
        if apply:
            path.write_text(json.dumps(doc, indent=2, ensure_ascii=False))

    verb = "rewrote" if apply else "would change"
    logger.info("%s %d / %d stage files across %d session(s)",
                verb, changed_files, len(files), len(per_session))
    if apply and changed_files:
        stamped = _normalize_stage_mtimes(files)
        logger.info("normalized mtimes on %d stage files (no spurious stage re-runs)",
                    stamped)
    return changed_files


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", type=Path, required=True,
                        help="data-dir root (covers cache/ + processed/)")
    parser.add_argument("--session", default="",
                        help="regex (re.search) on session number; default '' "
                             "(all sessions — dedupe affects 18-21)")
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
