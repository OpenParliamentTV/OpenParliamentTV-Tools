#! /usr/bin/env python3
"""One-time in-place normalization of stray formatting whitespace in published
speech text — ``processed/*-session.json`` **and** the cache stage files —
*without re-running the pipeline*.

Some proceedings carry source pretty-print artifacts: a newline or tab plus
indentation embedded mid-sentence (e.g. ``"wir\\n            auf"``,
``"mehrjährigen\\t Bundesinvestitionsfonds"``). Those reach the platform DB /
search index and every downstream consumer (transcript, meta-image, WebVTT,
feeds) verbatim, where they render as literal ``\\n`` glyphs, blank gaps in the
quote image, and broken VTT cues. This collapses any whitespace run that
*contains a newline/CR/tab* to a single space, in every
``textContents[].textBody[].text`` and ``.sentences[].text``. Benign runs of
plain spaces are intentionally left untouched — they are not the defect and
render fine, and touching them would rewrite most of the corpus.

Safe without any re-alignment / re-NER: timing (``timeStart``/``timeEnd``) and
NER ``entities`` are per-sentence and offset-free, so nothing derived from the
text needs to change.

Idempotent (a file is rewritten only when its ``data`` signature changes) and,
like ``migrate_processed``, it **patches the cache files too** and normalizes
stage mtimes so a later re-publish from a stale ``cache/{merged,aligned,ner}``
can't silently revert the fix and the next workflow run doesn't needlessly
re-run merge/align/NER. See ``migrate_processed`` for the rationale on both.

Usage (point ``--dir`` at the data-dir root to cover cache + processed)::

    python -m optv.scripts.normalize_speech_whitespace --dir <data_dir> --dry-run
    python -m optv.scripts.normalize_speech_whitespace --dir <data_dir> --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from optv.scripts.migrate_processed import _normalize_stage_mtimes, _stage_files
from optv.shared.publish import data_signature

logger = logging.getLogger("normalize_speech_whitespace")

# Collapse any whitespace run that contains a newline/CR/tab (source pretty-print
# artifact) — together with the plain spaces/tabs flanking it — to a single
# space. A run of only plain spaces matches nothing here and is left as-is.
_STRUCTURAL_WS = re.compile(r"[ \t]*[\r\n\t]+[ \t]*")


def _normalize(text: str) -> str:
    return _STRUCTURAL_WS.sub(" ", text)


def normalize_data(data: list) -> bool:
    """Normalize speech text in ``data`` in place. Returns True if it changed."""
    before = data_signature(data)
    for speech in data:
        for tc in speech.get("textContents") or []:
            if not isinstance(tc, dict):
                continue
            for item in tc.get("textBody") or []:
                if not isinstance(item, dict):
                    continue
                if isinstance(item.get("text"), str):
                    item["text"] = _normalize(item["text"])
                for sentence in item.get("sentences") or []:
                    if isinstance(sentence, dict) and isinstance(sentence.get("text"), str):
                        sentence["text"] = _normalize(sentence["text"])
    return data_signature(data) != before


def run(directory: Path, apply: bool) -> int:
    files = _stage_files(directory)
    if not files:
        logger.warning("no stage files under %s", directory)
        return 0
    changed = 0
    for path in files:
        try:
            doc = json.loads(path.read_text())
        except Exception as exc:  # noqa: BLE001
            logger.error("%s: unreadable (%s)", path.name, exc)
            continue
        if not normalize_data(doc.get("data") or []):
            continue
        changed += 1
        if apply:
            path.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    verb = "rewrote" if apply else "would change"
    logger.info("%s %d / %d files", verb, changed, len(files))
    if apply:
        stamped = _normalize_stage_mtimes(directory)
        logger.info("normalized mtimes on %d stage files (no spurious stage re-runs)", stamped)
    return changed


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", type=Path, required=True,
                        help="data-dir root (covers cache/ + processed/)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", default=True,
                       help="report changes without writing (default)")
    group.add_argument("--apply", action="store_true", help="write changes in place")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.dir, apply=args.apply)


if __name__ == "__main__":
    main()
