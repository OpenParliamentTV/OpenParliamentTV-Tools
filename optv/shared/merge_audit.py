#! /usr/bin/env python3

"""Read-only diagnostic sweep over merger outputs.

Walks ``processed/*-session.json`` and/or ``cache/merged/*-merged.json`` and
produces three artefacts that quantify merger anomalies before any data-shape
change is approved:

  * ``bettermann.tsv``   — speeches matching the tail-accumulation fingerprint
                           (``len(textContents) > 5 AND confidence == 1 AND
                           len(linkedMediaIndexes) == 1 AND speaker is not
                           the chair``).
  * ``len_histogram.json`` — distribution of ``len(textContents)`` per speech,
                           split into ``all`` / ``gate_pass`` / ``synthetic``
                           buckets.
  * ``synthetic_ids.tsv`` — every speech whose top-level ``originTextID``
                           ends in ``-outro``/``-intro``, with the previous
                           speech's speaker for context.

Plus a short ``SWEEP.md`` summary readable at a glance.

Field choice — the script keys on top-level ``originTextID`` rather than
``originID`` because in DE 18-21 outputs ``originID`` resolves to ``None``
at the top level (the synthetic suffix only persists on ``originTextID``).
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import json
import logging
from pathlib import Path
import re
import sys
from typing import Iterator, Optional

logger = logging.getLogger("merge_audit")


CHAIR_CONTEXTS = {"president", "vice-president", "interim-president"}
_CHAIR_NAME_RE = re.compile(r"^(Vize)?präsident(in)?\b|^Alterspräsident", re.IGNORECASE)
SYNTHETIC_SUFFIXES = ("-outro", "-intro", "-closing", "-post")
SYNTH_SUSPECT = ("-outro", "-intro")


def is_chair(speech: dict) -> bool:
    p = (speech.get("people") or [{}])[0]
    if (p.get("context") or "") in CHAIR_CONTEXTS:
        return True
    return bool(_CHAIR_NAME_RE.match(p.get("label") or ""))


def origin_suffix(speech: dict) -> Optional[str]:
    oid = speech.get("originTextID") or ""
    for s in SYNTHETIC_SUFFIXES:
        if oid.endswith(s):
            return s
    return None


def gate_pass(speech: dict) -> bool:
    debug = speech.get("debug") or {}
    conf = debug.get("confidence")
    lmi = debug.get("linkedMediaIndexes") or []
    return conf == 1 and lmi == [speech.get("speechIndex")]


def speaker_label(speech: dict) -> str:
    return (speech.get("people") or [{}])[0].get("label") or ""


def speaker_context(speech: dict) -> str:
    return (speech.get("people") or [{}])[0].get("context") or ""


def walk(data_dir: Path, source: str, glob: str) -> Iterator[tuple[str, str, dict]]:
    """Yield ``(session_id, source_label, doc)`` per matched file."""
    roots: list[tuple[str, Path, str]] = []
    if source in ("processed", "both"):
        roots.append(("processed", data_dir / "processed", f"{glob}-session.json"))
    if source in ("merged", "both"):
        roots.append(("merged", data_dir / "cache" / "merged", f"{glob}-merged.json"))

    for label, root, pat in roots:
        if not root.is_dir():
            logger.warning("missing source root: %s", root)
            continue
        for f in sorted(root.glob(pat)):
            try:
                doc = json.loads(f.read_text())
            except (OSError, json.JSONDecodeError) as e:
                logger.warning("skipping %s: %s", f, e)
                continue
            session_id = f.name.split("-")[0]
            yield session_id, label, doc


def collect(data_dir: Path, source: str, glob: str):
    bettermann_rows: list[dict] = []
    synth_rows: list[dict] = []
    hist_all: Counter[int] = Counter()
    hist_gate: Counter[int] = Counter()
    hist_synth: Counter[int] = Counter()
    by_source_all: dict[str, Counter[int]] = {"processed": Counter(), "merged": Counter()}
    sessions_seen: set[tuple[str, str]] = set()

    for session, src, doc in walk(data_dir, source, glob):
        sessions_seen.add((session, src))
        speeches = doc.get("data") or []
        by_idx = {s.get("speechIndex"): s for s in speeches}

        for s in speeches:
            n = len(s.get("textContents") or [])
            debug = s.get("debug") or {}
            conf = debug.get("confidence")
            lmi = debug.get("linkedMediaIndexes") or []
            suf = origin_suffix(s)
            gate = gate_pass(s)
            chair = is_chair(s)

            hist_all[n] += 1
            by_source_all[src][n] += 1
            if gate:
                hist_gate[n] += 1
            if suf:
                hist_synth[n] += 1

            if n > 5 and conf == 1 and len(lmi) == 1 and not chair:
                ag = s.get("agendaItem") or {}
                bettermann_rows.append({
                    "session": session,
                    "src": src,
                    "speechIndex": s.get("speechIndex"),
                    "originTextID": s.get("originTextID") or "",
                    "speaker_label": speaker_label(s),
                    "speaker_context": speaker_context(s),
                    "len_textContents": n,
                    "confidence": conf,
                    "lmi": ",".join(str(x) for x in lmi),
                    "agenda_title": ag.get("title") or ag.get("officialTitle") or "",
                    "agenda_type": ag.get("type") or "",
                })

            if suf in SYNTH_SUSPECT:
                prev = by_idx.get((s.get("speechIndex") or 0) - 1)
                prev_label = speaker_label(prev) if prev else ""
                synth_rows.append({
                    "session": session,
                    "src": src,
                    "speechIndex": s.get("speechIndex"),
                    "originTextID": s.get("originTextID") or "",
                    "suffix": suf,
                    "speaker_label": speaker_label(s),
                    "speaker_context": speaker_context(s),
                    "len_textContents": n,
                    "confidence": conf,
                    "lmi": ",".join(str(x) for x in lmi),
                    "prev_speaker_label": prev_label,
                    "prev_speaker_same": "yes" if prev_label and prev_label == speaker_label(s) else "no",
                })

    return {
        "bettermann_rows": bettermann_rows,
        "synth_rows": synth_rows,
        "hist_all": hist_all,
        "hist_gate": hist_gate,
        "hist_synth": hist_synth,
        "hist_by_source": by_source_all,
        "sessions_seen": sessions_seen,
    }


def write_tsv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("")
        return
    fields = list(rows[0].keys())
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


def write_histogram(path: Path, results: dict) -> None:
    payload = {
        "all": dict(sorted(results["hist_all"].items())),
        "gate_pass": dict(sorted(results["hist_gate"].items())),
        "synthetic_origin": dict(sorted(results["hist_synth"].items())),
        "by_source": {
            src: dict(sorted(c.items()))
            for src, c in results["hist_by_source"].items()
            if c
        },
    }
    path.write_text(json.dumps(payload, indent=2))


def write_summary(path: Path, results: dict, args) -> None:
    bett = results["bettermann_rows"]
    synth = results["synth_rows"]
    hist_gate = results["hist_gate"]

    sessions = sorted({s for s, _ in results["sessions_seen"]})
    total_speeches = sum(results["hist_all"].values())
    gate_count = sum(hist_gate.values())

    tail = sorted(((k, v) for k, v in hist_gate.items() if k > 5),
                  key=lambda kv: -kv[0])
    top_suspects = sorted(bett, key=lambda r: -r["len_textContents"])[:20]

    lines = [
        "# Merger sweep summary",
        "",
        f"- data-dir: `{args.data_dir}`",
        f"- source: `{args.source}`  glob: `{args.session_glob}`",
        f"- sessions inspected: {len(sessions)}  total speeches: {total_speeches}",
        f"- gate-passing speeches (conf=1 and lmi=[self]): {gate_count}",
        f"- Bettermann fingerprint hits: **{len(bett)}**",
        f"- synthetic-id (-outro/-intro) speeches: {len(synth)}",
        "",
        "## gate_pass tail (len(textContents) > 5, sorted by len desc)",
        "",
    ]
    if tail:
        lines.append("| len | count |")
        lines.append("|---:|---:|")
        for k, v in tail:
            lines.append(f"| {k} | {v} |")
    else:
        lines.append("_none_")
    lines.append("")

    lines.append("## Top 20 Bettermann suspects (descending by len_textContents)")
    lines.append("")
    if top_suspects:
        lines.append("| session | idx | len | speaker | agenda |")
        lines.append("|---|---:|---:|---|---|")
        for r in top_suspects:
            lines.append(
                f"| {r['session']} | {r['speechIndex']} | {r['len_textContents']} | "
                f"{r['speaker_label']} | {r['agenda_title']} |"
            )
    else:
        lines.append("_none_")
    lines.append("")

    path.write_text("\n".join(lines))


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="merge_audit",
        description="Read-only sweep over merger outputs (Bettermann fingerprint, "
                    "len(textContents) histogram, synthetic-ID context).",
    )
    ap.add_argument("--data-dir", required=True, type=Path,
                    help="OPTV data root (must contain processed/ and/or cache/merged/).")
    ap.add_argument("--session-glob", default="*",
                    help="Session-id glob, e.g. '21*' (default: '*').")
    ap.add_argument("--source", choices=("processed", "merged", "both"), default="both",
                    help="Which side of the pipeline to inspect (default: both).")
    ap.add_argument("--out-dir", type=Path, default=Path("_planning/whisper_qc/sweep"),
                    help="Where to write artefacts (created if missing).")
    ap.add_argument("--debug", action="store_true")
    return ap


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args.out_dir.mkdir(parents=True, exist_ok=True)

    results = collect(args.data_dir, args.source, args.session_glob)
    write_tsv(args.out_dir / "bettermann.tsv", results["bettermann_rows"])
    write_tsv(args.out_dir / "synthetic_ids.tsv", results["synth_rows"])
    write_histogram(args.out_dir / "len_histogram.json", results)
    write_summary(args.out_dir / "SWEEP.md", results, args)

    logger.info("wrote artefacts to %s", args.out_dir)
    logger.info("Bettermann hits: %d  synthetic -outro/-intro: %d  total speeches: %d",
                len(results["bettermann_rows"]),
                len(results["synth_rows"]),
                sum(results["hist_all"].values()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
