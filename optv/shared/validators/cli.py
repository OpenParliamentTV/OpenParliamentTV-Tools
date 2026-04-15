"""CLI for OPTV Stage 2 validators.

Usage:
    python -m optv.shared.validators.cli --file path/to/session.json
    python -m optv.shared.validators.cli --dir path/to/processed/
    python -m optv.shared.validators.cli --dir ./processed --schema minimal --no-semantic
    python -m optv.shared.validators.cli --dir ./processed --summary
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from . import validate_stage2


def _load(path):
    with path.open() as f:
        return json.load(f)


def _run_one(path, schema, semantic):
    try:
        doc = _load(path)
    except Exception as e:
        return [{"severity": "error", "rule": "parse.error", "path": "<file>",
                 "message": f"{type(e).__name__}: {e}"}]
    return validate_stage2(doc, schema=schema, semantic=semantic)


def _print_findings(name, findings, fmt):
    if fmt == "json":
        print(json.dumps({"file": name, "findings": findings}))
        return
    for f in findings:
        sev = f["severity"].upper()
        print(f"  [{sev}] {f['rule']} @ {f['path']}: {f['message']}")


def main():
    ap = argparse.ArgumentParser(description="Validate OPTV Stage 2 session files.")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--file", type=Path, help="Validate a single session JSON file.")
    src.add_argument("--dir", type=Path, help="Validate all *.json files in a directory.")
    ap.add_argument("--schema", choices=["minimal", "full"], default="full",
                    help="Which schema to validate against (default: full).")
    ap.add_argument("--no-semantic", action="store_true",
                    help="Skip the semantic validator (schema only).")
    ap.add_argument("--summary", action="store_true",
                    help="Print aggregate counts instead of per-file findings.")
    ap.add_argument("--fmt", choices=["text", "json"], default="text",
                    help="Per-file output format (ignored with --summary).")
    ap.add_argument("--max-warnings", type=int, default=None,
                    help="Show at most this many warning lines per file (text only).")
    args = ap.parse_args()

    semantic = not args.no_semantic
    files = [args.file] if args.file else sorted(args.dir.glob("*.json"))
    if not files:
        print(f"No JSON files found at {args.dir}", file=sys.stderr)
        sys.exit(2)

    total = len(files)
    errors_files = 0
    warnings_files = 0
    clean_files = 0
    rule_counts = Counter()
    per_file_counts = []

    for i, f in enumerate(files, 1):
        findings = _run_one(f, args.schema, semantic)
        errs = [x for x in findings if x["severity"] == "error"]
        warns = [x for x in findings if x["severity"] == "warning"]
        if errs:
            errors_files += 1
        elif warns:
            warnings_files += 1
        else:
            clean_files += 1
        for x in findings:
            rule_counts[f"{x['severity']}:{x['rule']}"] += 1
        per_file_counts.append((f.name, len(errs), len(warns)))

        if not args.summary:
            status = "ERROR" if errs else ("WARN" if warns else "OK")
            print(f"{f.name}: {status} ({len(errs)} errors, {len(warns)} warnings)")
            shown = findings
            if args.max_warnings is not None and args.fmt == "text":
                shown = errs + warns[: args.max_warnings]
            if shown:
                _print_findings(f.name, shown, args.fmt)

    print(f"\n=== Summary ({args.schema} schema, semantic={'on' if semantic else 'off'}) ===")
    print(f"Total files:    {total}")
    print(f"Clean:          {clean_files}")
    print(f"Warnings only:  {warnings_files}")
    print(f"Errors:         {errors_files}")
    print()
    print("Top rule counts:")
    for rule, n in rule_counts.most_common(25):
        print(f"  {n:7d}  {rule}")

    sys.exit(1 if errors_files else 0)


if __name__ == "__main__":
    main()
