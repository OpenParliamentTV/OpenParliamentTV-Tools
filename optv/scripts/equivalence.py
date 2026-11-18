#! /usr/bin/env python3
"""Output-equivalence harness: prove a Tools change leaves pipeline output untouched.

``snapshot`` runs each parliament's real workflow entry point (parse, merge, publish) offline on a scratch copy of its data repo and records a digest of every file the run wrote. ``compare`` diffs two snapshots. Taking one snapshot with ``--tools`` pointing at the reference checkout and one at the changed checkout shows exactly which sessions a change affects.

    python -m optv.scripts.equivalence snapshot --tools <checkout> --data-root <dir> --out <snapshot_dir>
    python -m optv.scripts.equivalence compare <snapshot_dir_a> <snapshot_dir_b>
    python -m optv.scripts.equivalence status-dump --tools <checkout> --data-root <dir> --out <file.json>

``--data-root`` is the directory holding the ``OpenParliamentTV-Data-<ID>`` repos. They are only read: every run works on a copy under ``--scratch``.

What a snapshot covers: the per-parliament ``workflow.py`` wrapper, the shared argparser and runner, ``Config``, the parse hook, the merger and the publish step. What it cannot cover: scrapers, alignment, NER and the entity-dump builders (they need the network or external services). Network access is blocked in the workflow process; every blocked call is recorded, so a parse step that silently degrades without the network shows up in the snapshot instead of hiding a difference.

Volatile fields (``meta.processing.*``, ``meta.lastUpdate``) are blanked and the scratch path is tokenised before hashing; everything else, including key order and ``1`` vs ``1.0``, is significant. ``status-dump`` records what ``Config.sessions()`` / ``status()`` / ``file()`` return for the real data repos, which is the view the Conductor reads in-process.
"""

import argparse
import fnmatch
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_REPO_PREFIX = "OpenParliamentTV-Data-"
SESSION_SUFFIX = "-session.json"

# Derived or machine-local cache content; never an input to parse or merge.
CACHE_NOT_COPIED = {"merged", "aligned", "ner", "audio", "audio_session", "whisper",
                    ".stage-state.json", ".DS_Store"}
# Written by a run but machine-local or transient.
OUTPUTS_IGNORED = {".stage-state.json", "optv.lock", ".DS_Store"}

BLOCKED_MARKER = "EQUIVALENCE-NETWORK-BLOCKED"

# Runs a module as ``python -m`` would, with outgoing connections refused.
_WORKFLOW_CHILD = r'''
import runpy, socket, sys

marker = sys.argv[1]

def _blocked(*args, **kwargs):
    target = next((a for a in args if isinstance(a, (tuple, str))), "?")
    sys.stderr.write("%s %r\n" % (marker, target))
    raise OSError("network blocked by the equivalence harness")

socket.socket.connect = _blocked
socket.socket.connect_ex = _blocked
socket.getaddrinfo = _blocked
socket.create_connection = _blocked
module = sys.argv[2]
sys.argv = [module] + sys.argv[3:]
runpy.run_module(module, run_name="__main__", alter_sys=True)
'''

_STATUS_CHILD = r'''
import importlib, json, sys
from pathlib import Path
from optv.parliaments import load_manifest

pid, data_dir = sys.argv[1], Path(sys.argv[2])
config = importlib.import_module(f"optv.parliaments.{pid}.common").Config(data_dir)

def rel(path):
    return str(Path(path).relative_to(data_dir))

sessions = config.sessions()
json.dump({
    "sessions": sessions,
    "by_period": {str(p): config.sessions(prefix=str(p)) for p in load_manifest(pid).get("periods", [])},
    "session": {s: {"status": sorted(flag.name for flag in config.status(s)),
                    "media": rel(config.file(s, "media")),
                    "processed": rel(config.file(s, "processed"))}
                for s in sessions},
}, sys.stdout)
'''

_TIMESTAMP_RE = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?")


# ---- scratch copy ----

def _copy_tree(src: Path, dst: Path) -> None:
    """Copy ``src`` to ``dst``, as copy-on-write clones where the filesystem supports it."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    if sys.platform == "darwin":
        if subprocess.run(["cp", "-Rc", str(src), str(dst)], capture_output=True).returncode == 0:
            return
        shutil.rmtree(dst, ignore_errors=True)
    if src.is_dir():
        shutil.copytree(src, dst, symlinks=True)
    else:
        shutil.copy2(src, dst)


def _parse_output_patterns(data_dir: Path) -> list[str]:
    """File patterns under ``original/`` that the data repo ignores: the parse outputs."""
    ignore_file = data_dir / ".gitignore"
    if not ignore_file.exists():
        return []
    lines = (line.strip() for line in ignore_file.read_text().splitlines())
    return [line for line in lines if line.startswith("original/") and not line.endswith("/")]


def _prepare_scratch(data_dir: Path, scratch: Path, *, seed_processed: bool,
                     keep_parsed: bool, overlay: Path | None) -> None:
    for name in ("original", "metadata"):
        if (data_dir / name).exists():
            _copy_tree(data_dir / name, scratch / name)
    cache = data_dir / "cache"
    if cache.is_dir():
        for entry in cache.iterdir():
            if entry.name not in CACHE_NOT_COPIED:
                _copy_tree(entry, scratch / "cache" / entry.name)
    if seed_processed and (data_dir / "processed").exists():
        _copy_tree(data_dir / "processed", scratch / "processed")
    if not keep_parsed:
        for pattern in _parse_output_patterns(data_dir):
            for path in scratch.glob(pattern):
                if path.is_file():
                    path.unlink()
    if overlay is not None and overlay.is_dir():
        for path in overlay.rglob("*"):
            if path.is_file():
                target = scratch / path.relative_to(overlay)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, target)


def _listing(root: Path) -> dict[str, tuple[int, int]]:
    out = {}
    for path in root.rglob("*"):
        if path.is_file() and not path.is_symlink():
            stat = path.stat()
            out[str(path.relative_to(root))] = (stat.st_size, stat.st_mtime_ns)
    return out


# ---- digests ----

def _blank_volatile(doc):
    if isinstance(doc, dict) and isinstance(doc.get("meta"), dict):
        meta = doc["meta"]
        if isinstance(meta.get("processing"), dict):
            meta["processing"] = {key: "<T>" for key in meta["processing"]}
        if "lastUpdate" in meta:
            meta["lastUpdate"] = "<T>"
    return doc


def _digest(path: Path, scratch_tokens: list[str]) -> dict:
    """``{"sha", "fmt"}`` for one output file.

    ``sha`` covers the content with volatile fields blanked; ``fmt`` records the serialisation (``i2`` = ``json.dumps(indent=2, ensure_ascii=False)``), so a change in formatting alone is reported too.
    """
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {"sha": hashlib.sha256(raw).hexdigest(), "fmt": "binary"}
    for token in scratch_tokens:
        text = text.replace(token, "<SCRATCH>")
    if path.suffix == ".json":
        try:
            doc = json.loads(text)
        except ValueError:
            doc = None
        if doc is not None:
            canonical = json.dumps(doc, indent=2, ensure_ascii=False)
            if text == canonical:
                fmt = "i2"
            elif text == canonical + "\n":
                fmt = "i2+newline"
            else:
                fmt = "other:" + hashlib.sha256(_TIMESTAMP_RE.sub("<T>", text).encode()).hexdigest()[:12]
            blanked = json.dumps(_blank_volatile(doc), ensure_ascii=False)
            return {"sha": hashlib.sha256(blanked.encode()).hexdigest(), "fmt": fmt}
    return {"sha": hashlib.sha256(text.encode()).hexdigest(), "fmt": "text"}


# ---- snapshot ----

def _manifest_periods(tools: Path, parliament: str) -> list[int]:
    import yaml
    manifest = yaml.safe_load((tools / "optv" / "parliaments" / parliament / "manifest.yaml").read_text())
    return list(manifest.get("periods") or [])


def _child_env(tools: Path, hashseed: str) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(tools)
    env["PYTHONHASHSEED"] = hashseed
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def _snapshot_unit(unit: tuple[str, list[int] | None], args) -> dict:
    """Snapshot one parliament, for all its periods or (``--split-periods``) a single one."""
    parliament, unit_periods = unit
    name = parliament if unit_periods is None else f"{parliament}@{unit_periods[0]}"
    tools = args.tools
    data_dir = args.data_root / f"{DATA_REPO_PREFIX}{parliament}"
    scratch = args.scratch / name
    if scratch.exists():
        shutil.rmtree(scratch)
    scratch.mkdir(parents=True)
    overlay = args.overlay / parliament if args.overlay else None
    _prepare_scratch(data_dir, scratch, seed_processed=args.seed_processed,
                     keep_parsed=args.keep_parsed, overlay=overlay)
    before = _listing(scratch)

    periods = unit_periods or args.periods or _manifest_periods(tools, parliament)
    real_processed = data_dir / "processed"
    published = sorted(path.name[:-len(SESSION_SUFFIX)]
                       for path in real_processed.glob(f"*{SESSION_SUFFIX}")) if real_processed.is_dir() else []
    scope_args = []
    if args.scope == "published" and published:
        scope_args = ["--limit-session=^(?:" + "|".join(re.escape(s) for s in published) + ")$"]
    runs, blocked, log_chunks = [], set(), []
    for period in periods:
        argv = [sys.executable, "-c", _WORKFLOW_CHILD, BLOCKED_MARKER,
                f"optv.parliaments.{parliament}.workflow", str(scratch),
                f"--period={period}", "--merge-speeches", "--no-single-instance",
                "--no-validate", *scope_args, *args.workflow_args]
        started = time.monotonic()
        proc = subprocess.run(argv, cwd=tools, env=_child_env(tools, args.hashseed),
                              capture_output=True, text=True)
        log = proc.stderr + proc.stdout
        log_chunks.append(f"##### period {period} (exit {proc.returncode})\n{log}")
        for line in log.splitlines():
            if line.startswith(BLOCKED_MARKER):
                blocked.add(line[len(BLOCKED_MARKER):].strip())
        runs.append({
            "period": period,
            "returncode": proc.returncode,
            "seconds": round(time.monotonic() - started, 1),
            "warnings": sum(" WARNING " in line for line in log.splitlines()),
            "errors": sum(" ERROR " in line for line in log.splitlines()),
        })

    tokens = sorted({str(scratch), str(scratch.resolve())}, key=len, reverse=True)
    outputs = {}
    for rel, signature in sorted(_listing(scratch).items()):
        if before.get(rel) == signature or Path(rel).name in OUTPUTS_IGNORED:
            continue
        outputs[rel] = _digest(scratch / rel, tokens)

    result = {
        "parliament": parliament,
        "periods": periods,
        "scope": args.scope,
        "workflow_args": args.workflow_args,
        "seed_processed": args.seed_processed,
        "runs": runs,
        "blocked_network": sorted(blocked),
        "sessions_in_data_repo": len(published),
        "sessions_published": sum(fnmatch.fnmatch(rel, f"processed/*{SESSION_SUFFIX}") for rel in outputs),
        "outputs": outputs,
    }
    (args.out / f"{name}.json").write_text(json.dumps(result, indent=2, ensure_ascii=False))
    (args.out / f"{name}.log").write_text("\n".join(log_chunks))
    if not args.keep:
        shutil.rmtree(scratch)
    logger.info("%s: %d outputs, %d sessions published (%d in data repo), exit codes %s, %d blocked network calls",
                name, len(outputs), result["sessions_published"], result["sessions_in_data_repo"],
                [run["returncode"] for run in runs], len(blocked))
    return result


def _available_parliaments(tools: Path, data_root: Path) -> list[str]:
    parliaments_dir = tools / "optv" / "parliaments"
    return sorted(p.name for p in parliaments_dir.iterdir()
                  if (p / "manifest.yaml").is_file()
                  and (data_root / f"{DATA_REPO_PREFIX}{p.name}" / "original").is_dir())


def cmd_snapshot(args) -> int:
    args.out.mkdir(parents=True, exist_ok=True)
    cleanup_scratch = args.scratch is None
    if cleanup_scratch:
        args.scratch = Path(tempfile.mkdtemp(prefix="optv-equivalence-"))
    parliaments = args.parliaments or _available_parliaments(args.tools, args.data_root)
    units = []
    for parliament in parliaments:
        if args.split_periods:
            periods = args.periods or _manifest_periods(args.tools, parliament)
            units.extend((parliament, [period]) for period in periods)
        else:
            units.append((parliament, None))
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        results = list(pool.map(lambda unit: _snapshot_unit(unit, args), units))
    if cleanup_scratch and not args.keep:
        shutil.rmtree(args.scratch, ignore_errors=True)
    failed = sorted({r["parliament"] for r in results if any(run["returncode"] for run in r["runs"])})
    if failed:
        logger.error("Workflow exited non-zero for: %s", ", ".join(failed))
    return 1 if failed else 0


# ---- compare ----

def _load_snapshot(directory: Path) -> dict[str, dict]:
    return {path.stem: json.loads(path.read_text()) for path in sorted(directory.glob("*.json"))}


def cmd_compare(args) -> int:
    left, right = _load_snapshot(args.a), _load_snapshot(args.b)
    different = 0
    for parliament in sorted(set(left) | set(right)):
        if parliament not in left or parliament not in right:
            print(f"{parliament}: only in {'A' if parliament in left else 'B'}")
            different += 1
            continue
        a, b = left[parliament], right[parliament]
        findings = []
        for key in ("periods", "scope", "workflow_args", "seed_processed", "blocked_network"):
            if a.get(key) != b.get(key):
                findings.append(f"{key}: {a.get(key)!r} != {b.get(key)!r}")
        codes_a = [run["returncode"] for run in a["runs"]]
        codes_b = [run["returncode"] for run in b["runs"]]
        if codes_a != codes_b:
            findings.append(f"exit codes: {codes_a} != {codes_b}")
        out_a, out_b = a["outputs"], b["outputs"]
        for rel in sorted(set(out_a) - set(out_b)):
            findings.append(f"only in A: {rel}")
        for rel in sorted(set(out_b) - set(out_a)):
            findings.append(f"only in B: {rel}")
        for rel in sorted(set(out_a) & set(out_b)):
            if out_a[rel]["sha"] != out_b[rel]["sha"]:
                findings.append(f"content differs: {rel}")
            elif out_a[rel]["fmt"] != out_b[rel]["fmt"]:
                findings.append(f"formatting differs: {rel}")
        if findings:
            different += 1
            print(f"{parliament}: {len(findings)} difference(s)")
            for finding in findings[:args.max_lines]:
                print(f"    {finding}")
            if len(findings) > args.max_lines:
                print(f"    ... {len(findings) - args.max_lines} more")
        else:
            print(f"{parliament}: identical ({len(out_a)} outputs)")
    print(f"\n{different} parliament(s) differ" if different else "\nAll snapshots identical")
    return 1 if different else 0


# ---- status-dump ----

def cmd_status_dump(args) -> int:
    parliaments = args.parliaments or _available_parliaments(args.tools, args.data_root)
    dump = {}
    for parliament in parliaments:
        data_dir = args.data_root / f"{DATA_REPO_PREFIX}{parliament}"
        proc = subprocess.run([sys.executable, "-c", _STATUS_CHILD, parliament, str(data_dir)],
                              cwd=args.tools, env=_child_env(args.tools, "0"),
                              capture_output=True, text=True)
        if proc.returncode:
            logger.error("%s: status dump failed\n%s", parliament, proc.stderr)
            dump[parliament] = {"error": proc.stderr.strip().splitlines()[-1:]}
            continue
        dump[parliament] = json.loads(proc.stdout)
        logger.info("%s: %d sessions", parliament, len(dump[parliament]["sessions"]))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(dump, indent=2, ensure_ascii=False, sort_keys=True))
    return 1 if any("error" in entry for entry in dump.values()) else 0


# ---- CLI ----

def _csv(value: str) -> list[str]:
    return [item for item in value.split(",") if item]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--debug", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_source_arguments(p):
        p.add_argument("--tools", type=Path, required=True,
                       help="Tools checkout whose code is run")
        p.add_argument("--data-root", type=Path, required=True,
                       help=f"Directory holding the {DATA_REPO_PREFIX}<ID> repos (read-only)")
        p.add_argument("--parliaments", type=_csv, default=None,
                       help="Comma-separated parliament IDs (default: all with a data repo)")

    snap = sub.add_parser("snapshot", help="Run parse+merge+publish offline and record output digests")
    add_source_arguments(snap)
    snap.add_argument("--out", type=Path, required=True, help="Snapshot directory to write")
    snap.add_argument("--periods", type=lambda v: [int(p) for p in _csv(v)], default=None,
                      help="Comma-separated periods (default: the manifest's periods)")
    snap.add_argument("--split-periods", action="store_true",
                      help="Run each period on its own working copy, in parallel (snapshot files are named <ID>@<period>)")
    snap.add_argument("--scope", choices=("published", "all"), default="published",
                      help="published: only the sessions in the data repo's processed/ (via --limit-session); "
                           "all: every session the originals yield")
    snap.add_argument("--scratch", type=Path, default=None,
                      help="Where the working copies go (default: a temporary directory)")
    snap.add_argument("--overlay", type=Path, default=None,
                      help="Directory with <ID>/<path> files copied over each working copy before the run")
    snap.add_argument("--seed-processed", action="store_true",
                      help="Start from the data repo's processed/ so the publish guard and carry-forward run")
    snap.add_argument("--keep-parsed", action="store_true",
                      help="Keep existing parse outputs instead of regenerating them")
    snap.add_argument("--workflow-args", type=lambda v: v.split(), default=[],
                      help="Extra workflow flags as one string, attached with '=': --workflow-args='--link-entities --rebuild'")
    snap.add_argument("--hashseed", default="0", help="PYTHONHASHSEED for the workflow process")
    snap.add_argument("--jobs", type=int, default=4, help="Parliaments processed in parallel")
    snap.add_argument("--keep", action="store_true", help="Keep the working copies for inspection")
    snap.set_defaults(func=cmd_snapshot)

    comp = sub.add_parser("compare", help="Diff two snapshot directories")
    comp.add_argument("a", type=Path)
    comp.add_argument("b", type=Path)
    comp.add_argument("--max-lines", type=int, default=20, help="Differences listed per parliament")
    comp.set_defaults(func=cmd_compare)

    status = sub.add_parser("status-dump", help="Record Config.sessions()/status()/file() for the real data repos")
    add_source_arguments(status)
    status.add_argument("--out", type=Path, required=True, help="JSON file to write")
    status.set_defaults(func=cmd_status_dump)

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    for name in ("tools", "data_root", "out", "scratch", "overlay", "a", "b"):
        if getattr(args, name, None) is not None:
            setattr(args, name, getattr(args, name).resolve())
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
