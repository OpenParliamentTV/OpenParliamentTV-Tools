#! /usr/bin/env python3

# Fetch Bundestag proceedings in ParlaMint-DE_beta format.
#
# Used for periods 16-17 where Bundestag does not publish machine-readable
# proceedings in its native TEI format. Source corpus is published at
# https://github.com/PolMine/ParlaMint-DE_beta under CC-BY 4.0.

from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import argparse
import json
from pathlib import Path
import re
import sys
import urllib.request

POLMINE_REPO = "PolMine/ParlaMint-DE_beta"
POLMINE_BRANCH = "main"
TREE_API = f"https://api.github.com/repos/{POLMINE_REPO}/git/trees/{POLMINE_BRANCH}?recursive=1"
RAW_URL = f"https://raw.githubusercontent.com/{POLMINE_REPO}/{POLMINE_BRANCH}"

REGISTRY_FILES = ("ParlaMint-DE-listPerson.xml", "ParlaMint-DE-listOrg.xml")
SYNC_FILE = ".parlamint-sync.json"

# ParlaMint session filename: ParlaMint-DE_YYYY-MM-DD-PP-NNN.xml
SESSION_RE = re.compile(r'^.*ParlaMint-DE_\d{4}-\d{2}-\d{2}-(\d{2})-(\d{3})\.xml$')


def _fetch_tree() -> dict:
    logger.debug(f"Fetching tree from {TREE_API}")
    req = urllib.request.Request(
        TREE_API,
        headers={"Accept": "application/vnd.github+json", "User-Agent": "optv-parlamint-fetch"},
    )
    with urllib.request.urlopen(req) as f:
        return json.loads(f.read())


def _read_sync(proceedings_dir: Path) -> dict:
    sync_path = proceedings_dir / SYNC_FILE
    if sync_path.exists():
        return json.loads(sync_path.read_text())
    return {"tree_sha": None, "blobs": {}}


def _write_sync(proceedings_dir: Path, sync: dict) -> None:
    (proceedings_dir / SYNC_FILE).write_text(json.dumps(sync, indent=2))


def _download_to(url: str, dest: Path) -> None:
    """Download `url` into `dest`, prepending a <?source url="…"?> PI."""
    logger.info(f"downloading {url}")
    pi = f"""<?source url="{url}"?>\n""".encode("utf-8")
    with urllib.request.urlopen(url) as f:
        with open(dest, "wb") as out:
            first_line = f.readline()
            if b"<?xml" in first_line:
                out.write(first_line)
                out.write(pi)
            else:
                out.write(pi)
                out.write(first_line)
            out.write(f.read())


def list_session_files(period: int, tree: dict | None = None) -> list[dict]:
    """List ParlaMint session XML files for the given electoral period.

    Returns a list of dicts: {path, sha, size, sessionid}.
    """
    if tree is None:
        tree = _fetch_tree()
    out = []
    for entry in tree["tree"]:
        if entry["type"] != "blob":
            continue
        m = SESSION_RE.match(entry["path"])
        if not m:
            continue
        p, n = int(m.group(1)), int(m.group(2))
        if p != period:
            continue
        out.append({
            "path": entry["path"],
            "sha": entry["sha"],
            "size": entry["size"],
            "sessionid": p * 1000 + n,
        })
    return out


def download_parlamint_registries(proceedings_dir: Path, force: bool = False, tree: dict | None = None) -> list[Path]:
    """Download listPerson and listOrg into proceedings_dir.

    Re-downloads when the recorded blob SHA changes (or when `force`).
    Returns the list of files actually written.
    """
    proceedings_dir = Path(proceedings_dir)
    proceedings_dir.mkdir(parents=True, exist_ok=True)
    if tree is None:
        tree = _fetch_tree()
    blobs = {e["path"]: e["sha"] for e in tree["tree"] if e["type"] == "blob"}
    sync = _read_sync(proceedings_dir)
    written = []
    for name in REGISTRY_FILES:
        sha = blobs.get(name)
        if sha is None:
            logger.error(f"Registry {name} not found in tree")
            continue
        dest = proceedings_dir / name
        if not force and dest.exists() and sync["blobs"].get(name) == sha:
            logger.debug(f"Registry {name} up to date")
            continue
        _download_to(f"{RAW_URL}/{name}", dest)
        sync["blobs"][name] = sha
        written.append(dest)
    sync["tree_sha"] = tree["sha"]
    _write_sync(proceedings_dir, sync)
    return written


def download_parlamint_period(period: int, proceedings_dir: Path, force: bool = False) -> list[Path]:
    """Download all ParlaMint session XML files for `period` into `proceedings_dir`.

    Files are stored as `<sessionid>-data.xml` so they don't collide with
    Bundestag-native `<sessionid>-proceedings.xml`. Re-downloads only files
    whose blob SHA changed since the previous sync (or when `force`).
    """
    proceedings_dir = Path(proceedings_dir)
    proceedings_dir.mkdir(parents=True, exist_ok=True)
    tree = _fetch_tree()
    sessions = list_session_files(period, tree)
    if not sessions:
        logger.warning(f"No ParlaMint session files found for period {period}")
        return []

    # Refresh registries from the same tree fetch.
    download_parlamint_registries(proceedings_dir, force=force, tree=tree)

    sync = _read_sync(proceedings_dir)
    written = []
    for s in sessions:
        dest = proceedings_dir / f"{s['sessionid']}-data.xml"
        if not force and dest.exists() and sync["blobs"].get(s["path"]) == s["sha"]:
            logger.debug(f"Session {s['sessionid']} up to date (sha {s['sha'][:8]})")
            continue
        _download_to(f"{RAW_URL}/{s['path']}", dest)
        sync["blobs"][s["path"]] = s["sha"]
        written.append(dest)
    sync["tree_sha"] = tree["sha"]
    _write_sync(proceedings_dir, sync)
    logger.info(f"Period {period}: {len(written)} new/changed of {len(sessions)} session files")
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch ParlaMint-DE_beta proceedings.")
    parser.add_argument("output_dir", type=str, nargs="?", help="Proceedings output directory")
    parser.add_argument("--period", type=int, required=True, help="Electoral period (e.g. 16, 17)")
    parser.add_argument("--force", action="store_true", default=False,
                        help="Re-download even if SHA matches the sync record")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    if args.output_dir is None:
        parser.print_help()
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    download_parlamint_period(args.period, Path(args.output_dir), force=args.force)
