"""Which parliament to add next: the most feasible one that has no implementation yet.

The Research repository scores every parliament it has surveyed and writes them, most feasible first, to its ``overview.json``. This module reads that list where the repository is on the machine and leaves out the parliaments that are implemented here. It only reads and prints: nothing of the survey is written into this repository.
"""

from __future__ import annotations

import json
from pathlib import Path

from . import config as cfg

RESEARCH_REPO = "OpenParliamentTV-Research"
# The status the Research repository gives a parliament that was looked into and set aside.
DEFERRED = "Research"


class CandidatesError(Exception):
    pass


def implemented(tools: Path) -> set[str]:
    directory = tools / "optv" / "parliaments"
    return {path.name for path in directory.iterdir() if (path / "manifest.yaml").is_file()}


def _newer_profiles(research: Path, overview: Path) -> int:
    """Profiles edited after the overview was generated: its order may no longer be theirs."""
    generated = overview.stat().st_mtime
    return sum(1 for path in (research / "parliaments").rglob("*.md")
               if not path.name.startswith("_") and path.stat().st_mtime > generated)


def candidates(tools: Path | None = None, data_root: Path | None = None, *,
               include_deferred: bool = False) -> dict:
    """The surveyed parliaments without an implementation, in the Research repository's order of feasibility."""
    tools = tools or cfg.tools_root()
    research = (data_root or cfg.data_root()) / RESEARCH_REPO
    overview = research / "overview.json"
    if not overview.is_file():
        raise CandidatesError(f"no Research overview at {overview}; the choice of the next parliament "
                              "rests on the Research repository's survey")
    document = json.loads(overview.read_text())
    have = implemented(tools)
    listed, deferred = [], []
    for rank, entry in enumerate(document.get("parliaments") or [], start=1):
        code = entry.get("shortcode")
        if not code or code in have:
            continue
        feasibility = entry.get("feasibility") or {}
        if feasibility.get("total") is None:
            continue
        candidate = {
            "rank": rank,
            "code": code,
            "name": entry.get("parliament", ""),
            "type": (entry.get("type") or "").lower(),
            "score": feasibility["total"],
            "max": feasibility.get("max"),
            "tiers": {name: f"{tier.get('score')}/{tier.get('max')}"
                      for name, tier in (feasibility.get("tiers") or {}).items()},
            "verified": entry.get("verified"),
            "status": entry.get("optv_status"),
            "profile": str(research / entry["profile_path"]) if entry.get("profile_path") else None,
        }
        if candidate["status"] == DEFERRED and not include_deferred:
            deferred.append(code)
        else:
            listed.append(candidate)
    return {"generated": document.get("generated"), "newer_profiles": _newer_profiles(research, overview),
            "candidates": listed, "deferred": deferred}


def _line(candidate: dict) -> str:
    tiers = ", ".join(f"{name} {value}" for name, value in candidate["tiers"].items())
    return (f"{candidate['code']:<6} {candidate['score']}/{candidate['max']} ({tiers})  "
            f"{candidate['name']}; {candidate['type']}; profile verified {candidate['verified'] or 'never'}")


def render(result: dict, limit: int = 5) -> str:
    listed = result["candidates"]
    if not listed:
        return "Every surveyed parliament has an implementation."
    lines = [f"Next:  {_line(listed[0])}"]
    lines += [f"then:  {_line(candidate)}" for candidate in listed[1:limit]]
    if len(listed) > limit:
        lines.append(f"       and {len(listed) - limit} more")
    if result["deferred"]:
        lines.append(f"Left out, looked into and set aside: {', '.join(result['deferred'])}")
    if result["newer_profiles"]:
        lines.append(f"The order is from the overview generated {result['generated']}; {result['newer_profiles']} "
                     "profile(s) changed since. Regenerate it in the Research repository "
                     "(python3 scripts/update_overview.py) before relying on it.")
    return "\n".join(lines)
