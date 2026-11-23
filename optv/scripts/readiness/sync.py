"""Carry the status the scorecards derive to the places that state it by hand.

A parliament's status, ``production`` or ``in development``, follows from its level. Three places repeat it: the table of parliaments in this repository's README, the status line of each data repository's README, and the ``OPTV Status`` row of the parliament's profile in the Research repository. This module finds where they disagree with the scorecards and, when asked, rewrites just that value.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from . import config as cfg
from .overview import load_scorecards

# What each place calls the two statuses.
README_CELL = {cfg.STATUS_PRODUCTION: "**production**", cfg.STATUS_DEVELOPMENT: "in development"}
RESEARCH_STATUS = {cfg.STATUS_PRODUCTION: "Operational", cfg.STATUS_DEVELOPMENT: "In development"}

# A row of the table of implemented parliaments in the repository README.
README_ROW = re.compile(r"^\| \[`([A-Z0-9-]+)`\]\(optv/parliaments/\1/\) \|.*\|[ \t]*$", re.M)
DATA_STATUS_LINE = re.compile(r"^(> \*\*Status:\*\* )(.+?)[ \t]*$", re.M)
RESEARCH_STATUS_ROW = re.compile(r"^(\| \*\*OPTV Status\*\* \| )(.+?)( \|)[ \t]*$", re.M)


@dataclass
class Finding:
    parliament: str
    place: str            # what a person would call the file
    path: Path
    found: str | None     # None: the file has no such line
    expected: str
    pattern: re.Pattern | None = None

    @property
    def fixable(self) -> bool:
        return self.found is not None and self.pattern is not None

    def describe(self) -> str:
        if self.found is None:
            return f"{self.parliament}: {self.place} has no status entry (expected {self.expected!r})"
        return f"{self.parliament}: {self.place} says {self.found!r}, the scorecard gives {self.expected!r}"


def _readme_row(code: str) -> re.Pattern:
    return re.compile(rf"^(\| \[`{re.escape(code)}`\]\(optv/parliaments/{re.escape(code)}/\) \|.*\| )([^|\n]+?)( \|)[ \t]*$",
                      re.M)


def sort_readme_table(text: str) -> str:
    """The README with its table of implemented parliaments in order: parliaments in production first, then the others, each group by code."""
    rows = list(README_ROW.finditer(text))
    if not rows:
        return text

    def key(row: re.Match) -> tuple:
        status = row.group(0).rstrip().rstrip("|").rsplit("|", 1)[-1].strip()
        return (0 if status == README_CELL[cfg.STATUS_PRODUCTION] else 1, row.group(1))

    ordered = "\n".join(row.group(0) for row in sorted(rows, key=key))
    return text[:rows[0].start()] + ordered + text[rows[-1].end():]


def _compare(code: str, place: str, path: Path, pattern: re.Pattern, expected: str) -> Finding | None:
    match = pattern.search(path.read_text())
    if match is None:
        return Finding(code, place, path, None, expected)
    if match.group(2) != expected:
        return Finding(code, place, path, match.group(2), expected, pattern)
    return None


def findings(tools: Path | None = None, data_root: Path | None = None) -> list[Finding]:
    """Every place whose status differs from the scorecards. Repositories that are not on this machine are passed over."""
    tools = tools or cfg.tools_root()
    data_root = data_root or cfg.data_root()
    result = []
    for card in load_scorecards(tools):
        code, status = card["parliament"], card["status"]
        places = [("the Tools README table", tools / "README.md", _readme_row(code), README_CELL[status]),
                  ("the data repository README", data_root / f"{cfg.DATA_REPO_PREFIX}{code}" / "README.md",
                   DATA_STATUS_LINE, status),
                  ("the Research profile",
                   data_root / "OpenParliamentTV-Research" / "parliaments" / code.split("-")[0] / f"{code}.md",
                   RESEARCH_STATUS_ROW, RESEARCH_STATUS[status])]
        for place, path, pattern, expected in places:
            if not path.is_file():
                continue
            finding = _compare(code, place, path, pattern, expected)
            if finding:
                result.append(finding)
    return result


def apply(found: list[Finding]) -> list[Finding]:
    """Rewrite the differing values. Returns what could not be fixed: a missing entry has no line to rewrite."""
    left = []
    for finding in found:
        if not finding.fixable:
            left.append(finding)
            continue
        text = finding.path.read_text()
        replacement = lambda match: match.group(1) + finding.expected + (match.group(3) if match.lastindex >= 3 else "")
        text = finding.pattern.sub(replacement, text, count=1)
        if finding.path.name == "README.md" and finding.place == "the Tools README table":
            # A parliament that changed its status changes its place in the table.
            text = sort_readme_table(text)
        finding.path.write_text(text)
    return left
