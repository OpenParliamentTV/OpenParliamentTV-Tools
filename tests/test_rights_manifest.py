"""Rights (creator/license) must come exclusively from the manifest.

Two guarantees:

1. Every parliament resolves non-empty ``creator`` + ``license`` from the
   manifest ``media`` block (and ``proceedings`` block for the parliaments that
   emit ``textContents``) via ``optv.parliaments.get_rights``.
2. No parser/merger/workflow assigns a creator/license *string literal* into the
   emitted JSON — they must all flow from ``get_rights`` (the regression this
   suite locks down: hardcoded rights drifting away from the manifest).
"""

import re
from pathlib import Path

import pytest

from optv.parliaments import get_rights, list_parliaments

_REPO = Path(__file__).resolve().parent.parent
_PARL_DIR = _REPO / "optv" / "parliaments"

# Parliaments that emit a ``textContents`` stream and therefore need proceedings
# rights in the manifest. DE-HH is video-only today (no textContents).
_TEXT_PARLIAMENTS = {
    "DE", "DE-RP", "DE-ST", "DE-BW", "DE-BY", "DE-NI", "DE-NW", "DE-SH",
    "DE-SN", "ES", "EU", "FI", "FR", "NO", "PT", "SE", "TW",
}


@pytest.mark.parametrize("parliament", list_parliaments())
def test_media_rights_resolve(parliament):
    rights = get_rights(parliament, stream="media")
    assert rights.get("creator"), f"{parliament}: manifest media.creator missing"
    assert rights.get("license"), f"{parliament}: manifest media.license missing"


@pytest.mark.parametrize("parliament", sorted(_TEXT_PARLIAMENTS))
def test_proceedings_rights_resolve(parliament):
    rights = get_rights(parliament, stream="proceedings")
    assert rights.get("creator"), f"{parliament}: manifest proceedings.creator missing"
    assert rights.get("license"), f"{parliament}: manifest proceedings.license missing"


# Matches a dict-style assignment of a non-empty string literal to creator/license,
# e.g.  "creator": "Landtag X",   or   'license': 'CC-BY'
_LITERAL_RE = re.compile(r"""["'](?:creator|license)["']\s*:\s*["'][^"']+["']""")


def _emitter_files():
    for sub in ("parsers", "merger"):
        yield from _PARL_DIR.glob(f"*/{sub}/*.py")
    yield from _PARL_DIR.glob("*/workflow.py")


def test_no_hardcoded_rights_literals():
    offenders = []
    for path in _emitter_files():
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if _LITERAL_RE.search(line):
                offenders.append(f"{path.relative_to(_REPO)}:{lineno}: {stripped}")
    assert not offenders, "Hardcoded creator/license literals (use get_rights):\n" + "\n".join(offenders)
