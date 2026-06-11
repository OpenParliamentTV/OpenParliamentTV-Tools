"""Per-parliament detection config for the PDF→TEI core.

Each parliament differs in how speaker lines, government roles and agenda items
render in the PDF text layer. Those regexes are the only parliament-specific
knowledge; the rest of ``optv/shared/pdf2tei`` is generic. The config *bodies*
live in each parliament module's ``parsers/pdf_config.py`` (exporting a module
attribute ``CONFIG``); :func:`load_config` resolves one by parliament id.
"""
from __future__ import annotations

import importlib
import re
from dataclasses import dataclass, field


@dataclass
class ParliamentConfig:
    parliament_id: str
    # Each pattern is matched against a *block's* full cleaned text (one line).
    # Named groups: name, faction (mp); name, role (gov); role, name (chair).
    mp_speaker: re.Pattern
    gov_speaker: re.Pattern
    chair_speaker: re.Pattern
    # TOP / agenda announcement (chair text). None when the parliament has no
    # spoken announcement and agenda comes purely from the TOC.
    top_announce: re.Pattern | None = None
    # Factions that may appear, longest-first for matching (display labels).
    known_factions: list[str] = field(default_factory=list)
    # TOC sub-entries (vote results / doc refs) that are NOT agenda titles.
    toc_noise: re.Pattern | None = None
    # True when the speaker label is an inline PREFIX of the first speech block
    # ("Name (CSU): Sehr geehrte ...") rather than its own block ending ":".
    # Inline regexes then carry a (?P<rest>...) group = the speech after the label.
    speaker_inline: bool = False
    # TOC layout: "page-tail" (RP/SH: each entry ends with its own page number)
    # or "indented" (BY/NW: the page is on a following sub-entry/speaker-ref line).
    toc_layout: str = "page-tail"
    # Strip a leading TOP number ("1 Schulen schlagen Alarm …") before the title
    # checks (NW numbers every agenda item).
    toc_strip_leading_num: bool = False
    # Opening-bracket characters that mark an interjection/comment line. Most
    # parliaments use "(" only; Berlin (DE-BE) prints interjections in "[ … ]".
    incident_open: str = "("
    # Rede-merge rule for grouping per-<u> turns to the video-clip granularity
    # (see optv/shared/pdf2tei/merge.py). chain=False: one clip = chair + one
    # speaker (DE-BY). chain=True + merge_K: one clip = whole exchange incl.
    # bounded Zwischenfragen (DE-BW).
    merge_chain: bool = False
    merge_K: int = 2


def load_config(parliament_id: str) -> ParliamentConfig:
    """Import ``optv.parliaments.<id>.parsers.pdf_config`` and return its
    ``CONFIG`` (a :class:`ParliamentConfig`)."""
    mod = importlib.import_module(f"optv.parliaments.{parliament_id}.parsers.pdf_config")
    cfg = getattr(mod, "CONFIG", None)
    if not isinstance(cfg, ParliamentConfig):
        raise TypeError(
            f"optv.parliaments.{parliament_id}.parsers.pdf_config.CONFIG "
            f"is not a ParliamentConfig")
    return cfg
