"""End-to-end PDF → proceedings-turns pipeline for the German PDF tier.

One call wires the generic stages: extract reading-order blocks → build TEI →
read TEI into per-utterance turns → group into spine-granularity redes. Each
parliament's ``parsers/proceedings2json.py`` is a thin wrapper that supplies its
:class:`~optv.shared.pdf2tei.config.ParliamentConfig`.
"""
from __future__ import annotations

import datetime
from pathlib import Path
from typing import Callable, Optional

from .extract_text import extract_blocks
from .pdf2tei import build_tei, build_registries
from .tei2json import tei_to_turns
from .merge import merge_turns
from ..lang.de import is_running_header


def pdf_to_turns(pdf_path: Path, sid: str, cfg, *, agenda_mode: str = "toc",
                 sentencize: Optional[Callable[[str], list[str]]] = None) -> list[dict]:
    """Parse one PDF protocol into spine-granularity proceedings turns."""
    pdf_path = Path(pdf_path)
    blocks = extract_blocks(pdf_path, drop_line=is_running_header)
    root, persons, factions, has_gov, *_ = build_tei(
        sid, cfg, blocks, agenda_mode=agenda_mode, pdf_path=pdf_path)
    person_root, org_root = build_registries(persons, factions, has_gov)
    turns = tei_to_turns(root, person_root, org_root, sentencize=sentencize)
    return merge_turns(turns, chain=cfg.merge_chain, K=cfg.merge_K)


def pdf_to_proceedings_doc(pdf_path: Path, sid: str, cfg, *,
                           session: Optional[str] = None, agenda_mode: str = "toc",
                           sentencize: Optional[Callable[[str], list[str]]] = None) -> dict:
    """Return a ``{"meta": …, "data": [turns]}`` proceedings document."""
    turns = pdf_to_turns(pdf_path, sid, cfg, agenda_mode=agenda_mode, sentencize=sentencize)
    return {
        "meta": {
            "session": session or sid,
            "parliament": cfg.parliament_id,
            "processing": {
                "parse_proceedings":
                    datetime.datetime.utcnow().isoformat(timespec="seconds"),
            },
        },
        "data": turns,
    }
