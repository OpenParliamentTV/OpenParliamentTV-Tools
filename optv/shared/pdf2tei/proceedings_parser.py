"""Shared proceedings parser for the PDF tier.

Each parliament's ``parsers/proceedings2json.py`` binds its
:class:`~optv.shared.pdf2tei.config.ParliamentConfig` and calls these. For every
``original/proceedings/{sid}.pdf`` it writes ``{sid}-proceedings.json`` (spine-
granularity text turns) which the merger joins onto the video spine.
"""
from __future__ import annotations

import logging
from pathlib import Path

from .pipeline import pdf_to_proceedings_doc

logger = logging.getLogger(__name__)


def parse_proceedings_for_session(config, cfg, session: str) -> Path | None:
    pdf = config.dir('proceedings') / f"{session}.pdf"
    if not pdf.exists():
        logger.info(f"[{session}] no PDF at {pdf} — skipping proceedings parse")
        return None
    doc = pdf_to_proceedings_doc(pdf, session, cfg, session=session)
    out = config.save_data(doc, session, 'proceedings')
    logger.info(f"[{session}] wrote {out.name} ({len(doc['data'])} turns)")
    return out


def parse_proceedings_directory(config, cfg, args=None) -> None:
    proc_dir = config.dir('proceedings')
    pdfs = sorted(proc_dir.glob("*.pdf")) if proc_dir.is_dir() else []
    if not pdfs:
        logger.info("No Plenarprotokoll PDFs to parse.")
        return
    for pdf in pdfs:
        parse_proceedings_for_session(config, cfg, pdf.stem)
