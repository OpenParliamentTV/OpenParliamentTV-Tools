"""Shared proceedings parser for the PDF tier.

For every ``original/proceedings/{sid}.pdf`` it writes ``{sid}-proceedings.json`` (spine-granularity text turns), which the merger joins onto the video spine. The parliament-specific knowledge is its :class:`~optv.shared.pdf2tei.config.ParliamentConfig` in ``parsers/pdf_config.py``::

    python -m optv.shared.pdf2tei.proceedings_parser DE-BY <data_dir> [--session 19054]
"""
from __future__ import annotations

import argparse
import importlib
import logging
from pathlib import Path

from .config import load_config
from .pipeline import pdf_to_proceedings_doc
from .sitting_times import extract_sitting_times

logger = logging.getLogger(__name__)


def parse_proceedings_for_session(config, cfg, session: str) -> Path | None:
    pdf = config.dir('proceedings') / f"{session}.pdf"
    if not pdf.exists():
        logger.info(f"[{session}] no PDF at {pdf} — skipping proceedings parse")
        return None
    doc = pdf_to_proceedings_doc(pdf, session, cfg, session=session)
    if cfg.sitting_times:
        times = extract_sitting_times(pdf)
        if times:
            doc["meta"]["sittingTimes"] = times
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse a parliament's Plenarprotokoll PDFs into proceedings text turns.")
    parser.add_argument("parliament", help="Parliament ID with a parsers/pdf_config.py, e.g. DE-BY")
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", help="single session id (else all PDFs)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s %(message)s")
    config = importlib.import_module(f"optv.parliaments.{args.parliament}.common").Config(args.data_dir)
    cfg = load_config(args.parliament)
    if args.session:
        parse_proceedings_for_session(config, cfg, args.session)
    else:
        parse_proceedings_directory(config, cfg, args)


if __name__ == "__main__":
    main()
