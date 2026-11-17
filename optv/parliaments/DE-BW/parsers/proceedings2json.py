#! /usr/bin/env python3
"""Parse DE-BW Plenarprotokoll PDFs into proceedings text turns.

Thin wrapper over :mod:`optv.shared.pdf2tei.proceedings_parser`, binding the
DE-BW detection config. Each document's ``meta.sittingTimes`` carries the
sitting's start, end and breaks from the protocol
(:func:`optv.shared.pdf2tei.sitting_times.extract_sitting_times`); the merger
anchors the video parts to them for wall-clock speech times.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    sys.path.insert(0, str(module_dir.parent.parent.parent.parent))
    __package__ = "optv.parliaments.DE-BW.parsers"

from optv.shared.pdf2tei import proceedings_parser as _pp
from optv.shared.pdf2tei.sitting_times import extract_sitting_times
from .pdf_config import CONFIG


def _sitting_times_meta(pdf: Path) -> dict:
    times = extract_sitting_times(pdf)
    return {"sittingTimes": times} if times else {}


def parse_proceedings_for_session(config, session: str):
    return _pp.parse_proceedings_for_session(config, CONFIG, session,
                                             extra_meta=_sitting_times_meta)


def parse_proceedings_directory(config, args=None):
    return _pp.parse_proceedings_directory(config, CONFIG, args,
                                           extra_meta=_sitting_times_meta)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--session", help="single session id (else all PDFs)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(name)s %(message)s")
    from ..common import Config
    config = Config(args.data_dir)
    if args.session:
        parse_proceedings_for_session(config, args.session)
    else:
        parse_proceedings_directory(config, args)
