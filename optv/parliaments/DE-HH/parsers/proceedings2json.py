#! /usr/bin/env python3
"""Parse DE-HH Plenarprotokoll PDFs into proceedings text turns.

Thin wrapper over :mod:`optv.shared.pdf2tei.proceedings_parser`, binding the
DE-HH detection config.
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
    __package__ = "optv.parliaments.DE-HH.parsers"

from optv.shared.pdf2tei import proceedings_parser as _pp
from .pdf_config import CONFIG


def parse_proceedings_for_session(config, session: str):
    return _pp.parse_proceedings_for_session(config, CONFIG, session)


def parse_proceedings_directory(config, args=None):
    return _pp.parse_proceedings_directory(config, CONFIG, args)


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
