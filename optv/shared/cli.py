"""Command-line plumbing shared by the stage modules that can be run on their own (``python -m optv.parliaments.<ID>.<package>.<module>``)."""

from __future__ import annotations

import argparse
import importlib
import logging
from pathlib import Path
from typing import Callable

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s %(message)s"


def setup_logging(debug: bool = False) -> None:
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO, format=LOG_FORMAT)


def load_config(parliament_id: str, data_dir: Path):
    """The parliament's ``Config`` for ``data_dir``."""
    return importlib.import_module(f"optv.parliaments.{parliament_id}.common").Config(data_dir)


def session_cli(parliament_id: str, run: Callable, *, description: str, example: str) -> None:
    """``<session> <data_dir> [--debug]`` for a per-session stage function ``run(session, config, args)``."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("session", help=f"Session ID e.g. {example}")
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    setup_logging(args.debug)
    run(args.session, load_config(parliament_id, args.data_dir), args)
