"""Extract cleaned reading-order blocks from a PDF.

Thin dispatch over the extraction backends (PyMuPDF only). Callers in the German
PDF tier pass :func:`optv.shared.lang.de.is_running_header` as ``drop_line`` to
strip page headers/footers.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional


def get_backend(name: str = "pymupdf"):
    if name == "pymupdf":
        from .backends import pymupdf_backend as b
        return b
    raise ValueError(f"unknown PDF backend {name!r} (only 'pymupdf' is shipped)")


def extract_blocks(pdf_path: Path, *, backend: str = "pymupdf",
                   drop_line: Optional[Callable[[str], bool]] = None) -> list[dict]:
    """Return the reading-order block list for ``pdf_path``."""
    return get_backend(backend).extract(Path(pdf_path), drop_line=drop_line)
