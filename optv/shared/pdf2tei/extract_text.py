"""Extract cleaned reading-order blocks from a PDF.

Thin dispatch over the extraction backends (PyMuPDF only). Callers in the German
PDF tier pass :func:`optv.shared.lang.de.is_running_header` as ``drop_line`` to
strip page headers/footers.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Optional

from .common import write_blocks


def get_backend(name: str = "pymupdf"):
    if name == "pymupdf":
        from .backends import pymupdf_backend as b
        return b
    raise ValueError(f"unknown PDF backend {name!r} (only 'pymupdf' is shipped)")


def extract_blocks(pdf_path: Path, *, backend: str = "pymupdf",
                   drop_line: Optional[Callable[[str], bool]] = None) -> list[dict]:
    """Return the reading-order block list for ``pdf_path``."""
    return get_backend(backend).extract(Path(pdf_path), drop_line=drop_line)


def extract_to_file(pdf_path: Path, out_path: Path, *, backend: str = "pymupdf",
                    drop_line: Optional[Callable[[str], bool]] = None,
                    force: bool = False) -> list[dict]:
    """Extract blocks and cache them as JSON, skipping when the cache is newer
    than the PDF (unless ``force``)."""
    pdf_path, out_path = Path(pdf_path), Path(out_path)
    if (out_path.exists() and not force
            and out_path.stat().st_mtime >= pdf_path.stat().st_mtime):
        return json.loads(out_path.read_text())
    blocks = extract_blocks(pdf_path, backend=backend, drop_line=drop_line)
    write_blocks(out_path, blocks)
    return blocks
