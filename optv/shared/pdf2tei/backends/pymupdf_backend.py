"""PyMuPDF extraction backend.

Emits paragraph-ish blocks in reading order. Handles 1- and 2-column layouts by
detecting a column split from the block x-coordinate distribution and sorting
(column, y) — the cheap reading-order fix PyMuPDF needs.

The optional ``drop_line`` predicate filters running headers/footers (and empty
blocks). It is injected so the backend stays language-agnostic; the German tier
passes :func:`optv.shared.lang.de.is_running_header`.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional

import fitz


def _column_split(page) -> float | None:
    """Return an x threshold separating left/right columns, or None if single
    column. Heuristic: if a clear cluster of block x0 sits right of page center
    AND a cluster sits left, it's two-column."""
    w = page.rect.width
    xs = [b[0] for b in page.get_text("blocks") if b[4].strip()]
    if not xs:
        return None
    center = w / 2
    left = [x for x in xs if x < center]
    right = [x for x in xs if x >= center]
    # Two-column only if both sides carry a real share of blocks.
    if right and left and len(right) >= 0.25 * len(xs):
        return center
    return None


def extract(pdf_path: Path, drop_line: Optional[Callable[[str], bool]] = None) -> list[dict]:
    from .. common import clean_block_text
    drop = drop_line or (lambda _t: False)
    doc = fitz.open(str(pdf_path))
    out: list[dict] = []
    for pno in range(doc.page_count):
        page = doc[pno]
        split = _column_split(page)
        blocks = [b for b in page.get_text("blocks") if b[4].strip()]
        if split is None:
            ordered = sorted(blocks, key=lambda b: (round(b[1] / 3), b[0]))
        else:
            # Left column (x0 < split) fully, then right column, each top->bottom.
            ordered = sorted(blocks, key=lambda b: (0 if b[0] < split else 1, b[1]))
        for b in ordered:
            text = clean_block_text(b[4])
            if not text or drop(text):
                continue
            out.append({"text": text, "x0": round(b[0], 1),
                        "y0": round(b[1], 1), "page": pno})
    return out
