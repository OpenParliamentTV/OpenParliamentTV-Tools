"""Block types + text cleanup for the PDF→TEI pipeline.

A *block* is one paragraph-ish unit in reading order::

    {"text": str, "x0": float, "y0": float, "page": int}

The extraction backend emits a list of blocks in reading order; ``pdf2tei.py``
consumes that list and is backend-agnostic. Keeping x0/y0 lets the parser use
indentation (e.g. SH renders interjections indented) and lets us audit reading
order.

These helpers are language-agnostic; the German running-header predicate lives
in :mod:`optv.shared.lang.de` (``is_running_header``).
"""
from __future__ import annotations

import json
import re
from pathlib import Path


# Soft hyphen (U+00AD) and non-breaking space (U+00A0) appear all over German
# Plenarprotokoll text layers (and PDF text layers generally).
SOFT_HYPHEN = "­"
NBSP = " "


def dehyphenate(text: str) -> str:
    """Join words split across line breaks.

    - Soft hyphen at a line break is *always* a wrap artifact (RP uses these).
    - A hard '-' at end of a physical line followed by a lowercase letter is a
      wrap too (SH). We deliberately do NOT join when the next char is
      uppercase or a dash (compound names / en-dash lists stay intact).
    """
    # Soft hyphen wrap: "wort­\nrest" / "wort­ rest" -> "wortrest"
    text = re.sub(rf"{SOFT_HYPHEN}\s*\n?\s*", "", text)
    # Hard hyphen wrap across a newline: "Zwischenfra-\nge" -> "Zwischenfrage"
    text = re.sub(r"(?<=[a-zäöüß])-\s*\n\s*(?=[a-zäöüß])", "", text)
    return text


def normalize_ws(text: str) -> str:
    text = text.replace(NBSP, " ").replace(SOFT_HYPHEN, "")
    text = text.replace("\n", " ")
    return re.sub(r"\s+", " ", text).strip()


def clean_block_text(raw: str) -> str:
    """Dehyphenate, then flatten to a single normalized line of text."""
    return normalize_ws(dehyphenate(raw))


def write_blocks(path: Path, blocks: list[dict]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(blocks, f, ensure_ascii=False, indent=1)


def read_blocks(path: Path) -> list[dict]:
    with open(path) as f:
        return json.load(f)
