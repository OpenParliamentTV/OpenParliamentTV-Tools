"""Parse the front-matter table of contents of a DE Landtag Plenarprotokoll.

The TOC lists the *canonical* agenda-item titles each followed by dot-leaders
and the printed page where it starts. Sub-entries (speaker references,
"Drucksache ...", "Antrag ...", vote results) are interleaved and dropped —
what remains are the real TOPs.

Parsing is **line-level**: PyMuPDF bundles several TOC lines into one block, so
we split each block into physical lines and accumulate wrapped lines into one
logical entry, terminating at the line that ends with a page number. This keeps
multi-item blocks from bleeding titles together (the DE-RP failure mode) and
handles DE-SH's two-column TOC, where each title is its own wrapped entry.

Products:
  * parse_toc(pdf_path, cfg) -> ([(title, start_page), ...], page_offset)
  * page_offset(pdf): printed_page - pdf_page_index, from footer (RP: standalone
    block, +1) or header (SH: leading 4-digit in the running header).
  * title_for_printed_page(toc, printed_page): latest TOP whose start <= page.

The German-language heuristics (header lines, speaker references, vote results)
live in :mod:`optv.shared.lang.de`; the algorithm here is parliament-agnostic.
"""
from __future__ import annotations

import re
import statistics
from pathlib import Path

import fitz

from .common import clean_block_text
from ..lang.de import (
    TOC_HEADER_LINE as _HEADER_LINE,
    TOC_SPEAKER_REF as _SPEAKER_REF,
    TOC_RESULT_LINE as _RESULT_LINE,
)

# Trailing dot-leaders + page number(s): "... . . . 233", "... 8734, 8753" or
# "... 8937," (SH lists the first page then a trailing comma for further pages).
_PAGE_TAIL = re.compile(r"[.\s·]*\b(\d{1,4})(?:\s*,\s*\d{1,4})*[,\s]*$")


def _strip_page_tail(text: str) -> tuple[str, int | None]:
    m = _PAGE_TAIL.search(text)
    if not m:
        return text.strip(), None
    return text[:m.start()].strip(" .·\t"), int(m.group(1))


def _is_agenda_title(title: str, cfg) -> bool:
    if len(title) < 12:
        return False
    # Agenda titles start with a capital (or opening quote); cross-block wrap
    # fragments ("gung sichern ...", "steinischen Finanzbehörden ...") start
    # lowercase — drop them.
    if not (title[:1].isupper() or title[:1] in "„\"»"):
        return False
    if cfg.toc_noise and cfg.toc_noise.match(title):
        return False
    if _HEADER_LINE.search(title) or _RESULT_LINE.match(title) or _SPEAKER_REF.search(title):
        return False
    if cfg.mp_speaker.match(title + ":") or cfg.gov_speaker.match(title + ":"):
        return False
    return True


def _body_start_page(pdf, cfg) -> int:
    """First pdf page index that carries a real speaker label.

    Uses the speaker regexes directly (they already require the ':'), so it works
    for inline-label parliaments (DE-BY: 'Präsidentin Ilse Aigner: Liebe …')
    where the block does NOT end with ':'."""
    for pno in range(min(pdf.page_count, 25)):
        for b in pdf[pno].get_text("blocks"):
            t = re.sub(r"\s+", " ", b[4]).strip()
            if (cfg.chair_speaker.match(t) or cfg.mp_speaker.match(t)
                    or cfg.gov_speaker.match(t)):
                return pno
    return 8


def page_offset(pdf) -> int:
    """printed_page - pdf_page_index.

    RP prints the page as a standalone footer block ("6"). SH prints it as a
    leading 4-digit number in the running header ("8936 Schleswig-Holsteinischer
    Landtag ..."). We only trust a *leading* header number (SH headers also end
    in the year, which must not be mistaken for a page).
    """
    diffs = []
    for pno in range(2, min(pdf.page_count, 40)):
        for b in pdf[pno].get_text("blocks"):
            t = re.sub(r"\s+", " ", b[4]).strip()
            if re.fullmatch(r"\d{2,5}", t):                      # RP standalone footer
                diffs.append(int(t) - pno)
            elif "Landtag" in t or "Sitzung" in t:               # SH running header
                m = re.match(r"^(\d{3,5})\b", t)
                if m:
                    diffs.append(int(m.group(1)) - pno)
    return statistics.mode(diffs) if diffs else 1


def _join_wrapped(prev: str, line: str) -> str:
    """Append a wrapped physical line, fusing a hyphenated line break
    ('Landtagspräsiden-' + 'tin' -> 'Landtagspräsidentin') without a space."""
    if not prev:
        return line
    if prev.endswith("­") or (prev.endswith("-") and line[:1].islower()):
        return prev[:-1] + line
    return prev + " " + line


def _iter_entries(pdf, body_start: int, in_range):
    """Yield (raw_title, page) candidates, accumulating wrapped physical lines
    within each block and terminating only at a *plausible* page number.

    Terminating only on an in-range page is what stops a year inside a title
    ("Bericht 2023–2024") or a Drucksache number ("Drucksache 20/4428") from
    prematurely splitting the entry."""
    for pno in range(0, body_start):
        for b in pdf[pno].get_text("blocks"):
            joined = ""
            for line in b[4].splitlines():
                line = line.strip()
                if not line:
                    continue
                joined = _join_wrapped(joined, line)
                title, page = _strip_page_tail(joined)
                if page is not None and in_range(page):
                    yield title, page
                    joined = ""


_NUMBERED_TOP = re.compile(r"^\d+\s+[A-ZÄÖÜ„\"»]")


def parse_toc_indented(pdf_path, cfg) -> tuple[list[tuple[str, int]], int]:
    """TOC where agenda titles carry NO page of their own (DE-BY): a title is a
    left-column block (x0 small) with no trailing page and not a vote/sub-entry;
    its start page is the first page-bearing line that follows it.

    DE-NW numbers every TOP ("1 Schulen schlagen Alarm: …") and lays the TOC out
    in TWO columns, so the single dominant-column heuristic fails — when
    ``toc_strip_leading_num`` is set we instead pick title candidates by the
    numbered-heading pattern (any column), strip the number, and anchor to the
    first following page-bearing line."""
    pdf = fitz.open(str(pdf_path))
    body = _body_start_page(pdf, cfg)
    offset = page_offset(pdf)
    max_page = pdf.page_count + offset + 2
    min_page = max(1, offset - 2)
    numbered = getattr(cfg, "toc_strip_leading_num", False)
    # collect TOC blocks in reading order: (x0, clean_text, page_or_None)
    blocks: list[tuple[float, str, int | None]] = []
    for pno in range(0, body):
        for b in pdf[pno].get_text("blocks"):
            t = re.sub(r"\s+", " ", b[4]).strip()
            if not t:
                continue
            _, page = _strip_page_tail(t)
            if page is not None and not (min_page <= page <= max_page):
                page = None
            blocks.append((b[0], clean_block_text(t), page))
    # The title column is the dominant x0 among substantial content blocks;
    # indented speaker-refs sit a notch right; footers/headers are far left.
    col_xs = [round(x) for x, t, _ in blocks if len(t) >= 15]
    title_x = statistics.mode(col_xs) if col_xs else 0
    entries: list[tuple[str, int]] = []
    for i, (x0, text, page) in enumerate(blocks):
        if page is not None:
            continue
        if numbered:
            if not _NUMBERED_TOP.match(text):           # title = numbered heading
                continue
            title = re.sub(r"^\d+\s+", "", text).strip()
        else:
            if abs(x0 - title_x) > 4:                   # title = dominant column
                continue
            title = re.sub(r"[.\s·]+$", "", text)
        if len(title) < 15 or not (title[:1].isupper() or title[:1] in "„\"»"):
            continue
        if cfg.toc_noise and cfg.toc_noise.match(title):
            continue
        if _HEADER_LINE.search(title) or _SPEAKER_REF.search(title):
            continue
        start = next((blocks[j][2] for j in range(i + 1, min(i + 9, len(blocks)))
                      if blocks[j][2] is not None), None)
        if start is not None:
            entries.append((title, start))
    seen: dict[str, int] = {}
    for title, page in entries:
        if title not in seen or page < seen[title]:
            seen[title] = page
    return sorted(((t, p) for t, p in seen.items()), key=lambda x: x[1]), offset


def parse_toc(pdf_path, cfg) -> tuple[list[tuple[str, int]], int]:
    if getattr(cfg, "toc_layout", "page-tail") == "indented":
        return parse_toc_indented(pdf_path, cfg)
    pdf = fitz.open(str(pdf_path))
    body = _body_start_page(pdf, cfg)
    offset = page_offset(pdf)
    max_page = pdf.page_count + offset + 2   # reject years / Drucksache numbers
    min_page = max(1, offset - 2)
    in_range = lambda p: min_page <= p <= max_page  # noqa: E731
    entries: list[tuple[str, int]] = []
    for raw_title, page in _iter_entries(pdf, body, in_range):
        if _is_agenda_title(raw_title, cfg):
            entries.append((clean_block_text(raw_title), page))
    # Dedupe (keep earliest page), then sort by start page.
    seen: dict[str, int] = {}
    for title, page in entries:
        if title not in seen or page < seen[title]:
            seen[title] = page
    return sorted(((t, p) for t, p in seen.items()), key=lambda x: x[1]), offset


def title_for_printed_page(toc: list[tuple[str, int]], printed_page: int) -> str | None:
    chosen = None
    for title, start in toc:
        if start <= printed_page:
            chosen = title
        else:
            break
    return chosen
