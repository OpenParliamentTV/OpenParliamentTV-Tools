#! /usr/bin/env python3
"""Small, language-agnostic formatting helpers shared by mergers.

Pure formatting / string utilities with no language or media-type assumptions —
extracted from the per-parliament mergers so they are written once. Anything
language-specific (honorific stripping, chair-title → speaker context) lives in
``optv.shared.lang.<code>`` instead.
"""

from __future__ import annotations

import json
import re

# Slug for deriving a stable agenda id from a free-text title.
SLUG_RE = re.compile(r'[^a-z0-9]+')


def format_offset(value) -> str:
    """Format a media-fragment / time offset: integers without a trailing ``.0``."""
    f = float(value)
    return f"{int(f)}" if f.is_integer() else f"{f:g}"


def split_first_last(name: str) -> tuple[str, str]:
    """Split a ``Firstname Lastname...`` label into ``(first, rest)``."""
    parts = name.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def agenda_id(top_number, title: str, *, slug_re: re.Pattern = SLUG_RE,
              cap: int = 64, fallback: str = "top") -> str:
    """``TOP-{n}`` when the item is numbered, else a length-capped title slug."""
    if top_number:
        return f"TOP-{str(top_number).lower()}"
    slug = slug_re.sub("-", (title or "").lower()).strip("-")
    if len(slug) > cap:
        slug = slug[:cap].rsplit("-", 1)[0]   # cut at a word boundary
    return slug or fallback


def dedupe_documents(documents: list) -> list:
    """Drop duplicate document refs, first-seen order preserved.

    A merged speech unions the ``documents`` of every proceedings item folded
    into it, so the same Drucksache referenced by several items (or carried on
    several speeches under one agenda item) would otherwise appear multiple
    times. Identity is ``sourceURI`` (the stable per-document URL), falling back
    to ``label`` then the whole dict — so refs that never resolved to a URI are
    still de-duplicated by their human label."""
    seen = set()
    out = []
    for doc in documents:
        key = (doc.get("sourceURI") or doc.get("label")
               or json.dumps(doc, sort_keys=True, ensure_ascii=False))
        if key in seen:
            continue
        seen.add(key)
        out.append(doc)
    return out
