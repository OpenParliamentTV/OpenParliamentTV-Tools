"""Length-gated secondary splitting of over-long sentences.

The spaCy rule-based ``sentencizer`` used across the German tiers
(proceedings2json, parlamint2json, lang.de.spacy_sentencize) only breaks on
``. ! ? …`` and has no length awareness, so German parliamentary style — clauses
chained with ``;``, parenthetical ``– … –`` interjections, and ``:``-introduced
enumerations — yields a long tail of 50-200 word "sentences". This module
re-cuts only those, on secondary boundaries, leaving normal sentences untouched.

Design:

* **Length-gated** — a sentence at or under ``threshold`` words is returned
  unchanged, so the typical sentence (median ~12 words) is never altered.
* **Priority boundaries** — ``;`` first, then a parenthetical/standalone dash
  (``–``/``—``), then ``:``. Commas are a *last resort*, used only on a piece
  still over ``hard_cap`` (enumerations have no cleaner boundary).
* **Temporal order preserved** — pieces stay in spoken order, so each maps to a
  contiguous span of audio (a ``– X –`` interjection becomes its own in-order
  piece, not a head/tail that brackets the clause). This is what makes the
  output safe to re-time by sub-dividing the original sentence's interval.
* **No tiny fragments** — a piece below ``min_words`` is merged back into its
  preceding neighbour (the following one if it is the first), so a 3-word aside
  rides along with the clause it interrupts instead of becoming its own unit.

Returns the same text content re-segmented; the concatenation of the pieces is
the original sentence (delimiters normalised to a single space at split points).
"""

from __future__ import annotations

import re

DEFAULT_THRESHOLD = 50
DEFAULT_HARD_CAP = 80
DEFAULT_MIN_WORDS = 4

# Spaced en/em dash (paired interjection or trailing aside), optionally swallowing
# a comma that hugs the closing dash ("… trifft –, an der Zeit"). A hyphen in a
# German compound ("Strom- und Wärmemarkt") has no surrounding spaces, so it is
# never matched.
_DASH_RE = re.compile(r"\s+[–—][\s,]+")
_SEMI_RE = re.compile(r"\s*;\s+")
_COLON_RE = re.compile(r"\s*:\s+")
_COMMA_RE = re.compile(r"\s*,\s+")


def _wc(s: str) -> int:
    return len(s.split())


def _merge_tiny(parts: list[str], min_words: int) -> list[str]:
    """Coalesce pieces so none is below ``min_words``, preserving order.

    Greedy accumulation: pieces are joined into the current chunk until it
    reaches ``min_words``, then a new chunk starts; a leftover short tail folds
    into the previous chunk. A 3-word aside therefore rides along with an
    adjacent clause instead of standing alone, and a run of short enumeration
    items groups into chunks rather than collapsing into one giant piece."""
    if len(parts) <= 1:
        return parts
    out: list[str] = []
    buf = ""
    for p in parts:
        buf = f"{buf} {p}" if buf else p
        if _wc(buf) >= min_words:
            out.append(buf)
            buf = ""
    if buf:
        if out:
            out[-1] = f"{out[-1]} {buf}"
        else:
            out.append(buf)
    return out


def _try_split(text: str, rx: re.Pattern, min_words: int) -> list[str] | None:
    """Split on ``rx``; merge tiny pieces. Returns the pieces if it produced a
    real (>1) split, else None."""
    parts = [p.strip() for p in rx.split(text) if p.strip()]
    if len(parts) <= 1:
        return None
    parts = _merge_tiny(parts, min_words)
    return parts if len(parts) > 1 else None


def split_long_sentence(text: str, *, threshold: int = DEFAULT_THRESHOLD,
                        hard_cap: int = DEFAULT_HARD_CAP,
                        min_words: int = DEFAULT_MIN_WORDS,
                        clause_re: "re.Pattern | None" = None) -> list[str]:
    """Re-cut ``text`` into a list of sentence pieces if it exceeds ``threshold``
    words; otherwise return ``[text]`` unchanged.

    ``clause_re``, if given, matches a *language-specific* clause boundary (e.g.
    a comma before a German subordinating conjunction / relative pronoun). It is
    tried after the punctuation boundaries but before the blind-comma last
    resort, so long subordinate-clause / conditional chains break at real clause
    starts rather than on arbitrary commas."""
    text = (text or "").strip()
    if not text or _wc(text) <= threshold:
        return [text] if text else []

    def recurse(parts):
        out: list[str] = []
        for p in parts:
            if _wc(p) > threshold:
                out.extend(split_long_sentence(
                    p, threshold=threshold, hard_cap=hard_cap,
                    min_words=min_words, clause_re=clause_re))
            else:
                out.append(p)
        return out

    # Priority boundaries: cleanest first. Recurse so a still-long piece is
    # re-cut on the next available boundary.
    splitters = [_SEMI_RE, _DASH_RE, _COLON_RE]
    if clause_re is not None:
        splitters.append(clause_re)
    for rx in splitters:
        parts = _try_split(text, rx, min_words)
        if parts:
            return recurse(parts)
    # Last resort: comma-split only a piece still over the hard cap (a bare
    # enumeration with no cleaner boundary).
    if _wc(text) > hard_cap:
        parts = _try_split(text, _COMMA_RE, min_words)
        if parts:
            return parts
    return [text]


def split_long_sentences(texts, **kwargs) -> list[str]:
    """Apply :func:`split_long_sentence` to each text, flattening the result."""
    out: list[str] = []
    for t in texts:
        out.extend(split_long_sentence(t, **kwargs))
    return out
