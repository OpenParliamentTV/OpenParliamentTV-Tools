"""Shared HTTP helpers for the DE-SH scrapers.

The m7k mediathek is plain nginx + PHP with no anti-bot. Selector
endpoints and ``result.php`` are jQuery ``.load()`` calls (POST,
``application/x-www-form-urlencoded``). The Plenarprotokoll listing is
GET HTML on ``landtag.ltsh.de``. We use stdlib urllib with a polite
User-Agent + a small retry/backoff loop.
"""

from __future__ import annotations

import logging

from optv.shared.http import PoliteHTTP

logger = logging.getLogger(__name__)

# The m7k root serves no page of its own, but its ``iframe.php``, ``result.php``
# and ``*-html-selector.php`` answer the same requests with the same data as the
# redesign the Landtag's Mediathek page embeds
# (``/m7k-web-redesign-testversion-fa-avit/``). Every m7k URL the pipeline
# requests or emits (incl. ``media.sourcePage``) derives from this constant.
M7K_BASE = "https://m7k.ltsh.de"
LANDTAG_BASE = "https://www.landtag.ltsh.de"

# Internal WP IDs used by the m7k selectors. The displayed "WP18/19/20"
# in the UI maps to ``wp=4/5/6`` on the wire. Update when a new WP rolls
# over.
WP_INTERNAL_ID: dict[int, int] = {
    18: 4,
    19: 5,
    20: 6,
}


# Requests 0.5 s apart: m7k has not been observed to rate-limit at that pace, but
# this is a Landtag IT department's shared infra.
_http = PoliteHTTP(polite_delay=0.5)
fetch = _http.fetch
fetch_text = _http.fetch_text
post = _http.post
post_text = _http.post_text
