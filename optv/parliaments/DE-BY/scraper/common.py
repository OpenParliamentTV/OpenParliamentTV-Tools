"""HTTP + JSF-Ajax session helper for the Bayerischer Landtag "Plenum Online".

The session/TOP index is a PrimeFaces 13 JSF app
(``sitzungsablauf_accordion.xhtml`` on ``www1.bayern.landtag.de``). There is
**no** directory listing or JSON index for the per-TOP playlists; the
``meta_vod_*.json`` URLs are only rendered into a TOP panel when that panel is
expanded (the accordion is ``dynamic=true, cache=false``). So we drive the
stateful Ajax conversation:

1. **GET** the accordion → seed the ``PLON-Webanzeige`` session cookie, the
   ``_csrf`` hidden field, and the ``jakarta.faces.ViewState``. The
   ``<select id="sitzunggremium_input">`` dropdown is the session index
   (``sitzungGremiumId`` → date).
2. **valueChange** POST on ``sitzunggremium`` → load one session's Sitzungsablauf
   (renders the TOP headers + the Tagesordnung link carrying the citation
   ``sitzungsnr``). TOP panel bodies stay collapsed.
3. **tabChange** POST per TOP (with the dynamic-load params
   ``accordion_contentLoad`` / ``accordion_newTab`` / ``accordion_tabindex``) →
   lazy-load the panel, whose ``openTV1OndemandWindow(playerUrl, metaFileUrl,
   startId)`` onclick handlers carry the ``meta_vod`` playlist URL.

Stdlib ``urllib`` + ``http.cookiejar``; the session cookie and the ViewState
carry the conversation state. No browser, no external dependency (mirrors the
DE-SH scraper philosophy).
"""

from __future__ import annotations

import html as htmllib
import logging
import re
import time
from http.cookiejar import CookieJar
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener

logger = logging.getLogger(__name__)

USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")

PLON_BASE = "https://www1.bayern.landtag.de/plon-webanzeige"
ACCORDION_URL = f"{PLON_BASE}/views/sitzung/sitzungsablauf_accordion.xhtml"

# Wahlperiode → (first sitting day inclusive, last day exclusive or None).
# The accordion dropdown is not WP-filtered (it lists older WPs too), so we
# filter the (gremiumId, date) options by date. WP 19 of the Bavarian Landtag
# opened with the konstituierende Sitzung on 30.10.2023.
WP_DATE_RANGE: dict[int, tuple[str, str | None]] = {
    19: ("2023-10-30", None),
}

# Polite delay between successive requests to the Landtag's shared infra.
POLITE_DELAY = 0.5
_LAST_FETCH_AT = 0.0


def _viewstate(text: str) -> str | None:
    """Extract the ViewState from a full HTML page or a partial-response XML."""
    m = re.search(r'name="jakarta\.faces\.ViewState"[^>]*value="([^"]*)"', text)
    if m:
        return m.group(1)
    m = re.search(r'ViewState[^>]*><!\[CDATA\[([^\]]*)\]\]>', text)
    return m.group(1) if m else None


def _csrf(text: str) -> str | None:
    m = re.search(r'name="_csrf"[^>]*value="([^"]*)"', text)
    return m.group(1) if m else None


class PlonSession:
    """A stateful JSF-Ajax conversation with the Plenum Online accordion."""

    def __init__(self, *, retry_count: int = 20, timeout: float = 60.0):
        self.retry_count = retry_count
        self.timeout = timeout
        self._opener = build_opener(HTTPCookieProcessor(CookieJar()))
        self.viewstate: str | None = None
        self.csrf: str | None = None
        self.current_gremium: str | None = None

    # -- transport ---------------------------------------------------------
    def _request(self, req: Request, *, base_delay: float = 1.0) -> str:
        global _LAST_FETCH_AT
        delay = base_delay
        last_err: Exception | None = None
        for attempt in range(1, max(self.retry_count, 1) + 1):
            wait = _LAST_FETCH_AT + POLITE_DELAY - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            try:
                with self._opener.open(req, timeout=self.timeout) as resp:
                    _LAST_FETCH_AT = time.monotonic()
                    return resp.read().decode("utf-8", errors="replace")
            except (HTTPError, URLError, TimeoutError) as e:
                _LAST_FETCH_AT = time.monotonic()
                last_err = e
                if attempt >= self.retry_count:
                    break
                logger.warning(f"HTTP retry {attempt}/{self.retry_count} after {delay:.1f}s: {e}")
                time.sleep(delay)
                delay = min(delay * 2, 60.0)
        raise RuntimeError(f"HTTP failed after {self.retry_count} attempts: {last_err}")

    # -- JSF conversation --------------------------------------------------
    def get_text(self, url: str) -> str:
        """Plain GET (e.g. for a ``meta_vod_*.json`` playlist on the CDN/site)."""
        req = Request(url, headers={"User-Agent": USER_AGENT})
        return self._request(req)

    def start(self) -> str:
        """GET the accordion, seed cookie + ViewState + _csrf. Returns the HTML."""
        req = Request(ACCORDION_URL, headers={"User-Agent": USER_AGENT})
        html = self._request(req)
        self.viewstate = _viewstate(html)
        self.csrf = _csrf(html)
        if not self.viewstate or not self.csrf:
            raise RuntimeError("Could not seed ViewState/_csrf from the accordion page")
        return html

    def _ajax(self, payload: dict[str, str]) -> str:
        if self.viewstate is None or self.csrf is None:
            self.start()
        data = {
            "sitzungsablaufAccordionForm": "sitzungsablaufAccordionForm",
            "_csrf": self.csrf,
            "jakarta.faces.ViewState": self.viewstate,
            "jakarta.faces.partial.ajax": "true",
        }
        if self.current_gremium is not None:
            data["sitzunggremium_input"] = self.current_gremium
        data.update(payload)
        req = Request(
            ACCORDION_URL,
            data=urlencode(data).encode("utf-8"),
            headers={
                "User-Agent": USER_AGENT,
                "Content-Type": "application/x-www-form-urlencoded",
                "Faces-Request": "partial/ajax",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/xml, text/xml, */*",
                "Referer": ACCORDION_URL,
            },
        )
        text = self._request(req)
        # The server rotates the ViewState on most responses; keep the latest.
        new_vs = _viewstate(text)
        if new_vs:
            self.viewstate = new_vs
        return text

    def load_session(self, gremium_id: int | str) -> str:
        """valueChange: load the Sitzungsablauf for one ``sitzungGremiumId``."""
        self.current_gremium = str(gremium_id)
        return self._ajax({
            "sitzunggremium_input": str(gremium_id),
            "accordion_active": "",
            "jakarta.faces.source": "sitzunggremium",
            "jakarta.faces.partial.event": "change",
            "jakarta.faces.partial.execute": "sitzunggremium",
            "jakarta.faces.partial.render": "sitzungsablaufAccordionForm",
            "jakarta.faces.behavior.event": "valueChange",
        })

    def load_tab(self, tab_index: int, tab_component_id: str) -> str:
        """tabChange: lazy-load one TOP panel (dynamic accordion content load)."""
        return self._ajax({
            "automaticRefreshOnOff": "true",
            "accordion_active": str(tab_index),
            "accordion_contentLoad": "true",
            "accordion_newTab": tab_component_id,
            "accordion_tabindex": str(tab_index),
            "jakarta.faces.source": "accordion",
            "jakarta.faces.partial.event": "tabChange",
            "jakarta.faces.behavior.event": "tabChange",
            "jakarta.faces.partial.execute": "accordion",
            "jakarta.faces.partial.render": "accordion",
        })


# -- parsing helpers (shared by fetch_archive) -----------------------------

# <option value="640" ...>21.05.2026</option> in the sitzunggremium dropdown.
_OPTION_RE = re.compile(
    r'<option\s+value="(?P<gremium>\d+)"[^>]*>(?P<date>\d{2}\.\d{2}\.\d{4})</option>',
    re.I,
)
# Per-TOP header: id="accordion:0:j_idt70_header" ... > <title markup> < panel div
_TAB_HEADER_RE = re.compile(
    r'id="(?P<comp>accordion:(?P<idx>\d+):[a-z0-9_]+)_header"[^>]*>'
    r'(?P<inner>.*?)<div[^>]*id="accordion:(?P=idx):[a-z0-9_]+"',
    re.I | re.S,
)
# openTV1OndemandWindow('playerUrl','metaFileUrl',' startId')
_ONCLICK_RE = re.compile(
    r"openTV1OndemandWindow\('[^']*','(?P<meta>[^']*)','?\s*(?P<start>\d+)'?\)",
    re.I,
)
_SITZUNGSNR_RE = re.compile(r'sitzungsnr=(\d+)', re.I)


def parse_session_options(accordion_html: str) -> list[tuple[int, str]]:
    """Return ``[(gremium_id, "DD.MM.YYYY"), ...]`` from the session dropdown."""
    return [(int(m.group("gremium")), m.group("date"))
            for m in _OPTION_RE.finditer(accordion_html)]


def _clean_text(fragment: str) -> str:
    txt = htmllib.unescape(re.sub(r'<[^>]+>', ' ', fragment))
    return re.sub(r'\s+', ' ', txt).strip()


def parse_tab_headers(session_html: str) -> list[dict]:
    """Return ``[{index, component_id, title}, ...]`` for the loaded session.

    ``component_id`` is the per-tab JSF client id (e.g. ``accordion:1:j_idt70``)
    needed as ``accordion_newTab`` for the dynamic content load.
    """
    out: list[dict] = []
    for m in _TAB_HEADER_RE.finditer(session_html):
        out.append({
            "index": int(m.group("idx")),
            "component_id": m.group("comp"),
            "title": _clean_text(m.group("inner")),
        })
    out.sort(key=lambda t: t["index"])
    return out


def parse_sitzungsnr(session_html: str) -> int | None:
    m = _SITZUNGSNR_RE.search(session_html)
    return int(m.group(1)) if m else None


def parse_tab_meta(panel_html: str) -> tuple[str | None, int]:
    """Return ``(meta_vod_url, speech_count)`` for one expanded TOP panel.

    Each speech links to the same per-TOP ``meta_vod`` playlist with a distinct
    ``startId``; ``speech_count`` is the number of those links.
    """
    metas: list[str] = []
    starts: set[str] = set()
    for m in _ONCLICK_RE.finditer(panel_html):
        metas.append(m.group("meta"))
        starts.add(m.group("start"))
    if not metas:
        return None, 0
    # One distinct meta_vod per TOP in practice; take the first.
    return htmllib.unescape(metas[0]), len(starts)
