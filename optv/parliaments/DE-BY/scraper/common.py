"""HTTP + JSF-Ajax session helper for the Bayerischer Landtag "Plenum Online".

The session/TOP index is a PrimeFaces JSF app (``sitzungsablauf_accordion.xhtml``
under ``www.bayern.landtag.de/plon2-webanzeige``). There is **no** directory
listing or JSON index for the playlists; their URLs are only rendered into a TOP
panel when that panel is expanded (the accordion is ``dynamic=true,
cache=false``). So we drive the stateful Ajax conversation:

1. **GET** the accordion → seed the session cookie, the ``_csrf`` hidden field,
   and the ``jakarta.faces.ViewState``. The ``<select id="sitzunggremium_input">``
   dropdown is the session index (``sitzungGremiumId`` → date).
2. **valueChange** POST on ``sitzunggremium`` → load one session's Sitzungsablauf
   (renders the TOP headers and the heading "Sitzungsablauf für die N.
   Plenarsitzung", which carries the citation Sitzungsnr). TOP panel bodies stay
   collapsed.
3. **tabChange** POST per TOP (with the dynamic-load params
   ``accordion_contentLoad`` / ``accordion_newTab`` / ``accordion_tabindex``) →
   lazy-load the panel. Each debated item in the panel has a playlist; its
   ``openTV1OndemandWindow…(playerUrl, playlistUrl, startId)`` onclick handlers
   carry the playlist URL. Up to sitting 87 the handler is
   ``…ForStreamingInfoFromMetafile`` with a ``meta_vod_{N}.json`` file, from
   sitting 88 ``…ForStreamingInfoFromDatabase`` with ``streaming?topId={N}``.
   Both return the same ``items[]`` JSON, and ``N`` is the same TOP-item id.

A TOP panel can hold several playlists — the debate on urgent motions
(Dringlichkeitsanträge) has one per motion. Each speech row carries the
speaker's biography link (``…/profil/{ID}``, absent for state ministers) and a
download button with the speech's ``aktionId``.

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

from optv.shared.http import USER_AGENT

logger = logging.getLogger(__name__)


PLON_BASE = "https://www.bayern.landtag.de/plon2-webanzeige"
ACCORDION_URL = f"{PLON_BASE}/views/sitzung/sitzungsablauf_accordion.xhtml"

# Wahlperiode → (first sitting day inclusive, last day exclusive or None).
# The accordion dropdown is not WP-filtered (it lists older WPs too), so we
# filter the (gremiumId, date) options by date. WP 19 of the Bavarian Landtag
# opened with the konstituierende Sitzung on 30.10.2023.
WP_DATE_RANGE: dict[int, tuple[str, str | None]] = {
    19: ("2023-10-30", None),
}

# Polite delay between successive requests to the Landtag's shared infra.
POLITE_DELAY = 1.0
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
        """Plain GET (e.g. for a playlist JSON on the site)."""
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
# openTV1OndemandWindow…('playerUrl','playlistUrl', 'startId') — the handler
# name varies (…ForStreamingInfoFromMetafile up to sitting 87,
# …ForStreamingInfoFromDatabase from sitting 88).
_ONCLICK_RE = re.compile(
    r"openTV1OndemandWindow\w*\('(?P<player>[^']*)','(?P<url>[^']*)',\s*'?\s*(?P<start>\d+)'?\)",
    re.I,
)
# Speech-row video button: nested client id accordion:i:itemParts:p:j_idtX:r:j_idtY.
# The sub-item header button (accordion:i:itemParts:p:j_idtX) is not nested.
_ROW_BUTTON_RE = re.compile(
    r'<button id="accordion:\d+:itemParts:\d+:j_idt\d+:\d+:j_idt\d+"[^>]*'
    r'onclick="[^"]*?openTV1OndemandWindow\w*\(\'[^\']*\',\'(?P<url>[^\']*)\','
    r'\s*\'?\s*(?P<start>\d+)\'?\)',
    re.I,
)
_DOWNLOAD_RE = re.compile(r"openDownloadWindow\('[^']*','(?P<aktion>[^']*)'\)")
_PROFILE_RE = re.compile(r'/profil/(\d+)"')
_ROW_LABEL_RE = re.compile(r'<span style="font-size:14px;display:inline;[^"]*">([^<]+)</span>')
_PLAYLIST_ID_RE = re.compile(r'(?:meta_vod_(\d+)\.json|[?&]topId=(\d+))', re.I)
_HEADING_RE = re.compile(r'Sitzungsablauf\s+f(?:ü|&uuml;|&#252;)r\s+die\s+(\d+)\.\s*Plenarsitzung', re.I)


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
    """Sitzungsnr from the heading "Sitzungsablauf für die N. Plenarsitzung".

    The Tagesordnung link in the same view carries ``sitzungsnr=`` too, but in a
    multi-day plenary week it points to the agenda of the week's first day, so
    it is wrong for the second and third day and is not used.
    """
    m = _HEADING_RE.search(session_html)
    return int(m.group(1)) if m else None


def playlist_id(url: str) -> str | None:
    """TOP-item id of a playlist URL (``meta_vod_{N}.json`` or ``topId={N}``)."""
    m = _PLAYLIST_ID_RE.search(url or "")
    if not m:
        return None
    return m.group(1) or m.group(2)


def _is_whole_top(url: str) -> bool:
    return "gesamt=true" in url


def _parse_rows(panel_html: str) -> dict[str, list[dict]]:
    """Speech rows per playlist URL: ``{url: [{start_id, label, profile_id, aktion_id}]}``."""
    rows: dict[str, list[dict]] = {}
    buttons = list(_ROW_BUTTON_RE.finditer(panel_html))
    for i, m in enumerate(buttons):
        url = htmllib.unescape(m.group("url"))
        if _is_whole_top(url):
            continue
        # The row's own cells: the image <td> and the label <td> before the button.
        td = panel_html.rfind("<td>", 0, m.start())
        td = panel_html.rfind("<td>", 0, td)
        cell = panel_html[td:m.start()] if td >= 0 else ""
        prof = _PROFILE_RE.findall(cell)
        labels = [_clean_text(x) for x in _ROW_LABEL_RE.findall(cell)]
        # The row's download button follows its video button, before the next row.
        end = buttons[i + 1].start() if i + 1 < len(buttons) else len(panel_html)
        dl = _DOWNLOAD_RE.search(panel_html, m.end(), end)
        rows.setdefault(url, []).append({
            "start_id": int(m.group("start")),
            "label": labels[-1] if labels else None,
            "profile_id": prof[-1] if prof else None,
            "aktion_id": dl.group("aktion") if dl else None,
        })
    return rows


def parse_tab_playlists(panel_html: str) -> list[dict]:
    """Return the playlists of one expanded TOP panel, in panel order.

    Each entry is ``{tid, url, player_url, rows}``: ``tid`` the TOP-item id,
    ``url`` the playlist JSON URL, ``player_url`` the on-demand player page, and
    ``rows`` the speech rows (``start_id``, ``label``, ``profile_id``,
    ``aktion_id``). The ``gesamt=true`` variant (the whole item as one stream)
    is skipped. A TOP panel usually has one playlist, the urgent-motions debate
    one per motion.
    """
    rows = _parse_rows(panel_html)
    out: list[dict] = []
    seen: set[str] = set()
    for m in _ONCLICK_RE.finditer(panel_html):
        url = htmllib.unescape(m.group("url"))
        if _is_whole_top(url) or url in seen:
            continue
        seen.add(url)
        out.append({
            "tid": playlist_id(url),
            "url": url,
            "player_url": htmllib.unescape(m.group("player")),
            "rows": rows.get(url, []),
        })
    return out
