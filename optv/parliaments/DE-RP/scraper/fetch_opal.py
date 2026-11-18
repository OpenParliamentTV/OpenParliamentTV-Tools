#! /usr/bin/env python3

# Save an OPAL "Suche nach Reden" result page into the media inbox, for
# test runs.
#
# OPAL renders its results with JavaScript, so a plain HTTP request gets only
# the page frame; this helper drives a headless browser (Scrapling's
# DynamicFetcher, Playwright underneath). opal.rlp.de's robots.txt allows
# only Googlebot: use this for tests, not for continuous ingestion.
#
# The result list shows 50 hits per page; the helper switches the page-size
# select to "Alle auf einer Seite" and waits until the counter reads
# "Treffer: 1 bis Y von Y" and the speech rows are rendered again, so the
# saved page holds every speech of the search. One search per call.
#
# Install the browser dependency with
#   pip install -r requirements-browser.txt && scrapling install

from __future__ import annotations

import argparse
from datetime import date
import logging
from pathlib import Path
import re

logger = logging.getLogger(__name__)

OPAL_SEARCH = "https://opal.rlp.de/portal/browse.tt.html"
COUNTER_SELECTOR = "#efxSegmentCounter"
PAGE_SIZE_SELECTOR = "#pagination-select"
ALL_ON_ONE_PAGE = "0"
ROW_SELECTOR = "a[data-video]"
COUNTER_RE = re.compile(r"Treffer:\s*(\d+)\s*bis\s*(\d+)\s*von\s*(\d+)")


def _de_date(d: date) -> str:
    return d.strftime("%d.%m.%Y")


def opal_search_url(wp: int, date_from: date, date_to: date) -> str:
    """Link URL of OPAL's speech search for one electoral period and date range."""
    return (f"{OPAL_SEARCH}?type=generic4&action=link"
            f"&searchgeneric4-from={_de_date(date_from)}"
            f"&searchgeneric4-to={_de_date(date_to)}"
            f"&searchgeneric4-wp={wp}")


def inbox_filename(wp: int, date_from: date, date_to: date) -> str:
    return f"opal-wp{wp}-{date_from.isoformat()}-{date_to.isoformat()}.html"


def hit_counter(text: str) -> tuple[int, int] | None:
    """'Treffer: 1 bis 50 von 149' → (50, 149)."""
    m = COUNTER_RE.search(text or "")
    if not m:
        return None
    return int(m.group(2)), int(m.group(3))


def expand_all_hits(page):
    """Playwright page action: show all hits on one page.

    Scrapling runs the page action before its own `wait_selector` and only
    logs an exception, so wait for the results here; fetch_speech_search
    re-checks the counter of the returned page.
    """
    page.locator(ROW_SELECTOR).first.wait_for(state="attached", timeout=60000)
    counter = page.locator(COUNTER_SELECTOR).first
    text = counter.inner_text() if counter.count() else ""
    hits = hit_counter(text)
    if hits is None or hits[0] >= hits[1]:
        return page
    # The native select sits hidden behind a multiselect widget.
    page.locator(PAGE_SIZE_SELECTOR).first.select_option(ALL_ON_ONE_PAGE, force=True)
    page.wait_for_function(
        "([sel, prev]) => { const e = document.querySelector(sel);"
        " return e && e.innerText !== prev; }",
        arg=[COUNTER_SELECTOR, text], timeout=60000)
    # The result list re-renders; its speech rows arrive after the counter.
    page.locator(ROW_SELECTOR).first.wait_for(state="attached", timeout=60000)
    page.wait_for_load_state("networkidle")
    return page


def _default_fetch(url: str) -> str:
    try:
        from scrapling.fetchers import DynamicFetcher
    except ImportError as e:
        raise SystemExit(
            "fetch_opal needs Scrapling with its browser: "
            "pip install -r requirements-browser.txt && scrapling install") from e
    response = DynamicFetcher.fetch(
        url,
        wait_selector="a[data-video]",
        network_idle=True,
        disable_resources=True,
        google_search=False,
        timeout=60000,
        page_action=expand_all_hits,
    )
    if response.status != 200:
        raise RuntimeError(f"OPAL returned HTTP {response.status} for {url}")
    return response.html_content


def fetch_speech_search(wp: int, date_from: date, date_to: date, inbox_dir: Path,
                        fetch=_default_fetch) -> Path:
    """Render OPAL's speech search and save the page into the media inbox."""
    url = opal_search_url(wp, date_from, date_to)
    logger.info(f"Fetching {url}")
    page_html = fetch(url)
    hits = hit_counter(page_html)
    if hits is None:
        logger.warning("No hit counter on the OPAL page; the search may have returned nothing")
    elif hits[0] < hits[1]:
        logger.warning(f"Only {hits[0]} of {hits[1]} hits rendered; "
                       f"fetch a narrower date range")
    inbox_dir = Path(inbox_dir)
    inbox_dir.mkdir(parents=True, exist_ok=True)
    target = inbox_dir / inbox_filename(wp, date_from, date_to)
    target.write_text(page_html, encoding="utf-8")
    logger.info(f"Saved {target} ({hits[1] if hits else 0} hits)")
    return target


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Save an OPAL speech search into the media inbox (headless browser, for tests).")
    parser.add_argument("--wp", type=int, required=True, help="Electoral period")
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat, required=True,
                        help="First sitting day (YYYY-MM-DD)")
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat, required=True,
                        help="Last sitting day (YYYY-MM-DD)")
    parser.add_argument("--inbox-dir", required=True,
                        help="Media inbox (typically <data>/original/media/inbox)")
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    fetch_speech_search(args.wp, args.date_from, args.date_to, Path(args.inbox_dir))
