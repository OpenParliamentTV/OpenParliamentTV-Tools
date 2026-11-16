#! /usr/bin/env python3

# Shared HTTP helpers for the ES scrapers.
#
# www.congreso.es (open data portal and files, intervention search, Diario
# HTML, the open data's MP4 links) sits behind Akamai, which answers requests
# without a browser User-Agent (urllib's or curl's default, a generic tool
# name) with a 403 "Access Denied"; a realistic browser User-Agent is served
# without any challenge. app.congreso.es (archive sitting pages) and
# static.congreso.es (MP4s) serve any client. The HTML text view additionally
# round-trips a cookie, so we keep a per-process cookie jar.

import logging
logger = logging.getLogger(__name__)

import http.cookiejar
import urllib.parse
import urllib.request

# A realistic desktop Chrome UA. www.congreso.es 403s requests without one.
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")

BASE_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

# Shared opener with an in-memory cookie jar (handles the textointegro
# 302→cookie→200 round-trip without us tracking Set-Cookie by hand).
_cookie_jar = http.cookiejar.CookieJar()
_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_cookie_jar))


def fetch_url(url: str, referer: str = None, accept: str = None,
              timeout: int = 120, form: dict = None) -> bytes:
    """GET `url` (POST `form` as an urlencoded body when given) with browser
    headers + shared cookie jar; return raw bytes.

    Raises urllib.error.URLError / HTTPError on failure (callers handle retries).
    """
    headers = dict(BASE_HEADERS)
    if accept:
        headers["Accept"] = accept
    if referer:
        headers["Referer"] = referer
    data = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        headers["X-Requested-With"] = "XMLHttpRequest"
    req = urllib.request.Request(url, data=data, headers=headers)
    with _opener.open(req, timeout=timeout) as resp:
        return resp.read()
