"""DE proceedings downloader: list paging, early stop, and refusing non-protocols.

The Bundestag opendata list is not strictly newest-first (21108 was once listed
above 21109), renders each entry twice per page, and may answer with a 303 to a
bot challenge. All network access is replaced by a fake PoolManager.
"""

from __future__ import annotations

import logging
import re

import pytest

from optv.parliaments.DE.scraper import fetch_proceedings as fp

PAGE_SIZE = 10


def _url(sid: str) -> str:
    return f"https://www.bundestag.de/resource/blob/{1000000 + int(sid)}/{sid}.xml"


def _page(sids: list[str]) -> bytes:
    # Each entry appears twice, like the real list view + table template.
    items = "".join(f'<li><a href="{_url(s)}">Plenarprotokoll {s}</a></li>' for s in sids)
    rows = "".join(f'<tr><td><a href="{_url(s)}">{s}</a></td></tr>' for s in sids)
    return f"<div><ul>{items}</ul><table>{rows}</table></div>".encode()


def _xml(sid: str, sitzung: int | None = None) -> bytes:
    nr = int(sid[2:]) if sitzung is None else sitzung
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<dbtplenarprotokoll wahlperiode="{sid[:2]}" sitzung-nr="{nr}">'
            f'<sitzungsverlauf/></dbtplenarprotokoll>\n').encode()


class _Response:
    def __init__(self, status: int = 200, data: bytes = b"", headers: dict | None = None):
        self.status = status
        self.data = data
        self.headers = headers or {}


CHALLENGE = _Response(303, b"", {"Location": "/.enodia/challenge?redirect=%2Fx"})


class _FakeHttp:
    """List pages by offset (lists of session ids, or a _Response), blobs by session id."""

    def __init__(self, listing: list[str] | _Response, blobs: dict | None = None):
        self.listing = listing
        self.blobs = blobs or {}
        self.list_offsets: list[int] = []
        self.downloads: list[str] = []

    def request(self, method, url, redirect=True, **kw):
        assert method == "GET"
        assert redirect is False, "redirects must not be followed"
        m = re.search(r"/opendata/[^?]+\?noFilterSet=true&offset=(\d+)$", url)
        if m:
            offset = int(m.group(1))
            self.list_offsets.append(offset)
            if isinstance(self.listing, _Response):
                return self.listing
            chunk = self.listing[offset:offset + PAGE_SIZE]
            return _Response(data=_page(chunk) if chunk else b"\n")
        sid = re.search(r"/(\d{5})\.xml$", url).group(1)
        self.downloads.append(sid)
        blob = self.blobs.get(sid, _xml(sid))
        return blob if isinstance(blob, _Response) else _Response(data=blob)


@pytest.fixture
def run(monkeypatch, tmp_path):
    def _run(http: _FakeHttp, cached=(), fullscan=False):
        for sid in cached:
            (tmp_path / f"{sid}-proceedings.xml").write_bytes(
                fp._with_source_pi(_xml(sid), _url(sid)))
        monkeypatch.setattr(fp.urllib3, "PoolManager", lambda: http)
        created = fp.download_plenary_protocols(str(tmp_path), fullscan=fullscan, period=21)
        return [path.name[:5] for path, _ in created]
    return _run


def _sids(newest: int, count: int) -> list[str]:
    return [f"21{n:03d}" for n in range(newest, newest - count, -1)]


def test_reordered_head_still_fetches_the_missing_protocol(run, tmp_path):
    # Regression: 21108 listed above 21109 and already cached used to stop the
    # scan before 21109 was ever looked at.
    listing = ["21108", "21109", "21107"] + _sids(106, 17)
    http = _FakeHttp(listing)
    created = run(http, cached=["21108", "21107"] + _sids(106, 17))
    assert created == ["21109"]
    assert http.list_offsets == [0]
    stored = (tmp_path / "21109-proceedings.xml").read_bytes()
    assert stored.splitlines()[1] == f'<?source url="{_url("21109")}"?>'.encode()


def test_steady_state_is_one_list_request(run):
    listing = _sids(109, 25)
    http = _FakeHttp(listing)
    assert run(http, cached=listing) == []
    assert http.list_offsets == [0]
    assert http.downloads == []


def test_cold_cache_pages_through_by_entry_count(run):
    listing = _sids(30, 30)
    http = _FakeHttp(listing)
    created = run(http)
    assert sorted(created) == sorted(listing)
    # Offsets count entries (10 per page), not the doubled links.
    assert http.list_offsets == [0, 10, 20, 30]


def test_full_scan_visits_every_page(run):
    listing = _sids(30, 30)
    http = _FakeHttp(listing)
    assert run(http, cached=listing, fullscan=True) == []
    assert http.list_offsets == [0, 10, 20, 30]


def test_challenge_on_list_is_a_warning_not_an_empty_list(run, tmp_path, caplog):
    http = _FakeHttp(CHALLENGE)
    with caplog.at_level(logging.WARNING):
        assert run(http) == []
    assert "enodia" in caplog.text
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("blob", [
    CHALLENGE,
    _Response(200, b"<!DOCTYPE html><html><body>Please wait</body></html>"),
    _Response(200, _xml("21109")[:60]),  # truncated
])
def test_non_protocol_download_is_not_stored(run, tmp_path, blob):
    http = _FakeHttp(["21109", "21108"], blobs={"21109": blob})
    assert run(http, cached=["21108"]) == []
    assert not (tmp_path / "21109-proceedings.xml").exists()
    assert not list(tmp_path.glob("*.part"))


def test_challenge_on_download_stops_the_run(run):
    http = _FakeHttp(["21110", "21109", "21108"], blobs={"21110": CHALLENGE})
    assert run(http, cached=["21108"]) == []
    assert http.downloads == ["21110"]


def test_wrong_session_is_rejected(run, tmp_path):
    http = _FakeHttp(["21081", "21080"], blobs={"21081": _xml("21081", sitzung=80)})
    assert run(http, cached=["21080"]) == []
    assert not (tmp_path / "21081-proceedings.xml").exists()
    assert not list(tmp_path.glob("*.part"))


def test_source_pi_goes_after_xml_declaration_or_first():
    url = "https://example.org/x.xml"
    pi = f'<?source url="{url}"?>\n'.encode()
    with_decl = b'\xef\xbb\xbf<?xml version="1.0"?>\n<root/>\n'
    assert fp._with_source_pi(with_decl, url) == b'\xef\xbb\xbf<?xml version="1.0"?>\n' + pi + b"<root/>\n"
    assert fp._with_source_pi(b"<root/>\n", url) == pi + b"<root/>\n"
