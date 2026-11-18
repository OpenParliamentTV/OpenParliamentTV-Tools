"""optv.shared.http.PoliteHTTP: spacing, retry and the 404 probe mode."""

import importlib
from urllib.error import HTTPError, URLError

import pytest

from optv.shared import http


class _Response:
    def __init__(self, body: bytes):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def net(monkeypatch):
    """Scripted answers for urlopen (bytes or an exception per request) and a fake clock."""
    state = {"answers": [], "requests": [], "slept": [], "now": 1000.0}

    def urlopen(request, timeout):
        state["requests"].append(request)
        answer = state["answers"].pop(0)
        if isinstance(answer, Exception):
            raise answer
        return _Response(answer)

    def sleep(seconds):
        state["slept"].append(seconds)
        state["now"] += seconds

    monkeypatch.setattr(http, "urlopen", urlopen)
    monkeypatch.setattr(http.time, "sleep", sleep)
    monkeypatch.setattr(http.time, "monotonic", lambda: state["now"])
    return state


def _http_error(code):
    return HTTPError("https://example.invalid/x", code, "error", {}, None)


def test_requests_are_spaced_by_the_polite_delay(net):
    net["answers"] = [b"one", b"two"]
    client = http.PoliteHTTP(polite_delay=0.5)
    assert client.fetch("https://example.invalid/1") == b"one"
    assert net["slept"] == []
    assert client.fetch_text("https://example.invalid/2") == "two"
    assert net["slept"] == [0.5]
    assert net["requests"][0].get_header("User-agent") == http.USER_AGENT


def test_failures_are_retried_with_doubling_delay_then_raise(net):
    net["answers"] = [URLError("down"), _http_error(503), _http_error(404)]
    client = http.PoliteHTTP(polite_delay=0.0)
    with pytest.raises(RuntimeError, match="after 3 attempts"):
        client.fetch("https://example.invalid/x", retry_count=3, base_delay=1.0)
    assert len(net["requests"]) == 3
    assert net["slept"] == [1.0, 2.0]


def test_a_404_is_retried_like_any_error_by_default(net):
    net["answers"] = [_http_error(404), b"late"]
    assert http.PoliteHTTP(polite_delay=0.0).fetch("https://example.invalid/x", retry_count=2) == b"late"


def test_probe_mode_returns_none_on_404_without_retrying(net):
    net["answers"] = [_http_error(404), _http_error(500), b"body"]
    client = http.PoliteHTTP(polite_delay=0.0, none_on_404=True)
    assert client.fetch_text("https://example.invalid/missing") is None
    assert len(net["requests"]) == 1
    assert client.fetch_text("https://example.invalid/flaky", retry_count=2) == "body"


def test_post_sends_a_form_body(net):
    net["answers"] = ["ünïcode".encode()]
    text = http.PoliteHTTP(polite_delay=0.0).post_text("https://example.invalid/form", {"a": "1", "b": "ä"})
    request = net["requests"][0]
    assert text == "ünïcode"
    assert request.data == b"a=1&b=%C3%A4"
    assert request.get_header("Content-type") == "application/x-www-form-urlencoded"
    assert request.get_method() == "POST"


@pytest.mark.parametrize("parliament, probe", [
    ("DE-BW", False), ("DE-SH", False), ("DE-SN", False), ("DE-HH", True), ("DE-NW", True),
])
def test_landtag_scrapers_share_the_client(parliament, probe):
    common = importlib.import_module(f"optv.parliaments.{parliament}.scraper.common")
    assert isinstance(common._http, http.PoliteHTTP)
    assert common._http.none_on_404 is probe
    assert common._http.polite_delay == 0.5
    assert common.fetch_text.__self__ is common._http
