"""DE media list download: a shorter answer from the feed never replaces the cached list.

The feed occasionally answers 200 with part of a sitting's clips missing; the
latest sitting is re-fetched on every run, so without the guard such an answer
overwrote a complete list (21062: 198 clips became 1). Network access and
parsing are replaced by fakes.
"""

from __future__ import annotations

import json
import logging

import pytest

from optv.parliaments.DE.scraper import fetch_media as fm


@pytest.fixture
def feed(monkeypatch):
    """Set how many clips the fake feed returns for the next download."""
    answer = {}
    monkeypatch.setattr(fm, "download_meeting_data", lambda period, meeting: answer["raw"])
    monkeypatch.setattr(fm, "parse_media_data",
                        lambda raw: {"meta": {}, "data": [{"id": e["title"]} for e in raw["entries"]]})

    def clips(n: int) -> None:
        answer["raw"] = {"root": {"status": 200},
                         "entries": [{"title": f"clip {i}"} for i in range(n)]}
    return clips


def _download(tmp_path):
    return fm.download_data(21, 62, tmp_path, save_raw_data=True, force=True)


def _saved(tmp_path) -> tuple[int, int]:
    parsed = json.loads((tmp_path / "21062-media.json").read_text())
    raw = json.loads((tmp_path / "raw-21062-media.json").read_text())
    return len(parsed["data"]), len(raw["entries"])


def test_first_download_is_saved(tmp_path, feed):
    feed(3)
    _, data = _download(tmp_path)
    assert len(data["data"]) == 3
    assert _saved(tmp_path) == (3, 3)


def test_longer_download_replaces_the_cache(tmp_path, feed):
    feed(3)
    _download(tmp_path)
    feed(5)
    _, data = _download(tmp_path)
    assert len(data["data"]) == 5
    assert _saved(tmp_path) == (5, 5)


def test_shorter_download_keeps_the_cache(tmp_path, feed, caplog):
    caplog.set_level(logging.WARNING)
    feed(198)
    _download(tmp_path)
    feed(1)
    _, data = _download(tmp_path)
    # The caller gets the cached list, so update_media counts the fetch as done
    # instead of retrying and cancelling the whole update.
    assert len(data["data"]) == 198
    assert _saved(tmp_path) == (198, 198)
    assert "keeping the cached list" in caplog.text
