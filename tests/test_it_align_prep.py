"""IT audio staging: the recording of a part is fetched as one file, continued when interrupted, and not fetched when its audio is there. No network."""

from __future__ import annotations

import io
from pathlib import Path
from urllib.error import HTTPError

import pytest

from optv.parliaments.IT import align_prep as A

URL = "https://video.example.org/assemblea19/L19_S0900_parte1.mp4"


class FakeResponse(io.BytesIO):
    def __init__(self, body: bytes, status: int = 200, length: int | None = None):
        super().__init__(body)
        self.status = status
        self.headers = {"Content-Length": str(len(body) if length is None else length)}


class FakeServer:
    """Serves one file and honours ``Range: bytes=N-`` unless told to ignore it."""

    def __init__(self, content: bytes, *, ranges: bool = True, cut_at: int | None = None):
        self.content = content
        self.ranges = ranges
        self.cut_at = cut_at
        self.requests: list[dict] = []

    def __call__(self, request, timeout=None):
        self.requests.append(dict(request.header_items()))
        offset = 0
        asked = request.get_header("Range")
        if asked and self.ranges:
            offset = int(asked.removeprefix("bytes=").rstrip("-"))
            if offset >= len(self.content):
                raise HTTPError(request.full_url, 416, "Range Not Satisfiable", {}, None)
        body = self.content[offset:]
        sent = body if self.cut_at is None else body[:self.cut_at]
        return FakeResponse(sent, status=206 if offset else 200, length=len(body))


def test_a_file_is_fetched_with_one_request(tmp_path):
    server = FakeServer(b"0123456789" * 100)
    path = tmp_path / "audio.mp3.video.part"
    assert A.fetch_file(URL, path, opener=server) == 1000
    assert path.read_bytes() == server.content
    assert len(server.requests) == 1 and "Range" not in server.requests[0]


def test_an_interrupted_download_is_continued(tmp_path):
    content = bytes(range(256)) * 8
    path = tmp_path / "audio.mp3.video.part"
    with pytest.raises(RuntimeError, match="ended early"):
        A.fetch_file(URL, path, opener=FakeServer(content, cut_at=700))
    assert path.stat().st_size == 700
    server = FakeServer(content)
    assert A.fetch_file(URL, path, opener=server) == len(content)
    assert server.requests[0]["Range"] == "bytes=700-"
    assert path.read_bytes() == content


def _whole(monkeypatch, server):
    """``fetch_whole`` against ``server``, with the waits recorded instead of slept."""
    waits: list[float] = []
    fetch = A.fetch_file
    monkeypatch.setattr(A, "fetch_file", lambda url, path: fetch(url, path, opener=server))
    return waits, lambda url, path: A.fetch_whole(url, path, pause=waits.append)


def test_a_transfer_the_server_ends_early_is_continued_in_the_same_run(tmp_path, monkeypatch):
    content = bytes(range(256)) * 8                           # 2048 bytes, 700 per request
    server = FakeServer(content, cut_at=700)
    waits, whole = _whole(monkeypatch, server)
    path = tmp_path / "audio.mp3.video.part"
    assert whole(URL, path) == len(content)
    assert path.read_bytes() == content
    assert [r.get("Range") for r in server.requests] == [None, "bytes=700-", "bytes=1400-"]
    assert waits == [A._CONTINUE_PAUSE_S] * 2


def test_continuing_stops_after_a_few_requests_and_keeps_what_arrived(tmp_path, monkeypatch):
    server = FakeServer(b"x" * 10_000, cut_at=100)
    waits, whole = _whole(monkeypatch, server)
    path = tmp_path / "audio.mp3.video.part"
    with pytest.raises(A.DownloadIncomplete, match="ended early"):
        whole(URL, path)
    assert len(server.requests) == A._MAX_CONTINUATIONS + 1
    assert path.stat().st_size == 100 * (A._MAX_CONTINUATIONS + 1)


def test_a_request_that_brings_nothing_new_is_not_repeated(tmp_path, monkeypatch):
    server = FakeServer(b"x" * 1000, cut_at=0)
    waits, whole = _whole(monkeypatch, server)
    with pytest.raises(A.DownloadIncomplete):
        whole(URL, tmp_path / "audio.mp3.video.part")
    assert len(server.requests) == 1 and waits == []


def test_a_connection_that_breaks_while_reading_keeps_what_arrived(tmp_path):
    class Breaking(FakeResponse):
        def read(self, size=-1):
            data = super().read(size)
            if not data:
                raise ConnectionResetError("reset by peer")
            return data

    def opener(request, timeout=None):
        return Breaking(b"y" * 300, length=1000)

    path = tmp_path / "audio.mp3.video.part"
    with pytest.raises(A.DownloadIncomplete, match="interrupted after 300 bytes"):
        A.fetch_file(URL, path, opener=opener)
    assert path.stat().st_size == 300


def test_a_server_that_ignores_the_range_makes_the_download_start_over(tmp_path):
    content = b"abcdefghij" * 50
    path = tmp_path / "audio.mp3.video.part"
    path.write_bytes(b"stale bytes")
    assert A.fetch_file(URL, path, opener=FakeServer(content, ranges=False)) == len(content)
    assert path.read_bytes() == content


def test_a_file_that_is_whole_already_is_kept(tmp_path):
    content = b"abcdefghij" * 50
    path = tmp_path / "audio.mp3.video.part"
    path.write_bytes(content)
    assert A.fetch_file(URL, path, opener=FakeServer(content)) == len(content)
    assert path.read_bytes() == content


def test_a_download_larger_than_the_free_space_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(A, "_DISK_RESERVE", 10 ** 18)
    path = tmp_path / "audio.mp3.video.part"
    with pytest.raises(RuntimeError, match="not enough disk space"):
        A.fetch_file(URL, path, opener=FakeServer(b"x" * 100))
    assert not path.exists() or path.stat().st_size == 0


def test_a_whole_file_is_kept_when_the_server_offers_all_of_it_again(tmp_path):
    content = b"abcdefghij" * 50
    path = tmp_path / "audio.mp3.video.part"
    path.write_bytes(content)
    before = path.stat().st_mtime_ns
    assert A.fetch_file(URL, path, opener=FakeServer(content, ranges=False)) == len(content)
    assert path.read_bytes() == content and path.stat().st_mtime_ns == before


def _lengths(monkeypatch, seconds: dict[str, float]):
    """Media lengths by file suffix, in place of ffprobe; a file that is not there has none."""
    monkeypatch.setattr(A, "clip_seconds",
                        lambda path: seconds.get(path.suffix, 0.0) if path.exists() else 0.0)


def _staging(monkeypatch, calls):
    """Record the requests for the recording and the audio extraction instead of making them."""
    def fake_fetch(url, path):
        path.write_bytes(b"video")
        calls.append(("fetch", url, path.name))

    def fake_extract(source, out, **options):
        calls.append(("audio", Path(source).name, options["hls"], options["required_duration"]))
        out.write_bytes(b"mp3")

    monkeypatch.setattr(A, "fetch_file", fake_fetch)
    monkeypatch.setattr(A, "download_ffmpeg", fake_extract)


def test_nothing_is_requested_when_the_audio_is_there(tmp_path, monkeypatch):
    target = tmp_path / "audio_session" / "abc.mp3"
    target.parent.mkdir()
    target.write_bytes(b"mp3")
    _lengths(monkeypatch, {".mp3": 5000.0})
    monkeypatch.setattr(A, "fetch_file", lambda *a, **k: pytest.fail("the recording was requested"))
    # The last time range ends after the recording does: the audio is whole all the same.
    A._download(URL, target, required_duration=5040.0)


def test_the_recording_is_fetched_turned_into_audio_and_deleted(tmp_path, monkeypatch):
    target = tmp_path / "audio_session" / "abc.mp3"
    calls = []
    _staging(monkeypatch, calls)
    _lengths(monkeypatch, {".video": 1200.0, ".mp3": 1200.0})
    A._download(URL, target, required_duration=1234.0)
    # The audio is asked to be as long as the recording, not as long as the page's last range.
    assert calls == [("fetch", URL, "abc.mp3.video.part"), ("audio", "abc.mp3.video", False, 1200.0)]
    assert target.read_bytes() == b"mp3"
    assert sorted(p.name for p in target.parent.iterdir()) == ["abc.mp3"]


def test_a_whole_recording_in_the_cache_is_not_requested_again(tmp_path, monkeypatch):
    target = tmp_path / "audio_session" / "abc.mp3"
    target.parent.mkdir()
    (target.parent / "abc.mp3.video").write_bytes(b"video")
    calls = []
    _staging(monkeypatch, calls)
    _lengths(monkeypatch, {".video": 1200.0, ".mp3": 1200.0})
    A._download(URL, target, required_duration=1210.0)
    assert calls == [("audio", "abc.mp3.video", False, 1200.0)]
    assert sorted(p.name for p in target.parent.iterdir()) == ["abc.mp3"]


def test_a_recording_that_is_no_media_file_is_reported_and_kept(tmp_path, monkeypatch):
    target = tmp_path / "audio_session" / "abc.mp3"
    calls = []
    _staging(monkeypatch, calls)
    _lengths(monkeypatch, {})
    with pytest.raises(RuntimeError, match="cannot be read"):
        A._download(URL, target, required_duration=100.0)
    assert [c[0] for c in calls] == ["fetch"] and (target.parent / "abc.mp3.video").exists()


def test_time_ranges_far_beyond_the_recording_are_reported(tmp_path, monkeypatch, caplog):
    calls = []
    _staging(monkeypatch, calls)
    _lengths(monkeypatch, {".video": 1000.0, ".mp3": 1000.0})
    with caplog.at_level("WARNING", logger=A.logger.name):
        A._download(URL, tmp_path / "a" / "near.mp3", required_duration=1000.0 + A._END_SLACK_S)
        assert not caplog.records
        A._download(URL, tmp_path / "b" / "far.mp3", required_duration=1001.0 + A._END_SLACK_S)
    assert "reach to" in caplog.text


def test_audio_counts_as_there_only_when_it_has_a_length(tmp_path, monkeypatch):
    target = tmp_path / "abc.mp3"
    _lengths(monkeypatch, {".mp3": 900.0})
    assert not A.has_audio(target)
    target.write_bytes(b"mp3")
    assert A.has_audio(target)


def test_only_speeches_with_text_and_a_bounded_range_are_staged():
    speech = {"media": {"videoFileURI": URL + "#t=30,60"},
              "textContents": [{"textBody": [{"sentences": [{"text": "Grazie."}]}]}]}
    spec = A.fragment_speech_audio(speech)
    assert (spec.source_url, spec.session_key, spec.start, spec.duration) == (URL, URL, 30.0, 30.0)
    assert A.fragment_speech_audio({**speech, "textContents": []}) is None
    assert A.fragment_speech_audio({**speech, "media": {"videoFileURI": URL + "#t=30"}}) is None
