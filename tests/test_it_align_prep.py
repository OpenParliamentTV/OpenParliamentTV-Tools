"""IT audio staging: the recording of a part is fetched as one file, continued when interrupted, and not fetched when its audio is there. No network."""

from __future__ import annotations

import io
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


def test_nothing_is_requested_when_the_audio_is_there(tmp_path, monkeypatch):
    target = tmp_path / "audio_session" / "abc.mp3"
    target.parent.mkdir()
    target.write_bytes(b"mp3")
    monkeypatch.setattr(A, "clip_seconds", lambda path: 5000.0)
    monkeypatch.setattr(A, "fetch_file", lambda *a, **k: pytest.fail("the recording was requested"))
    A._download(URL, target, required_duration=5020.0)        # within the tolerance of the audio's length


def test_the_recording_is_fetched_turned_into_audio_and_deleted(tmp_path, monkeypatch):
    target = tmp_path / "audio_session" / "abc.mp3"
    calls = []

    def fake_fetch(url, path):
        path.write_bytes(b"video")
        calls.append(("fetch", url, path.name))

    def fake_extract(source, out, **options):
        calls.append(("audio", source, options["hls"], options["required_duration"]))
        out.write_bytes(b"mp3")

    monkeypatch.setattr(A, "fetch_file", fake_fetch)
    monkeypatch.setattr(A, "download_ffmpeg", fake_extract)
    A._download(URL, target, required_duration=1234.0)
    video = tmp_path / "audio_session" / "abc.mp3.video.part"
    assert calls == [("fetch", URL, "abc.mp3.video.part"), ("audio", str(video), False, 1234.0)]
    assert target.read_bytes() == b"mp3" and not video.exists()


def test_audio_that_ends_before_the_last_slice_is_not_enough(tmp_path, monkeypatch):
    target = tmp_path / "abc.mp3"
    assert not A.has_audio(target, 100.0)
    target.write_bytes(b"mp3")
    monkeypatch.setattr(A, "clip_seconds", lambda path: 900.0)
    assert A.has_audio(target, 929.0) and not A.has_audio(target, 931.0)


def test_only_speeches_with_text_and_a_bounded_range_are_staged():
    speech = {"media": {"videoFileURI": URL + "#t=30,60"},
              "textContents": [{"textBody": [{"sentences": [{"text": "Grazie."}]}]}]}
    spec = A.fragment_speech_audio(speech)
    assert (spec.source_url, spec.session_key, spec.start, spec.duration) == (URL, URL, 30.0, 30.0)
    assert A.fragment_speech_audio({**speech, "textContents": []}) is None
    assert A.fragment_speech_audio({**speech, "media": {"videoFileURI": URL + "#t=30"}}) is None
