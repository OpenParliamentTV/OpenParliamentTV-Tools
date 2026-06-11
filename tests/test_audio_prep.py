"""Unit tests for the shared per-speech audio-prep driver and its adapters.

No network or ffmpeg: the driver is exercised with recording stub callables,
and each parliament adapter is checked at the boundary — its pure ``_extract``
field mapping plus the deliberate download/slice choice it hands the driver.
This pins the SE/NO/FI/FR/EU/PT consolidation and the DE-BY direct path.
"""

from __future__ import annotations

from pathlib import Path

from optv.shared import audio_prep as ap
from optv.shared.audio_prep import SpeechAudio, md5_key, prepare_per_speech_audio


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _speech(idx: int, *, text: bool = True, **media) -> dict:
    sp = {
        "electoralPeriod": {"number": 19},
        "session": {"number": 54},
        "speechIndex": idx,
        "media": media,
    }
    if text:
        sp["textContents"] = [{"textBody": [{"type": "speech",
                                             "sentences": [{"text": "a"}]}]}]
    return sp


def _target(cachedir: Path, idx: int) -> Path:
    # Mirrors align.cachedfile: {period}{session:03d}{speechIndex}.mp3
    return cachedir / "audio" / f"19054{idx}.mp3"


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #

def test_slice_path_downloads_session_once(tmp_path):
    dl, sl = [], []

    def extract(sp):
        return SpeechAudio("u", start=sp["speechIndex"] * 10.0, duration=5.0, session_key="K")

    def download(url, target, *, required_duration=0.0):
        dl.append((url, target, required_duration))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("session")

    def slice_fn(session_audio, start, duration, out):
        sl.append((session_audio, start, duration, out))
        out.write_text("clip")

    data = [_speech(0), _speech(1)]
    counts = prepare_per_speech_audio(data, tmp_path, extract=extract,
                                      download_session=download, slice_fn=slice_fn)
    assert counts == (2, 0, 0)
    assert len(dl) == 1                                   # one shared-session download
    assert dl[0][1] == tmp_path / "audio_session" / "K.mp3"
    assert [c[1] for c in sl] == [0.0, 10.0]             # per-speech offsets
    assert _target(tmp_path, 0).read_text() == "clip"
    assert _target(tmp_path, 1).exists()


def test_direct_path_writes_target_no_slice(tmp_path):
    dl = []

    def extract(sp):
        return SpeechAudio("u", session_key=None)

    def download(url, target, *, required_duration=0.0):
        dl.append(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("x")

    def slice_fn(*a):
        raise AssertionError("direct path must not slice")

    counts = prepare_per_speech_audio([_speech(3)], tmp_path, extract=extract,
                                      download_session=download, slice_fn=slice_fn)
    assert counts == (1, 0, 0)
    assert dl == [_target(tmp_path, 3)]


def test_cache_hit_short_circuits_and_calls_on_existing(tmp_path):
    _target(tmp_path, 0).parent.mkdir(parents=True)
    _target(tmp_path, 0).write_text("old")
    seen = []

    def extract(sp):
        return SpeechAudio("u", 0.0, 5.0, "K")

    def boom(*a, **k):
        raise AssertionError("must not run on cache hit")

    counts = prepare_per_speech_audio([_speech(0)], tmp_path, extract=extract,
                                      download_session=boom, slice_fn=boom,
                                      on_existing=lambda sp, t: seen.append(t))
    assert counts == (0, 1, 0)
    assert seen == [_target(tmp_path, 0)]


def test_skip_removes_stale_slice(tmp_path):
    stale = _target(tmp_path, 0)
    stale.parent.mkdir(parents=True)
    stale.write_text("stale")

    counts = prepare_per_speech_audio([_speech(0)], tmp_path, extract=lambda sp: None,
                                      download_session=lambda *a, **k: None,
                                      slice_fn=lambda *a: None)
    assert counts == (0, 0, 1)
    assert not stale.exists()


def test_two_pass_required_duration(tmp_path):
    rec = []

    def extract(sp):
        return SpeechAudio("u", start=sp["speechIndex"] * 100.0, duration=50.0, session_key="K")

    def download(url, target, *, required_duration=0.0):
        rec.append(required_duration)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("s")

    def slice_fn(session_audio, start, duration, out):
        out.write_text("c")

    data = [_speech(0), _speech(2)]                       # ends 50 and 250
    prepare_per_speech_audio(data, tmp_path, two_pass=True, extract=extract,
                             download_session=download, slice_fn=slice_fn)
    assert rec == [250.0]


def test_force_reslices_existing(tmp_path):
    _target(tmp_path, 0).parent.mkdir(parents=True)
    _target(tmp_path, 0).write_text("old")
    sl = []

    def extract(sp):
        return SpeechAudio("u", 0.0, 5.0, "K")

    def download(url, target, *, required_duration=0.0):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("session")

    counts = prepare_per_speech_audio([_speech(0)], tmp_path, force=True, extract=extract,
                                      download_session=download,
                                      slice_fn=lambda *a: sl.append(a))
    assert counts == (1, 0, 0)
    assert len(sl) == 1


# --------------------------------------------------------------------------- #
# Adapters: field mapping (_extract) + deliberate driver kwargs
# --------------------------------------------------------------------------- #

def _capture_kwargs(monkeypatch, module):
    captured = {}

    def fake_prepare(data, cachedir, *, force=False, **kw):
        captured.update(kw)
        return (0, 0, 0)

    monkeypatch.setattr(module, "_prepare", fake_prepare)
    return captured


def test_se_adapter(monkeypatch):
    from optv.parliaments.SE import align_prep as se
    sp = {"media": {"audioFileURI": "http://x/d.mp3", "originMediaID": "DEB1",
                    "additionalInformation": {"startOffset": 12.0}, "duration": 30.0}}
    spec = se._extract(sp)
    assert (spec.session_key, spec.start, spec.duration) == ("DEB1", 12.0, 30.0)
    assert spec.source_url.endswith("d.mp3")

    sp0 = {"media": {"audioFileURI": "u", "additionalInformation": {"startOffset": 1},
                     "duration": 0}}
    assert se._extract(sp0) is None
    assert sp0["debug"]["align-skip"] == "zero-duration-from-source"

    kw = _capture_kwargs(monkeypatch, se)
    se.prepare_per_speech_audio([], "x")
    assert kw["extract"] is se._extract
    assert kw["download_session"] is ap.download_http
    assert kw["slice_fn"] is ap.slice_copy


def test_no_adapter(monkeypatch):
    from optv.parliaments.NO import align_prep as no
    sp = {"media": {"duration": 30.0,
                    "additionalInformation": {"audio_source_url": "http://x/m.mp4",
                                              "qbvid": "Q1", "startOffset": 5.0}}}
    spec = no._extract(sp)
    assert (spec.session_key, spec.start) == ("Q1", 5.0)
    assert spec.source_url.endswith("m.mp4")

    wb = {}
    no._writeback(wb, Path("/t/x.mp3"))
    assert wb["media"]["audioFileURI"] == "/t/x.mp3"

    kw = _capture_kwargs(monkeypatch, no)
    no.prepare_per_speech_audio([], "x")
    assert kw["slice_fn"] is ap.slice_copy
    assert kw["on_prepared"] is no._writeback
    assert kw["on_existing"] is no._writeback


def test_fi_adapter(monkeypatch):
    from optv.parliaments.FI import align_prep as fi
    sp = {"media": {"audioFileURI": "http://x/s.m3u8",
                    "additionalInformation": {"eventRef": "E1", "startOffset": 2.0},
                    "duration": 9.0}}
    spec = fi._extract(sp)
    assert spec.session_key == "E1"
    kw = _capture_kwargs(monkeypatch, fi)
    fi.prepare_per_speech_audio([], "x")
    assert kw["slice_fn"] is ap.slice_reencode


def test_eu_adapter(monkeypatch):
    from optv.parliaments.EU import align_prep as eu
    sp = {"media": {"audioFileURI": "http://x/s.m3u8",
                    "additionalInformation": {"eventRef": "E2", "startOffset": 1.0},
                    "duration": 4.0}}
    assert eu._extract(sp).session_key == "E2"
    sp_nokey = {"media": {"audioFileURI": "http://x/y.m3u8",
                          "additionalInformation": {"startOffset": 1.0}, "duration": 4.0}}
    assert eu._extract(sp_nokey).session_key == md5_key("http://x/y.m3u8")
    kw = _capture_kwargs(monkeypatch, eu)
    eu.prepare_per_speech_audio([], "x")
    assert kw["slice_fn"] is ap.slice_reencode


def test_fr_adapter(monkeypatch):
    from optv.parliaments.FR import align_prep as fr
    sp = {"media": {"audioFileURI": "u",
                    "additionalInformation": {"crvId": "C1", "startOffset": 2.0},
                    "duration": 10.0}}
    assert fr._extract(sp).session_key == "C1"
    sub = {"media": {"audioFileURI": "u", "additionalInformation": {"startOffset": 1.0},
                     "duration": 0.05}}
    assert fr._extract(sub) is None
    assert sub["debug"]["align-skip"] == "sub-100ms-duration"

    kw = _capture_kwargs(monkeypatch, fr)
    fr.prepare_per_speech_audio([], "x")
    assert kw["two_pass"] is True
    assert kw["slice_fn"] is ap.slice_reencode


def test_pt_adapter(monkeypatch):
    from optv.parliaments.PT import align_prep as pt
    no_text = {"media": {"audioFileURI": "u",
                         "additionalInformation": {"startOffset": 1.0}, "duration": 5.0}}
    assert pt._extract(no_text) is None                  # text-presence gate
    sp = {"textContents": [{"textBody": [{"type": "speech", "sentences": [{"text": "x"}]}]}],
          "media": {"audioFileURI": "u",
                    "additionalInformation": {"startOffset": 3.0}, "duration": 7.0}}
    assert pt._extract(sp).session_key == md5_key("u")
    kw = _capture_kwargs(monkeypatch, pt)
    pt.prepare_per_speech_audio([], "x")
    assert kw["slice_fn"] is ap.slice_reencode


def test_de_by_adapter(monkeypatch):
    import importlib
    by = importlib.import_module("optv.parliaments.DE-BY.align_prep")
    sp = {"textContents": [{"textBody": [{"type": "speech", "sentences": [{"text": "x"}]}]}],
          "media": {"videoFileURI": "http://x/s.csmil"}}
    spec = by._extract(sp)
    assert spec.session_key is None                      # per-speech, no slice
    assert spec.source_url.endswith("s.csmil")
    assert by._extract({"media": {"videoFileURI": "u"}}) is None   # no text → skip

    kw = _capture_kwargs(monkeypatch, by)
    by.prepare_per_speech_audio([], "x")
    assert kw["extract"] is by._extract
    assert kw["download_session"] is by._download
    assert "slice_fn" not in kw                          # direct mode uses the default
