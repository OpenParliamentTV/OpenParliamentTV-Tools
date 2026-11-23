"""Unit tests for the shared per-speech audio-prep driver and its adapters.

No network or ffmpeg: the driver is exercised with recording stub callables,
and each parliament adapter is checked at the boundary — its pure ``_extract``
field mapping plus the deliberate download/slice choice it hands the driver.
This pins the SE/NO/FI/FR/EU/PT consolidation and the DE-BY direct path.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

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
    assert dl[0][1] == tmp_path / "audio_session" / f"{md5_key('K')}.mp3"  # hashed, not raw URL/key
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
    assert sp0["debug"]["alignSkip"] == "zero-duration-from-source"

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
    sp = {"media": {"audioFileURI": "u", "originMediaID": "hemi_20261112144538",
                    "additionalInformation": {"startOffset": 2.0},
                    "duration": 10.0}}
    assert fr._extract(sp).session_key == "hemi_20261112144538"
    no_id = {"media": {"audioFileURI": "u", "additionalInformation": {"startOffset": 2.0},
                       "duration": 10.0}}
    assert fr._extract(no_id).session_key == md5_key("u")
    sub = {"media": {"audioFileURI": "u", "additionalInformation": {"startOffset": 1.0},
                     "duration": 0.05}}
    assert fr._extract(sub) is None
    assert sub["debug"]["alignSkip"] == "sub-100ms-duration"

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


# --------------------------------------------------------------------------- #
# Fragment adapter (DE-BW/HH/NW/SH/SN: `<stream>#t=start,end` videoFileURI)
# --------------------------------------------------------------------------- #

def test_fragment_speech_audio_parses_t_fragment():
    spec = ap.fragment_speech_audio(_speech(3, videoFileURI="http://x/s.m3u8#t=10,40"))
    assert spec.source_url == "http://x/s.m3u8"
    assert spec.session_key == "http://x/s.m3u8"        # whole stream is the slice source
    assert (spec.start, spec.duration) == (10.0, 30.0)


def test_fragment_speech_audio_clamps_negative_start():
    spec = ap.fragment_speech_audio(_speech(3, videoFileURI="http://x/c/master.m3u8#t=-5,40"))
    assert (spec.start, spec.duration) == (0.0, 40.0)


def test_fragment_speech_audio_text_gate():
    assert ap.fragment_speech_audio(
        _speech(3, text=False, videoFileURI="http://x/s.m3u8#t=10,40")) is None


def test_fragment_speech_audio_end_fallback_and_unbounded():
    # start-only fragment: end taken from additionalInformation.endOffset
    spec = ap.fragment_speech_audio(
        _speech(3, videoFileURI="http://x/s.mp4#t=10",
                additionalInformation={"endOffset": 25.0}))
    assert (spec.start, spec.duration) == (10.0, 15.0)
    # start-only, no endOffset → unbounded → skip
    assert ap.fragment_speech_audio(_speech(4, videoFileURI="http://x/s.mp4#t=10")) is None
    # no media fragment at all → skip
    assert ap.fragment_speech_audio(_speech(5, videoFileURI="http://x/s.mp4")) is None


def test_make_fragment_prepare_wires_driver(monkeypatch):
    captured = {}

    def fake_prepare(data, cachedir, *, force=False, **kw):
        captured.update(kw)
        return (0, 0, 0)

    monkeypatch.setattr(ap, "prepare_per_speech_audio", fake_prepare)
    ap.make_fragment_prepare(hls=True, reconnect=True)([], "x")
    assert captured["extract"] is ap.fragment_speech_audio
    assert captured["slice_fn"] is ap.slice_reencode
    assert captured["two_pass"] is True


def test_make_align_hook_raises_without_merged(tmp_path):
    class _Cfg:
        def file(self, session, kind, create=False):
            return tmp_path / f"{session}-{kind}.json"

    hook = ap.make_align_hook(lambda *a, **k: (0, 0, 0))
    with pytest.raises(FileNotFoundError):
        hook(_Cfg(), "21001", object())


def test_make_align_hook_sources_richest_doc(tmp_path, monkeypatch):
    """Part A: align reads the published (NER-bearing) doc when present, not the
    bare merged cache, so its output keeps NER and is not a publish demotion."""
    import json
    from types import SimpleNamespace

    class _Cfg:
        def file(self, session, kind, create=False):
            return tmp_path / f"{session}-{kind}.json"

    cfg = _Cfg()
    cfg.file("21001", "merged").write_text(
        json.dumps({"meta": {}, "data": [{"id": "from-merged"}]}), encoding="utf-8")
    cfg.file("21001", "processed").write_text(
        json.dumps({"meta": {}, "data": [{"id": "from-processed"}]}), encoding="utf-8")

    captured = {}

    def fake_prepare(data, cachedir, *, force=False):
        captured["data"] = data
        return (0, 0, 0)

    monkeypatch.setattr(ap, "align_audio", lambda *a, **k: None)
    hook = ap.make_align_hook(fake_prepare)
    args = SimpleNamespace(cache_dir=tmp_path, force=False, aeneas_language="de",
                           align_timeout=1200, align_max_audio_seconds=2400)
    hook(cfg, "21001", args)

    assert captured["data"][0]["id"] == "from-processed"


# --------------------------------------------------------------------------- #
# Slice window records
# --------------------------------------------------------------------------- #

def _window_run(tmp_path, start, duration, *, calls, force=False):
    def extract(sp):
        return SpeechAudio("u", start=start, duration=duration, session_key="K")

    def download(url, target, *, required_duration=0.0):
        calls.append(("download",))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("session")

    def slice_fn(source, s, d, out):
        calls.append(("slice", source.name, s, d))
        out.write_text(f"{s}+{d}")

    return prepare_per_speech_audio([_speech(0)], tmp_path, force=force, extract=extract,
                                    download_session=download, slice_fn=slice_fn)


def test_slice_gets_a_window_record_and_is_reused(tmp_path):
    calls = []
    _window_run(tmp_path, 100.0, 300.0, calls=calls)
    record = ap._read_window(_target(tmp_path, 0))
    assert record == {"session": md5_key("K"), "start": 100.0, "duration": 300.0}
    calls.clear()
    assert _window_run(tmp_path, 100.0, 300.0, calls=calls) == (0, 1, 0)
    assert calls == []


def test_shrunk_window_is_trimmed_from_the_old_slice(tmp_path):
    calls = []
    _window_run(tmp_path, 100.0, 5500.0, calls=calls)
    calls.clear()
    assert _window_run(tmp_path, 100.0, 334.0, calls=calls) == (1, 0, 0)
    # Cut from the cached slice itself; no session download.
    assert calls == [("slice", "190540.mp3", 0.0, 334.0)]
    assert ap._read_window(_target(tmp_path, 0))["duration"] == 334.0


def test_moved_window_is_resliced_from_the_session(tmp_path):
    calls = []
    _window_run(tmp_path, 100.0, 300.0, calls=calls)
    calls.clear()
    assert _window_run(tmp_path, 380.0, 300.0, calls=calls) == (1, 0, 0)
    assert calls[0] == ("download",)
    assert calls[1][0] == "slice" and calls[1][2:] == (380.0, 300.0)


def test_legacy_slice_longer_than_its_window_is_resliced(tmp_path, monkeypatch):
    _target(tmp_path, 0).parent.mkdir(parents=True)
    _target(tmp_path, 0).write_text("legacy")
    monkeypatch.setattr(ap, "_duration_seconds", lambda path: 5535.0)
    calls = []
    assert _window_run(tmp_path, 100.0, 334.0, calls=calls) == (1, 0, 0)
    assert calls[0] == ("download",)


def test_legacy_slice_matching_its_window_is_adopted(tmp_path, monkeypatch):
    _target(tmp_path, 0).parent.mkdir(parents=True)
    _target(tmp_path, 0).write_text("legacy")
    monkeypatch.setattr(ap, "_duration_seconds", lambda path: 334.04)
    calls = []
    assert _window_run(tmp_path, 100.0, 334.0, calls=calls) == (0, 1, 0)
    assert calls == []
    assert ap._read_window(_target(tmp_path, 0))["start"] == 100.0


def test_force_reuses_slice_with_matching_window(tmp_path):
    calls = []
    _window_run(tmp_path, 100.0, 300.0, calls=calls)
    calls.clear()
    assert _window_run(tmp_path, 100.0, 300.0, calls=calls, force=True) == (0, 1, 0)
    assert calls == []                      # no session download, no re-slice


def test_force_reslices_a_changed_window_from_the_session(tmp_path):
    calls = []
    _window_run(tmp_path, 100.0, 300.0, calls=calls)
    calls.clear()
    assert _window_run(tmp_path, 100.0, 250.0, calls=calls, force=True) == (1, 0, 0)
    assert calls[0] == ("download",)


# --------------------------------------------------------------------------- #
# download_ffmpeg resume (fake ffmpeg: a file's text is its duration in s)
# --------------------------------------------------------------------------- #

def _fake_ffmpeg(monkeypatch, chunks):
    """Each extract writes the next chunk's duration; returns the start offsets."""
    starts, chunks = [], list(chunks)

    def extract(url, dest, *, start_s=0.0, **kw):
        starts.append(start_s)
        dest.write_text(str(chunks.pop(0)))

    def duration(path):
        return float(path.read_text()) if path.exists() and path.stat().st_size else 0.0

    def concat(parts, out):
        out.write_text(str(sum(float(p.read_text()) for p in parts)))

    monkeypatch.setattr(ap, "_ffmpeg_extract", extract)
    monkeypatch.setattr(ap, "_duration_seconds", duration)
    monkeypatch.setattr(ap, "_concat_mp3", concat)
    return starts


def test_truncated_download_resumes_in_the_same_call(tmp_path, monkeypatch):
    starts = _fake_ffmpeg(monkeypatch, [3000, 4000, 4000])
    target = tmp_path / "s.mp3"
    ap.download_ffmpeg("u", target, required_duration=10800)
    assert starts == [0.0, 3000.0, 7000.0]
    assert float(target.read_text()) == 11000
    assert [p.name for p in tmp_path.iterdir()] == ["s.mp3"]


def test_resume_keeps_progress_for_the_next_run(tmp_path, monkeypatch):
    starts = _fake_ffmpeg(monkeypatch, [1000, 1000, 1000, 1000, 8000])
    target = tmp_path / "s.mp3"
    with pytest.raises(RuntimeError, match="too short"):
        ap.download_ffmpeg("u", target, required_duration=10000)
    assert float(ap._part_path(target).read_text()) == 4000
    ap.download_ffmpeg("u", target, required_duration=10000)
    assert starts[4] == 4000.0              # continues from the folded progress
    assert target.exists() and not ap._part_path(target).exists()


def test_resume_stops_when_a_continuation_adds_nothing(tmp_path, monkeypatch):
    starts = _fake_ffmpeg(monkeypatch, [3000, 0])
    target = tmp_path / "s.mp3"
    with pytest.raises(RuntimeError, match="too short"):
        ap.download_ffmpeg("u", target, required_duration=10000)
    assert starts == [0.0, 3000.0]
    assert not ap._sidecar_path(target, "tail_cont").exists()


def test_leftover_continuation_is_folded_not_reused(tmp_path, monkeypatch):
    # A short tail_cont from an older run used to be appended as-is forever.
    starts = _fake_ffmpeg(monkeypatch, [5000])
    target = tmp_path / "s.mp3"
    ap._part_path(target).write_text("3000")
    ap._sidecar_path(target, "tail_cont").write_text("2000")
    ap.download_ffmpeg("u", target, required_duration=10000)
    assert starts == [5000.0]
    assert float(target.read_text()) == 10000


def test_short_download_without_required_duration_is_not_resumed(tmp_path, monkeypatch):
    starts = _fake_ffmpeg(monkeypatch, [0.5])
    with pytest.raises(RuntimeError, match="too short"):
        ap.download_ffmpeg("u", tmp_path / "s.mp3")
    assert starts == [0.0]


# --------------------------------------------------------------------------- #
# HLS gap guard (fake ffmpeg run: its stderr, and an output whose text is its
# duration in s)
# --------------------------------------------------------------------------- #

_HLS_HEADER = ("Input #0, hls, from 'u':\n"
               "  Duration: 01:00:00.00, start: 1.500000, bitrate: N/A\n")


def _fake_run(monkeypatch, stderr: str, out_duration: float) -> list:
    cmds = []

    def run(cmd, **kw):
        cmds.append(cmd)
        Path(cmd[-1]).write_text(str(out_duration))
        return subprocess.CompletedProcess(cmd, 0, b"", stderr.encode())

    monkeypatch.setattr(ap.subprocess, "run", run)
    monkeypatch.setattr(ap, "_duration_seconds",
                        lambda path: float(path.read_text()) if path.exists() else 0.0)
    return cmds


def _extract(dest: Path, *, hls: bool = True, start_s: float = 0.0) -> None:
    ap._ffmpeg_extract("u", dest, hls=hls, sample_rate=22050, bitrate="64k",
                       reconnect=True, timeout=60, start_s=start_s)


def test_fetch_is_paced_only_when_a_readrate_is_given(tmp_path, monkeypatch):
    cmds = _fake_run(monkeypatch, _HLS_HEADER, 3600)
    _extract(tmp_path / "a.mp3")
    ap._ffmpeg_extract("u", tmp_path / "b.mp3", hls=True, sample_rate=22050, bitrate="64k",
                       reconnect=True, timeout=60, readrate=10)
    assert "-readrate" not in cmds[0]
    paced = cmds[1]
    assert paced[paced.index("-readrate") + 1] == "10"
    assert paced.index("-readrate") < paced.index("-i")          # an input option


def test_download_ffmpeg_passes_the_readrate_on(tmp_path, monkeypatch):
    seen = {}

    def extract(url, dest, **opts):
        seen.update(opts)
        dest.write_text("x")

    monkeypatch.setattr(ap, "_ffmpeg_extract", extract)
    monkeypatch.setattr(ap, "_duration_seconds", lambda path: 60.0)
    ap.download_ffmpeg("u", tmp_path / "t.mp3", hls=True, readrate=10)
    assert seen["readrate"] == 10
    ap.download_ffmpeg("u", tmp_path / "t2.mp3", hls=True)
    assert seen["readrate"] is None


def test_hls_fetch_retries_segments_and_logs_the_input_duration(tmp_path, monkeypatch):
    cmds = _fake_run(monkeypatch, _HLS_HEADER, 3600)
    _extract(tmp_path / "s.mp3")
    cmd = cmds[0]
    assert cmd[cmd.index("-seg_max_retry") + 1] == "5"
    assert cmd.index("-seg_max_retry") < cmd.index("-i")
    assert cmd[cmd.index("-loglevel") + 1] == "info"


def test_non_hls_fetch_is_not_gap_checked(tmp_path, monkeypatch):
    cmds = _fake_run(monkeypatch, "", 10)
    _extract(tmp_path / "s.mp3", hls=False)
    assert "-seg_max_retry" not in cmds[0]
    assert (tmp_path / "s.mp3").exists()


def test_hls_skipped_segment_raises_and_drops_the_output(tmp_path, monkeypatch):
    _fake_run(monkeypatch, _HLS_HEADER
              + "[hls @ 0x1] Failed to open segment 3509 of playlist 0\n"
              + "[hls @ 0x1] Segment 3509 of playlist 0 failed too many times, skipping\n",
              3596)
    dest = tmp_path / "s.mp3"
    with pytest.raises(ap.HLSGapError, match="first 3509"):
        _extract(dest)
    assert not dest.exists()


def test_hls_segment_that_opens_on_a_retry_is_accepted(tmp_path, monkeypatch):
    _fake_run(monkeypatch, _HLS_HEADER
              + "[hls @ 0x1] Failed to open segment 12 of playlist 0\n", 3599)
    dest = tmp_path / "s.mp3"
    _extract(dest)
    assert dest.exists()


def test_hls_result_short_of_the_playlist_raises(tmp_path, monkeypatch):
    # A segment lost mid-read logs nothing; only the length shows the hole.
    _fake_run(monkeypatch, _HLS_HEADER, 3290)
    dest = tmp_path / "s.mp3"
    with pytest.raises(ap.HLSGapError, match="segments were lost"):
        _extract(dest)
    assert not dest.exists()


def test_hls_continuation_is_measured_from_its_start(tmp_path, monkeypatch):
    _fake_run(monkeypatch, _HLS_HEADER, 600)
    _extract(tmp_path / "s.mp3", start_s=3000.0)
    assert (tmp_path / "s.mp3").exists()


def _gap_extract(monkeypatch) -> list:
    starts = []

    def extract(url, dest, *, start_s=0.0, **kw):
        starts.append(start_s)
        raise ap.HLSGapError("hole")

    monkeypatch.setattr(ap, "_ffmpeg_extract", extract)
    monkeypatch.setattr(ap, "_duration_seconds",
                        lambda path: float(path.read_text()) if path.exists() else 0.0)
    return starts


def test_hls_gap_in_a_fresh_download_is_not_resumed(tmp_path, monkeypatch):
    starts = _gap_extract(monkeypatch)
    with pytest.raises(ap.HLSGapError):
        ap.download_ffmpeg("u", tmp_path / "s.mp3", required_duration=10000, hls=True)
    assert starts == [0.0]
    assert list(tmp_path.iterdir()) == []


def test_hls_gap_in_a_continuation_keeps_the_earlier_progress(tmp_path, monkeypatch):
    starts = _gap_extract(monkeypatch)
    target = tmp_path / "s.mp3"
    ap._part_path(target).write_text("4000")    # left by a killed run
    with pytest.raises(ap.HLSGapError):
        ap.download_ffmpeg("u", target, required_duration=10000, hls=True)
    assert starts == [4000.0]
    assert float(ap._part_path(target).read_text()) == 4000
