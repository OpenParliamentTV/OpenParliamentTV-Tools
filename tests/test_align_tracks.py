"""Per-track alignment plumbing in optv.shared.align (no aeneas, no network).

A video with several audio tracks (ES: floor audio + Spanish interpretation)
can have each textContents entry aligned against its own track. These tests
pin the grouping, the cache names, the single video download shared by all
tracks, and the coarse-duration refinement.
"""

from pathlib import Path

from optv.shared import align
from optv.parliaments.ES.workflow import audio_track as es_audio_track


def _speech(*languages, original="ca", duration=120.0, coarse=True):
    return {
        "electoralPeriod": {"number": 15}, "session": {"number": 179}, "speechIndex": 40,
        "originalLanguage": original,
        "media": {"videoFileURI": "https://static.example/clip.mp4", "duration": duration},
        "debug": {"coarseTiming": True} if coarse else {},
        "textContents": [
            {"language": lang, "textBody": [
                {"type": "speech", "sentences": [{"text": f"{lang} 1"}, {"text": f"{lang} 2"}]}]}
            for lang in languages
        ],
    }


def test_track_groups_default_is_one_pass():
    groups = align._track_groups(_speech("ca", "es"), None)
    assert [(track, len(sentences)) for track, sentences in groups] == [(None, 4)]


def test_track_groups_follow_es_mapping():
    groups = align._track_groups(_speech("ca", "es"), es_audio_track)
    assert [(track, [s["text"] for _i, s in sentences]) for track, sentences in groups] == [
        (0, ["ca 1", "ca 2"]), (1, ["es 1", "es 2"])]
    # A Spanish-only speech follows the floor audio.
    groups = align._track_groups(_speech("es", original="es"), es_audio_track)
    assert [track for track, _ in groups] == [0]


def test_track_audio_file_names(tmp_path):
    speech = _speech("es")
    assert align.track_audio_file(speech, None, tmp_path).name == "1517940.mp3"
    assert align.track_audio_file(speech, 0, tmp_path).name == "1517940.mp3"
    assert align.track_audio_file(speech, 1, tmp_path).name == "1517940.a1.mp3"


def test_stage_audio_downloads_video_once_for_all_tracks(tmp_path, monkeypatch):
    downloads, conversions = [], []

    def fake_mediafile(speech, cachedir, mediatype='audio'):
        downloads.append(mediatype)
        if mediatype != 'video':
            return None
        path = align.cachedfile(speech, 'mp4', cachedir)
        path.write_bytes(b"video")
        return path

    def fake_convert(video, audio, track=None):
        conversions.append(track)
        Path(audio).write_bytes(b"audio")
        return True

    monkeypatch.setattr(align, "mediafile", fake_mediafile)
    monkeypatch.setattr(align, "convert_video_to_audio", fake_convert)
    speech = _speech("ca", "es")
    staged = align.stage_audio(speech, tmp_path, [0, 1])
    assert downloads == ["video"]
    assert conversions == [0, 1]
    assert {t: p.name for t, (p, _cached) in staged.items()} == {
        0: "1517940.mp3", 1: "1517940.a1.mp3"}
    assert not align.cachedfile(speech, 'mp4', tmp_path).exists()
    # Second run: both tracks come from the cache.
    assert all(cached for _p, cached in align.stage_audio(speech, tmp_path, [0, 1]).values())
    assert downloads == ["video"]


def test_refine_coarse_duration(tmp_path, monkeypatch):
    clip = tmp_path / "clip.mp3"
    clip.write_bytes(b"audio")
    staged = {0: (clip, True)}
    monkeypatch.setattr(align, "_probe_duration_seconds", lambda media: 145.016)

    speech = _speech("es", original="es")
    assert align._refine_coarse_duration(speech, 120.0, staged) == 145.016
    assert speech["media"]["duration"] == 145.016

    # Exact durations, and probes too far off to be the same clip, stay.
    exact = _speech("es", original="es", coarse=False)
    assert align._refine_coarse_duration(exact, 120.0, staged) == 120.0
    far = _speech("es", original="es", duration=40.0)
    assert align._refine_coarse_duration(far, 40.0, staged) == 40.0
    assert far["media"]["duration"] == 40.0
