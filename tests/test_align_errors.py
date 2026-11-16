"""``debug.alignError`` describes the latest alignment attempt (no aeneas).

A failed run leaves ``alignError`` on the speech; when a later run aligns the
speech, the old error must not stay next to the new timings.
"""

from optv.shared import align


def _speech(error=None):
    speech = {
        "electoralPeriod": {"number": 18}, "session": {"number": 117}, "speechIndex": 2,
        "media": {"videoFileURI": "https://example/stream.m3u8#t=0,60"},
        "debug": {},
        "textContents": [{"textBody": [
            {"type": "speech", "sentences": [{"text": "Eins."}, {"text": "Zwei."}]}]}],
    }
    if error:
        speech["debug"]["alignError"] = error
    return speech


def _patch(monkeypatch, tmp_path, result):
    clip = tmp_path / "clip.mp3"
    clip.write_bytes(b"audio")
    monkeypatch.setattr(align, "stage_audio",
                        lambda speech, cachedir, tracks: {t: (clip, True) for t in tracks})

    def fake_pass(speech, sentence_list, *args, **kwargs):
        if result is None:
            speech["debug"]["alignError"] = "timeout"
            return None
        for i, (_ident, sentence) in enumerate(sentence_list):
            sentence["timeStart"], sentence["timeEnd"] = float(i), float(i + 1)
        return result

    monkeypatch.setattr(align, "_align_pass", fake_pass)


def test_successful_realignment_drops_the_old_error(tmp_path, monkeypatch):
    _patch(monkeypatch, tmp_path, 1.5)
    speech = _speech(error="exit 1")
    align.align_audio([speech], "deu", tmp_path)
    assert "alignError" not in speech["debug"]
    assert speech["debug"]["alignDuration"] == 1.5


def test_failed_realignment_reports_the_new_error(tmp_path, monkeypatch):
    _patch(monkeypatch, tmp_path, None)
    speech = _speech(error="exit 1")
    align.align_audio([speech], "deu", tmp_path)
    assert speech["debug"]["alignError"] == "timeout"
