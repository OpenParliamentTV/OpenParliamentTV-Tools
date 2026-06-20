"""AT merger: trailing-handoff trim + confidence/linkedMediaIndexes gate."""

from __future__ import annotations

import importlib

_merger = importlib.import_module("optv.parliaments.AT.merger.merge_session")


def _block(pad, text):
    return {"speakerID": pad, "speaker": f"P{pad}", "speakerstatus": "role",
            "sentences": [{"text": text}], "text": text}


def _speech_with_blocks(pad_blocks, people_pads):
    """pad_blocks: list of (pad, text); people_pads: ordered PADs for people[]."""
    return {
        "people": [{"originPersonID": p, "label": f"P{p}"} for p in people_pads],
        "textContents": [{"textBody": [_block(p, t) for p, t in pad_blocks]}],
    }


# --------------------------------------------------------------------------- #
# Trailing-handoff trim
# --------------------------------------------------------------------------- #

def test_trim_drops_trailing_chair_handoff():
    # speaker "1" ... chair "9" handoff at the end → drop the chair tail only.
    sp = _speech_with_blocks(
        [("1", "Meine Damen und Herren"), ("1", "Zur Sache"),
         ("9", "Zu Wort gelangt der nächste Redner.")],
        people_pads=["1", "9"])
    removed = _merger._trim_trailing_handoff(sp, "1")
    assert removed == 1
    bodies = [b["speakerID"] for b in sp["textContents"][0]["textBody"]]
    assert bodies == ["1", "1"]
    # chair, now unreferenced, drops out of people[]
    assert [p["originPersonID"] for p in sp["people"]] == ["1"]


def test_trim_keeps_interior_interruptions():
    # chair interrupts mid-speech and the speaker continues → keep everything.
    sp = _speech_with_blocks(
        [("1", "Beginn"), ("9", "Bitte, Herr Abgeordneter."), ("1", "Fortsetzung"),
         ("9", "Zu Wort gelangt der nächste Redner.")],
        people_pads=["1", "9"])
    removed = _merger._trim_trailing_handoff(sp, "1")
    assert removed == 1  # only the final chair handoff
    bodies = [b["speakerID"] for b in sp["textContents"][0]["textBody"]]
    assert bodies == ["1", "9", "1"]  # interior chair interruption preserved


def test_trim_noop_when_speaker_is_last():
    sp = _speech_with_blocks([("9", "Intro"), ("1", "Rede")], people_pads=["1", "9"])
    assert _merger._trim_trailing_handoff(sp, "1") == 0
    assert len(sp["textContents"][0]["textBody"]) == 2  # leading chair kept


def test_trim_noop_on_pad_mismatch():
    # On-camera speaker never authors a block → keep text verbatim, no guessing.
    sp = _speech_with_blocks([("7", "A"), ("8", "B")], people_pads=["7", "8"])
    assert _merger._trim_trailing_handoff(sp, "1") == 0
    assert len(sp["textContents"][0]["textBody"]) == 2


# --------------------------------------------------------------------------- #
# apply_gate: confidence + linkedMediaIndexes
# --------------------------------------------------------------------------- #

def _text_speech(speech_index, std_id, agenda_type, chars, duration):
    n = max(chars // 5, 1)
    body = [{"sentences": [{"text": "x" * 5} for _ in range(n)]}]
    return {
        "speechIndex": speech_index,
        "agendaItem": {"type": agenda_type},
        "media": {"duration": duration},
        "textContents": [{"textBody": body}],
        "debug": {"stdId": std_id},
    }


def test_apply_gate_clean_speech():
    s = _text_speech(1, std_id=100, agenda_type="regular", chars=3000, duration=180)
    _merger.apply_gate([s])
    assert s["debug"]["confidence"] == 1.0
    assert s["debug"]["linkedMediaIndexes"] == [1]
    assert "confidenceReason" not in s["debug"]


def test_apply_gate_cps_cap():
    s = _text_speech(70, std_id=319278, agenda_type="regular", chars=17900, duration=124)
    _merger.apply_gate([s])
    assert s["debug"]["confidence"] == 0.5
    assert s["debug"]["confidenceReason"] == "cps-cap"
    assert s["debug"]["linkedMediaIndexes"] == [70]


def test_apply_gate_qa_not_blanket_by_default():
    # AT default: Q&A keeps confidence 1 unless cps-cap fires.
    s = _text_speech(5, std_id=200, agenda_type="qa", chars=5000, duration=600)
    _merger.apply_gate([s])
    assert s["debug"]["confidence"] == 1.0


def test_apply_gate_shared_stdid_links_multiple():
    a = _text_speech(3, std_id=500, agenda_type="regular", chars=1000, duration=120)
    b = _text_speech(4, std_id=500, agenda_type="regular", chars=1000, duration=120)
    _merger.apply_gate([a, b])
    assert a["debug"]["linkedMediaIndexes"] == [3, 4]
    assert b["debug"]["linkedMediaIndexes"] == [3, 4]  # count>1 → text gated downstream


def test_apply_gate_skips_media_only():
    s = {"speechIndex": 9, "media": {"duration": 60}, "textContents": [],
         "debug": {"stdId": 1}}
    _merger.apply_gate([s])
    assert "confidence" not in s["debug"]
    assert "linkedMediaIndexes" not in s["debug"]
