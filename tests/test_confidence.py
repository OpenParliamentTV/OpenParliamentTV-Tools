"""Shared media↔proceedings confidence gate (optv/shared/confidence.py)."""

import pytest

from optv.shared.confidence import compute_confidence, DEFAULT_CPS_CAP_TYPES


def test_clean_speech_keeps_confidence_1():
    # Normal debate text on a clip that can physically hold it.
    conf, reason = compute_confidence("regular", chars=3000, duration=180,
                                      cps_floor=8000)
    assert conf == 1.0
    assert reason is None


def test_cps_cap_fires_when_text_too_dense_for_clip():
    # 17 900 chars on a 124 s clip = ~144 cps (humans ~16) → mis-merge.
    conf, reason = compute_confidence("regular", chars=17900, duration=124,
                                      cps_floor=8000)
    assert conf == 0.5
    assert reason == "cps-cap"


def test_cps_cap_respects_floor():
    # Above the cps cap but below the char floor → not gated (short clip, short text).
    conf, reason = compute_confidence("regular", chars=5000, duration=20,
                                      cps_floor=8000)
    assert conf == 1.0
    assert reason is None


def test_cps_cap_only_for_listed_types():
    # Procedural carries long chair text on a short clip legitimately.
    conf, reason = compute_confidence("procedural", chars=20000, duration=60,
                                      cps_floor=8000)
    assert conf == 1.0
    assert reason is None


def test_blanket_type_gates_regardless_of_length():
    conf, reason = compute_confidence("qa", chars=100, duration=600,
                                      blanket_types=frozenset({"qa"}),
                                      cps_floor=8000)
    assert conf == 0.5
    assert reason == "blanket-type"


def test_empty_blanket_set_keeps_qa_confident():
    # AT default: Q&A is gated only by cps-cap, not blanket-suppressed.
    conf, reason = compute_confidence("qa", chars=5000, duration=600,
                                      blanket_types=frozenset(), cps_floor=8000)
    assert conf == 1.0
    assert reason is None


def test_missing_duration_does_not_gate():
    conf, reason = compute_confidence("regular", chars=50000, duration=None,
                                      cps_floor=8000)
    assert conf == 1.0
    assert reason is None


def test_questioning_is_a_cps_cap_type():
    assert "questioning_of_the_government" in DEFAULT_CPS_CAP_TYPES
