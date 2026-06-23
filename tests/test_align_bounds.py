"""Unit test for the aligner's output-side bound check.

align_audio itself needs aeneas + real audio (not exercised by this suite), so
the drop-decision is factored into a pure predicate that we can pin here. This
guards the DE-0210037079 class of defect: an aligned end far beyond the
published clip duration (aligned audio longer than the trimmed CDN clip).
"""

from __future__ import annotations

from optv.shared.align import aligned_end_out_of_bounds


def test_in_bounds_end_is_accepted():
    # Reproduced 21037 case on today's trimmed 21s clip: max end 20.96s.
    assert aligned_end_out_of_bounds(20.96, 21) is False


def test_out_of_bounds_end_is_flagged():
    # Live DE-0210037079 defect: 57.96s aligned end on a 21s clip.
    assert aligned_end_out_of_bounds(57.96, 21) is True


def test_tail_rounding_within_tolerance_is_accepted():
    # 1.1x + 1s tolerance absorbs aeneas tail-boundary rounding.
    assert aligned_end_out_of_bounds(22.0, 21) is False
    assert aligned_end_out_of_bounds(24.2, 21) is True


def test_unknown_duration_never_flags():
    assert aligned_end_out_of_bounds(9999.0, 0) is False
    assert aligned_end_out_of_bounds(9999.0, None) is False
