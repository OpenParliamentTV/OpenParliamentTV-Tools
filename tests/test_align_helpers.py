"""Pure helpers of optv/shared/align.py."""

from __future__ import annotations

from optv.shared.align import previous_current_next


def test_previous_current_next_windows():
    assert list(previous_current_next([1, 2, 3])) == [(None, 1, 2), (1, 2, 3), (2, 3, None)]
    assert list(previous_current_next([1])) == [(None, 1, None)]


def test_previous_current_next_empty_yields_nothing():
    # A speech whose textContents entry has an empty textBody gives the
    # comment-timecode pass no bodies; this used to raise StopIteration inside
    # the generator (RuntimeError under PEP 479) and fail the whole session.
    assert list(previous_current_next([])) == []
