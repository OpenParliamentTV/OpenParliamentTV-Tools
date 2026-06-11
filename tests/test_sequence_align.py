"""Contract tests for the shared equal-key Needleman-Wunsch aligner.

The PT and ES mergers (and the PDF tier) share ``align_equal_keys``. This test
pins it against the bespoke implementations it replaced, so a future change can't
silently shift a live parliament's alignment. The reference implementation below
is a verbatim copy of the pre-refactor merger code. (DE keeps its own weighted-
path NW private in its merger — it is not part of the shared aligner.)
"""

from __future__ import annotations

import random

from optv.shared.sequence_align import align_equal_keys


# --------------------------------------------------------------------------- #
# Reference (pre-refactor) implementations
# --------------------------------------------------------------------------- #

def _ref_equal_keys(a, b, match, mismatch, gap):
    m, n = len(a), len(b)
    if m == 0 or n == 0:
        return []
    score = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        score[i][0] = i * gap
    for j in range(1, n + 1):
        score[0][j] = j * gap
    for i in range(1, m + 1):
        ai = a[i - 1]
        for j in range(1, n + 1):
            equal = bool(ai) and ai == b[j - 1]
            diag = score[i - 1][j - 1] + (match if equal else mismatch)
            score[i][j] = max(diag, score[i - 1][j] + gap, score[i][j - 1] + gap)
    out = []
    i, j = m, n
    while i > 0 and j > 0:
        ai = a[i - 1]
        equal = bool(ai) and ai == b[j - 1]
        if score[i][j] == score[i - 1][j - 1] + (match if equal else mismatch):
            if equal:
                out.append((i - 1, j - 1))
            i, j = i - 1, j - 1
        elif score[i][j] == score[i - 1][j] + gap:
            i -= 1
        else:
            j -= 1
    out.reverse()
    return out


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #

def test_align_equal_keys_basic():
    # av spine is finer; text interleaves an extra chair turn -> still matched in order
    assert align_equal_keys(["a", "b", "c"], ["a", "x", "b", "c"]) == [(0, 0), (1, 2), (2, 3)]


def test_align_equal_keys_empty():
    assert align_equal_keys([], ["a"]) == []
    assert align_equal_keys(["a"], []) == []


def test_align_equal_keys_skips_empty_key():
    # an empty key never matches, even against another empty key
    assert align_equal_keys(["", "b"], ["", "b"]) == [(1, 1)]


def test_equal_keys_matches_reference_pt():
    random.seed(7)
    alpha = ["", "a", "b", "c", "d"]
    for _ in range(5000):
        a = [random.choice(alpha) for _ in range(random.randint(0, 7))]
        b = [random.choice(alpha) for _ in range(random.randint(0, 7))]
        assert align_equal_keys(a, b) == _ref_equal_keys(a, b, 2, -1, -1)


def test_equal_keys_matches_reference_es():
    # ES uses a high mismatch penalty; matched set must still match the reference
    random.seed(11)
    alpha = ["", "a", "b", "c"]
    for _ in range(5000):
        a = [random.choice(alpha) for _ in range(random.randint(0, 7))]
        b = [random.choice(alpha) for _ in range(random.randint(0, 7))]
        assert (align_equal_keys(a, b, match=2, mismatch=-100, gap=-1)
                == _ref_equal_keys(a, b, 2, -100, -1))
