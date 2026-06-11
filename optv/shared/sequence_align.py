#! /usr/bin/env python3
"""Equal-key Needleman-Wunsch alignment shared across the parliament mergers.

The two-source mergers whose join is "match two speaker sequences on a robust
key" (surname for members, canonical role for the chair/government) share this
one bordered, equal-key Needleman-Wunsch: a diagonal step only "matches" when the
two keys are equal and non-empty. Used by PT, ES, and the whole PDF tier (the six
PDF Landtage via ``optv/shared/pdf2tei/spine_join.py``).

Only the parliament-agnostic mechanics live here; everything structural — how the
keys are built from a media/proceedings item, and what the caller does with the
matched pairs (PT keeps a 1:1 mapping, ES absorbs same-surname neighbours) — stays
in each merger. Parliaments whose join is genuinely different keep their own
implementation: the Bundestag (DE) needs a weighted speaker+title score with a
seeded matrix, a raw-neighbour backtrack and an unmatched-clip tail, so that NW
stays private in ``optv/parliaments/DE/merger/merge_session.py``.
"""

from __future__ import annotations


def align_equal_keys(a_keys: list[str], b_keys: list[str],
                     *, match: int = 2, mismatch: int = -1, gap: int = -1
                     ) -> list[tuple[int, int]]:
    """Global alignment of two key sequences; return matched ``(i, j)`` pairs.

    Classic bordered Needleman-Wunsch. A diagonal step scores ``match`` only
    when ``a_keys[i] == b_keys[j]`` and the key is non-empty, otherwise
    ``mismatch``; gaps score ``gap``. The returned list contains exactly the
    diagonal steps whose keys were equal, in ascending order — i.e. the speeches
    the two streams agree on. Unmatched ``a`` rows and surplus ``b`` turns are
    simply omitted (they fall on gap steps).

    PT builds ``{i: j}`` from this; ES builds ``{i: [j, …]}`` and then absorbs
    same-surname neighbours into each match.
    """
    m, n = len(a_keys), len(b_keys)
    if m == 0 or n == 0:
        return []
    score = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        score[i][0] = i * gap
    for j in range(1, n + 1):
        score[0][j] = j * gap
    for i in range(1, m + 1):
        ai = a_keys[i - 1]
        for j in range(1, n + 1):
            equal = bool(ai) and ai == b_keys[j - 1]
            diag = score[i - 1][j - 1] + (match if equal else mismatch)
            score[i][j] = max(diag, score[i - 1][j] + gap, score[i][j - 1] + gap)

    matches: list[tuple[int, int]] = []
    i, j = m, n
    while i > 0 and j > 0:
        ai = a_keys[i - 1]
        equal = bool(ai) and ai == b_keys[j - 1]
        if score[i][j] == score[i - 1][j - 1] + (match if equal else mismatch):
            if equal:
                matches.append((i - 1, j - 1))
            i, j = i - 1, j - 1
        elif score[i][j] == score[i - 1][j] + gap:
            i -= 1
        else:
            j -= 1
    matches.reverse()
    return matches
