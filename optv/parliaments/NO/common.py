#! /usr/bin/env python3
"""Config + path layout for the NO (Stortinget) pipeline."""

from optv.shared.config import BaseConfig
# The Conductor imports SessionStatus from this module in-process.
from optv.shared.session_status import SessionStatus  # noqa: F401


# Stortingsperiode (4-year term) → OPTV-internal integer period.
# We can't use the API's string period ("2025-2029") because the shared
# argparser requires ``--period`` to be an integer. The integer is opaque
# (consecutive terms get consecutive numbers); the session string and the
# manifest anchor the value, not the number itself. Referats follow the
# current ``forhandlinger.dtd`` schema from session 2016-2017 on, so the
# terms from 2017-2021 are wired.
TERM_TO_PERIOD = {
    "2017-2021": 20,
    "2021-2025": 21,
    "2025-2029": 22,
}
PERIOD_TO_TERM = {v: k for k, v in TERM_TO_PERIOD.items()}

# Parliamentary sessions (sesjoner) run October → September inside a term.
# We list each term's session-years explicitly so the download stage knows
# which `sesjonid` values to query for a given period.
TERM_TO_SESJONIDER = {
    "2017-2021": ["2017-2018", "2018-2019", "2019-2020", "2020-2021"],
    "2021-2025": ["2021-2022", "2022-2023", "2023-2024", "2024-2025"],
    "2025-2029": ["2025-2026", "2026-2027", "2027-2028", "2028-2029"],
}


def period_to_sesjonider(period: int) -> list[str]:
    term = PERIOD_TO_TERM.get(period)
    if not term:
        raise ValueError(f"Unknown NO period {period} - extend TERM_TO_PERIOD in common.py")
    return TERM_TO_SESJONIDER[term]


class Config(BaseConfig):
    # Sessions are discovered from the per-meeting ``{session}-media.json``
    # files written by the scraper; session strings have the form
    # ``{period}_{moteid}`` (e.g. ``22_11518``). A speech without a non-empty
    # textBody makes the session ``no_text``.
    TEXT_SIGNAL = "textContents"
    # Per-sesjon meeting overviews, ``meetings/{sesjonid}.json``.
    EXTRA_DIRS = {"meetings": ("data", "original/meetings")}
