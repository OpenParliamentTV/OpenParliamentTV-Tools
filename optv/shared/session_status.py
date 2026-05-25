"""Per-session pipeline-stage flags.

``Config.status(session)`` in each parliament returns a ``set[SessionStatus]``
derived by peeking at the session's published file. Shared workflow stages
read these flags to decide whether to skip a session (e.g.
``SessionStatus.aligned in status``) or to refuse a demoting publish.
"""

from enum import Enum, auto


class SessionStatus(Enum):
    media = auto()
    proceedings = auto()
    merged = auto()
    aligned = auto()   # Time alignment info is present
    linked = auto()    # Wikidata id for people/factions is present
    ner = auto()       # Entities have been extracted from proceedings text
    session = auto()
    empty = auto()
    no_text = auto()
