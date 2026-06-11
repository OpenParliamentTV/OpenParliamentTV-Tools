"""DE-NW (Landtag Nordrhein-Westfalen) PDF→TEI detection config (lifted from prototype).

NW numbers every agenda item and lays the TOC out in two columns, so the TOC is
``indented`` with ``toc_strip_leading_num``. The spine is clean 1:1, so the
rede-merge is ``chain=False``.
"""
from __future__ import annotations

import re

from optv.shared.pdf2tei.config import ParliamentConfig
from optv.shared.lang.de import FACTIONS

CONFIG = ParliamentConfig(
    parliament_id="DE-NW",
    # A "*)" footnote marker often sits between the name and the faction
    # ("Dagmar Hanses*) (GRÜNE):") — the name class excludes it and an optional
    # "\*\)" consumes it, else those turns are dropped.
    mp_speaker=re.compile(
        r"^(?P<name>[A-ZÄÖÜ][^():*]+?)(?:\*\))?\s+\((?P<faction>CDU|SPD|GRÜNE|"
        r"FDP|AfD|fraktionslos)\)\s*:\s*(?P<rest>.+)$"),
    gov_speaker=re.compile(
        r"^(?P<name>[A-ZÄÖÜ][^,:]+?),\s*"
        r"(?P<role>Ministerpräsidentin|Ministerpräsident|Ministerin|Minister|"
        r"Staatssekretärin|Staatssekretär|Staatsministerin|Staatsminister)"
        r"[^:]*:\s*(?P<rest>.*)$"),
    chair_speaker=re.compile(
        r"^(?P<role>(?:Erste[r]?|Zweite[r]?|Dritte[r]?)\s+Vizepräsident(?:in)?|"
        r"Vizepräsident(?:in)?|Präsident(?:in)?|Alterspräsident(?:in)?)\s+"
        r"(?P<name>[A-ZÄÖÜ][^:]+?)\s*:\s*(?P<rest>.*)$"),
    top_announce=None,  # rely on TOC anchoring.
    known_factions=FACTIONS,
    speaker_inline=True,
    toc_layout="indented",
    toc_strip_leading_num=True,
    toc_noise=re.compile(
        r"^(Aktuelle Stunde|Antrag|Antr[äa]ge|Gesetz|Gesetzentwurf|In Verbindung|"
        r"Drucksache|Ergebnis|Beschlussempfehlung|Wahl |Mitteilung|Vor Eintritt|"
        r"Vereidigung|Erste Lesung|Zweite Lesung|Dritte Lesung|Große Anfrage|"
        r"Kleine Anfrage|Antwort|Bericht|Neudruck|Zur |Damit|und der|"
        r"Entschließungsantrag|Änderungsantrag)\b", re.IGNORECASE),
    merge_chain=False,
)
