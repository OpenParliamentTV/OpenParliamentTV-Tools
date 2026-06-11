"""DE-BY (Bayerischer Landtag) PDF→TEI detection config.

Lifted from the prototype. Bavaria prints inline speaker labels
("Name (CSU): …"), ministers role-first with the portfolio in parens, and an
indented TOC. The spine splits each speaker's turn into its own clip, so the
rede-merge is ``chain=False`` (one clip = chair intro + one speaker).
"""
from __future__ import annotations

import re

from optv.shared.pdf2tei.config import ParliamentConfig
from optv.shared.lang.de import FACTIONS

CONFIG = ParliamentConfig(
    parliament_id="DE-BY",
    mp_speaker=re.compile(
        r"^(?P<name>[A-ZÄÖÜ][^:(]+?)\s+\((?P<faction>CSU|GRÜNE|SPD|AfD|FREIE WÄHLER|FDP|"
        r"fraktionslos)\):\s+(?P<rest>.+)$"),
    # BY ministers are role-FIRST with the portfolio in parens:
    #   "Staatsministerin Ulrike Scharf (Familie, Arbeit und Soziales): …"
    gov_speaker=re.compile(
        r"^(?P<role>Staatsministerin|Staatsminister|Staatssekretärin|Staatssekretär|"
        r"Ministerpräsidentin|Ministerpräsident)\s+(?P<name>[A-ZÄÖÜ][^:(]+?)"
        r"(?:\s+\([^)]*\))?:\s*(?P<rest>.+)$"),
    chair_speaker=re.compile(
        r"^(?P<role>(?:Erster|Zweiter|Dritter|Vierter)\s+Vizepräsident(?:in)?|"
        r"Vizepräsident(?:in)?|Präsident(?:in)?)\s+(?P<name>[A-ZÄÖÜ][^:]+?):\s+(?P<rest>.+)$"),
    top_announce=re.compile(
        r"\brufe\b.*?\b(?:den\s+)?Tagesordnungspunkte?\b\s+\d+", re.IGNORECASE),
    known_factions=FACTIONS,
    # BY agenda titles often ARE "Antrag/Gesetzentwurf/Dringlichkeitsantrag …",
    # so those are NOT noise here — only vote/procedure results are.
    toc_noise=re.compile(
        r"^(Beschluss|Beschlussempfehlung|Verweisung|Abstimmung|Schlussabstimmung|"
        r"Namentliche|Geheime Wahl|Einfache Wahl|Zustimmung\b|Ablehnung\b|Annahme\b|"
        r"Ergebnis\b|Änderungsantr|Änderungsanträge|Entschließungsantr|Drucksache\b|"
        r"und\b|hierzu\b|dazu\b)\b", re.IGNORECASE),
    speaker_inline=True,
    toc_layout="indented",
    merge_chain=False,   # spine splits turns: one clip = chair + one speaker
)
