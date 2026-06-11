"""DE-HH (Hamburgische Bürgerschaft) PDF→TEI detection config (lifted from prototype).

Hamburg lists government members with Bürgermeister/Senator roles and MPs with a
trailing bare faction; the spine is clean, so the rede-merge is ``chain=False``.
"""
from __future__ import annotations

import re

from optv.shared.pdf2tei.config import ParliamentConfig
from optv.shared.lang.de import FACTIONS

CONFIG = ParliamentConfig(
    parliament_id="DE-HH",
    # The name must NOT begin with a procedural keyword — else TOC header lines
    # ("Antrag der Fraktion Die Linke:") parse as a speaker.
    mp_speaker=re.compile(
        r"^(?P<name>(?!(?:Antrag|Antr[äa]ge|Fraktion|Große|Kleine|Schriftliche|"
        r"Drucksache|Bericht|Senatsmitteilung|Senatsantrag|Unterrichtung|Mitteilung|"
        r"Wahl|Neufassung|Zusatzantrag|Beschlussempfehlung|Dringliche|Gesetz)\b)"
        r"[A-ZÄÖÜ][A-Za-zÄÖÜäöüß.'\- ]+?)\s+"
        r"(?P<faction>GRÜNE|SPD|CDU|AfD|Die Linke|FDP|fraktionslos)"
        r"(?:\s*\((?:fortfahrend|unterbrechend)\))?\s*:\s*(?P<rest>.*)$"),
    gov_speaker=re.compile(
        r"^(?P<role>Erste[r]?\s+Bürgermeister(?:in)?|Zweite[r]?\s+Bürgermeister(?:in)?|"
        r"Bürgermeister(?:in)?|Senator(?:in)?|Staatsrätin|Staatsrat)\s+"
        r"(?P<name>[A-ZÄÖÜ][A-Za-zÄÖÜäöüß.'\- ]+?)"
        r"(?:\s*\((?:fortfahrend|unterbrechend)\))?\s*:\s*(?P<rest>.*)$"),
    chair_speaker=re.compile(
        r"^(?P<role>(?:Erste[r]?|Zweite[r]?|Dritte[r]?)\s+Vizepräsident(?:in)?|"
        r"Vizepräsident(?:in)?|Präsident(?:in)?|Alterspräsident(?:in)?)\s+"
        r"(?P<name>[A-ZÄÖÜ][A-Za-zÄÖÜäöüß.'\- ]+?)"
        r"(?:\s*\((?:fortfahrend|unterbrechend)\))?\s*:\s*(?P<rest>.*)$"),
    top_announce=None,  # rely on TOC anchoring.
    known_factions=FACTIONS,
    speaker_inline=True,
    toc_noise=re.compile(
        r"^(Antrag|Antr[äa]ge|Große Anfrage|Kleine Anfrage|Schriftliche|"
        r"Senatsantrag|Senatsmitteilung|Unterrichtung|Mitteilung|Wahl |Drs\b|"
        r"Drucksache|Bericht des|Bericht der|Beschlussempfehlung|Änderungsantrag|"
        r"Entschließungsantrag|Neufassung|Zusatzantrag|Interfraktionell|"
        r"Erste Lesung|Zweite Lesung|gemäß|und\b)", re.IGNORECASE),
    merge_chain=False,
)
