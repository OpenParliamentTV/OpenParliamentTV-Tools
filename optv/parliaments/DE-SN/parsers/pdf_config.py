"""DE-SN (Sächsischer Landtag) PDF→TEI detection config (lifted from prototype)."""
from __future__ import annotations

import re

from optv.shared.pdf2tei.config import ParliamentConfig
from optv.shared.lang.de import FACTIONS

CONFIG = ParliamentConfig(
    parliament_id="DE-SN",
    mp_speaker=re.compile(
        r"^(?P<name>[A-ZÄÖÜ][^,:\(\[]+?)\s*,\s*"
        r"(?P<faction>CDU|SPD|AfD|BSW|BÜNDNISGRÜNE|BÜNDNISGRUNEN|BÜNDNIS ?90/DIE GRÜNEN|"
        r"GRÜNE|DIE LINKE|Die Linke|FDP)\s*:\s*(?P<rest>.*)$"),
    gov_speaker=re.compile(
        r"^(?P<name>[A-ZÄÖÜ][^,:]+?)\s*,\s*"
        r"(?P<role>Staatsministerin|Staatsminister|Staatssekretärin|Staatssekretär|"
        r"Ministerpräsidentin|Ministerpräsident|Ministerin|Minister)[^:]*\s*:\s*(?P<rest>.*)$"),
    chair_speaker=re.compile(
        r"^(?P<role>(?:Erste|Zweite|Dritte|Vierte|Fünfte|Sechste)\s+Vizepräsidentin|"
        r"(?:Erster|Zweiter|Dritter|Vierter|Fünfter|Sechster)\s+Vizepräsident|"
        r"Vizepräsidentin|Vizepräsident|Präsidentin|Präsident|Alterspräsidentin|Alterspräsident)\s+"
        r"(?P<name>[^:]+?)\s*:\s*(?P<rest>.*)$"),
    # DE-SN often carries thin/noisy body headlines; rely on TOC anchoring.
    top_announce=None,
    known_factions=FACTIONS,
    speaker_inline=True,
    toc_noise=re.compile(
        r"^(Antrag|Antr[äa]ge|Alternativantrag|Gesetzentwurf|Beschluss|Drucksache|"
        r"Wahlvorschlag|Bericht der|Bericht und|Änderungsantrag|Entschließungsantrag|"
        r"Mündliche Anfrage|Beschlussempfehlung|Befragung des Staatsministers|"
        r"Befragung der Staatsregierung|Fragestunde)\b", re.IGNORECASE),
    merge_chain=False,
)
