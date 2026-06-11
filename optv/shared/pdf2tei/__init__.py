"""Generic PDF → ParlaMint-TEI conversion for parliaments published only as PDF.

Parliament-agnostic core, promoted from the ``_planning/pdf2tei`` prototype:

- :mod:`optv.shared.pdf2tei.backends` — PDF text extraction (PyMuPDF).
- :mod:`optv.shared.pdf2tei.common` — block text cleanup + I/O.
- :mod:`optv.shared.pdf2tei.toc` — front-matter table-of-contents anchoring.
- :mod:`optv.shared.pdf2tei.pdf2tei` — block → TEI builder (takes a
  :class:`~optv.shared.pdf2tei.config.ParliamentConfig`).
- :mod:`optv.shared.pdf2tei.tei2json` — TEI → proceedings-JSON reader.

All German-language knowledge (month names, incident/result keywords, running-
header and TOC heuristics, faction tables) lives in :mod:`optv.shared.lang.de`;
per-parliament regex/layout config lives in each parliament module's
``parsers/pdf_config.py`` and is resolved by
:func:`optv.shared.pdf2tei.config.load_config`.
"""
