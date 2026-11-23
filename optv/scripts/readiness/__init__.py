"""Readiness of the parliament implementations and their data.

``python -m optv.scripts.readiness check --all`` evaluates every parliament's sample sessions against the criteria in ``criteria.py`` and writes one scorecard per parliament to ``docs/readiness/<CODE>.json`` plus the overview ``docs/READINESS.md``. The declared facts (sample sessions, attestations, thresholds) live in the ``readiness`` block of each ``manifest.yaml``.

The expensive evidence, Whisper transcripts of the sample sessions, is produced by detached jobs: ``estimate`` builds a plan with its expected duration, ``start`` runs it in the background once the plan's token is passed back, ``jobs`` reports on it.
"""
