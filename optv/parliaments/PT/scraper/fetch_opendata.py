#! /usr/bin/env python3
"""Download the open data *Intervenções* file and split it per reunião.

The parliament's open data portal publishes one Intervenções file per
legislatura (JSON and XML), with one record per intervention: its av video page
(``DadosAudiovisual[].url``, e.g. ``https://av.parlamento.pt/videos/Plenary/17/2/1/16``),
the Diário page range (``Publicacao[].pag``), the deputy's ``idCadastro`` and
group, and the debate (``IdDebate`` / ``Debate``). The av URL keys each record
to an av intervention, so the merger can anchor the text join to the record's
pages and fill ``people[].originPersonID``.

The file links sit in the plain HTML of the dataset's folder pages::

    https://www.parlamento.pt/Cidadania/Paginas/DAIntervencoes.aspx
      → folder link per legislatura (``t`` = hex(base64("XVII Legislatura")))
      → anchor ``IntervencoesXVII_json.txt`` → app.parlamento.pt/webutils/docs/doc.txt?…

Coverage is partial while a legislatura runs (records arrive weeks after the
sitting, and whole sessões legislativas can be missing), so the merger treats
the records as optional.

Outputs:

- ``cache/opendata/Intervencoes{ROMAN}.json`` — the raw file, re-downloaded at
  most every ``REFRESH_HOURS`` (or with ``--force``).
- ``original/opendata/{session}-od.json`` — the reunião's records, compacted and
  keyed by av intervention number; written only when they change, so a new
  open data release triggers a re-merge of exactly the affected reuniões.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import html as _html
import json
import logging
import re
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from optv.parliaments.PT.common import Config, make_session
from optv.parliaments.PT.scraper.common import http_get

logger = logging.getLogger(__name__)

INDEX_URL = "https://www.parlamento.pt/Cidadania/Paginas/DAIntervencoes.aspx"
REFRESH_HOURS = 20

_AV_URL_RE = re.compile(r"/videos/Plenary/(\d+)/(\d+)/(\d+)/(\d+)(?![\d/])")
_HREF_RE = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.IGNORECASE)
_PAGES_RE = re.compile(r"^\s*(\d+)\s*(?:-\s*(\d+))?\s*$")


def roman(n: int) -> str:
    """``17`` → ``"XVII"``."""
    out = ""
    for value, sym in ((1000, "M"), (900, "CM"), (500, "D"), (400, "CD"), (100, "C"),
                       (90, "XC"), (50, "L"), (40, "XL"), (10, "X"), (9, "IX"),
                       (5, "V"), (4, "IV"), (1, "I")):
        while n >= value:
            out += sym
            n -= value
    return out


def _decode_t(value: str) -> str:
    """Folder ids are hex-encoded base64 of the folder name."""
    try:
        return base64.b64decode(binascii.unhexlify(value)).decode("utf-8")
    except (binascii.Error, ValueError, UnicodeDecodeError):
        return ""


def find_folder_url(index_html: str, leg: int, base: str = INDEX_URL) -> Optional[str]:
    """Return the absolute URL of the legislatura's folder page, or None."""
    want = f"{roman(leg)} Legislatura"
    for href in _HREF_RE.findall(index_html):
        href = _html.unescape(href)
        m = re.search(r"DAIntervencoes\.aspx\?t=([0-9a-fA-F]+)", href)
        if m and _decode_t(m.group(1)).strip() == want:
            return urljoin(base, href)
    return None


def find_file_url(folder_html: str, leg: int, base: str = INDEX_URL) -> Optional[str]:
    """Return the absolute URL of ``Intervencoes{ROMAN}_json.txt``, or None."""
    fname = f"Intervencoes{roman(leg)}_json.txt"
    for href in _HREF_RE.findall(folder_html):
        href = _html.unescape(href)
        if f"fich={fname}" in href:
            return urljoin(base, href)
    return None


def _pages(record: dict) -> Optional[list[int]]:
    for pub in record.get("Publicacao") or []:
        for pag in pub.get("pag") or []:
            m = _PAGES_RE.match(str(pag))
            if m:
                a = int(m.group(1))
                return [a, int(m.group(2) or a)]
    return None


def _diario_url(record: dict) -> str:
    for pub in record.get("Publicacao") or []:
        if pub.get("URLDiario"):
            return pub["URLDiario"]
    return ""


def compact_record(record: dict) -> dict:
    """The fields the merger uses, from one Intervenções record."""
    dep = record.get("Deputados") or {}
    gov = record.get("MembrosGoverno") or {}
    av = (record.get("DadosAudiovisual") or [{}])[0]
    out = {
        "id": record.get("Id") or "",
        "type": record.get("TipoIntervencao") or "",
        "date": record.get("DataReuniaoPlenaria") or "",
        "personID": dep.get("idCadastro") or "",
        "personName": dep.get("nome") or gov.get("nome") or "",
        "party": dep.get("GP") or "",
        "governmentRole": gov.get("cargo") or "",
        "pages": _pages(record),
        "diarioURL": _diario_url(record),
        "debateID": record.get("IdDebate") or "",
        "debate": (record.get("Debate") or "").strip(),
        "summary": (record.get("Sumario") or "").strip(),
        "subject": (av.get("assunto") or "").strip(),
    }
    return {k: v for k, v in out.items() if v not in ("", None)}


def split_by_session(records: list[dict], leg: int) -> dict[str, dict[str, dict]]:
    """``{session: {av_number: compact_record}}`` for the legislatura's records."""
    out: dict[str, dict[str, dict]] = {}
    for rec in records:
        for av in rec.get("DadosAudiovisual") or []:
            m = _AV_URL_RE.search(av.get("url") or "")
            if not m or int(m.group(1)) != leg:
                continue
            session = make_session(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            out.setdefault(session, {})[m.group(4)] = compact_record(rec)
    return out


def _load_records(config: Config, leg: int, args) -> list[dict]:
    raw_path = config.dir("opendata_cache", create=True) / f"Intervencoes{roman(leg)}.json"
    fresh = (raw_path.exists()
             and time.time() - raw_path.stat().st_mtime < REFRESH_HOURS * 3600)
    if not fresh or getattr(args, "force", False):
        kw = dict(retry_count=args.retry_count, retry_delay_max=args.retry_delay_max)
        index_html = http_get(INDEX_URL, **kw)
        folder = find_folder_url(index_html, leg)
        if not folder:
            raise RuntimeError(f"no Intervenções folder for {roman(leg)} Legislatura")
        file_url = find_file_url(http_get(folder, **kw), leg)
        if not file_url:
            raise RuntimeError(f"no Intervencoes{roman(leg)}_json.txt in {folder}")
        logger.info(f"fetching open data Intervenções {roman(leg)}")
        payload = http_get(file_url, **kw)
        records = json.loads(payload.lstrip("\ufeff"))
        raw_path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
        return records
    return json.loads(raw_path.read_text(encoding="utf-8"))


def _in_scope(session: str, args) -> bool:
    pt_sessions = getattr(args, "pt_session", None) or []
    if pt_sessions:
        return session in pt_sessions
    limit = getattr(args, "limit_session", "") or ""
    if not limit:
        return True
    try:
        return bool(re.match(limit, session))
    except re.error:
        return limit == session


def download_opendata(config: Config, args) -> None:
    """Workflow hook: refresh the per-reunião open data records.

    Writes files only for reuniões that already have an av JSON (the spine) and
    are in scope, so the data repository holds no records for sittings it does
    not cover.
    """
    leg = int(getattr(args, "period", None) or 17)
    records = _load_records(config, leg, args)
    by_session = split_by_session(records, leg)
    known = set(config.sessions())
    written = 0
    for session, recs in sorted(by_session.items()):
        if session not in known or not _in_scope(session, args):
            continue
        out = config.raw_od(session)
        doc = {"session": session,
               "interventions": dict(sorted(recs.items(), key=lambda kv: int(kv[0])))}
        text = json.dumps(doc, indent=2, ensure_ascii=False)
        if out.exists() and out.read_text(encoding="utf-8") == text:
            continue
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
        written += 1
        logger.info(f"[{session}] wrote {out.name} ({len(recs)} interventions)")
    logger.info(f"open data {roman(leg)}: {len(records)} records, "
                f"{len(by_session)} reuniões, {written} file(s) updated")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=17)
    parser.add_argument("--pt-session", action="append", default=[], dest="pt_session")
    parser.add_argument("--limit-session", default="")
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--retry-delay-max", type=float, default=10.0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    download_opendata(Config(args.data_dir), args)


if __name__ == "__main__":
    main()
