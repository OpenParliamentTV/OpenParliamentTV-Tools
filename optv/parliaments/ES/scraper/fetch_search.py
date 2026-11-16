#! /usr/bin/env python3

# Fallback media source: the Congreso intervention search (Búsqueda de
# intervenciones), used when the open data interventions feed cannot be
# downloaded.
#
# The search page fills its result list from a JSON resource (POST
# `filtrarListado`) backed by the same database as the open data files: one
# entry per intervention, with speaker and office, initiative and expediente,
# the Diario page, the per-clip MP4 on static.congreso.es and clock times to
# the second. Filtered by legislature, date range and organ ("Pleno"), it
# lists 25 interventions per page, so a sitting takes about five requests.
# The entries are mapped onto the open data record fields, so the scraper's
# session grouping, media2json and the proceedings scraper read them
# unchanged.

import logging
logger = logging.getLogger(__name__)

import argparse
from datetime import datetime
import json
import math
from pathlib import Path
import re
import sys
import time
from random import random

if __package__ is None or __package__ == "":
    module_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(module_dir.parent))
    __package__ = module_dir.name

from .common import fetch_url

SEARCH_PAGE = "https://www.congreso.es/es/busqueda-de-intervenciones"
LIST_URL = (SEARCH_PAGE + "?p_p_id=intervenciones&p_p_lifecycle=2&p_p_state=normal"
            "&p_p_mode=view&p_p_resource_id=filtrarListado&p_p_cacheability=cacheLevelPage")
TEXT_URL = (SEARCH_PAGE + "?p_p_id=intervenciones&p_p_lifecycle=0&p_p_state=normal"
            "&p_p_mode=view&_intervenciones_mode=mostrarTextoIntegro"
            "&_intervenciones_legislatura={roman}&_intervenciones_id_texto={edia}")
PDF_BASE = "https://www.congreso.es/public_oficiales/L{period}/"
PAGE_SIZE = 25
# Pause between list pages: the requests go to www.congreso.es.
PAGE_DELAY = 2.0
RETRY_MAX_WAIT_TIME = 10
# Without a known start date the whole legislature is listed; refuse to page
# through more than this (about ten sittings) and ask for --since instead.
MAX_PAGES_UNBOUNDED = 40

_FILTER_FIELDS = ("legislatura", "orador", "cargo", "titulo", "texto", "tipoIniciativa",
                  "fechaDesde", "fechaHasta", "expedientes", "hasta", "fase", "organo",
                  "autor", "modoListado", "paginaActual", "id_iniciativa")
_ROMAN = {10: "X", 11: "XI", 12: "XII", 13: "XIII", 14: "XIV", 15: "XV", 16: "XVI",
          17: "XVII", 18: "XVIII", 19: "XIX", 20: "XX"}
_NUMBERED_RE = re.compile(r"^([a-z_]+?)(\d{2})$")


def list_form(period: int, date_from: str, date_to: str, page: int) -> dict:
    """Form fields of one result-list request (dates as dd/mm/yyyy)."""
    form = {f"_intervenciones_{name}": "" for name in _FILTER_FIELDS}
    form.update({
        "_intervenciones_legislatura": str(period),
        "_intervenciones_fechaDesde": date_from,
        "_intervenciones_fechaHasta": date_to,
        "_intervenciones_organo": "Pleno",
        "_intervenciones_modoListado": "1",
        "_intervenciones_paginaActual": str(page),
    })
    return form


def fetch_list_page(period: int, date_from: str, date_to: str, page: int,
                    retry_count: int = 0) -> dict:
    """Return one parsed result-list page; {} when nothing matches."""
    form = list_form(period, date_from, date_to, page)
    for attempt in range(retry_count + 1):
        try:
            payload = json.loads(fetch_url(LIST_URL, referer=SEARCH_PAGE, form=form,
                                           accept="application/json, text/javascript, */*; q=0.01"))
            break
        except Exception as e:
            if attempt == retry_count:
                raise
            wait = random() * RETRY_MAX_WAIT_TIME
            logger.warning(f"Search list page {page}: {type(e).__name__}: {e} - retrying in {wait:.1f}s")
            time.sleep(wait)
    if payload.get("error"):
        return {}
    return payload


def fetch_entries(period: int, date_from: str = "", date_to: str = "",
                  retry_count: int = 0, delay: float = PAGE_DELAY,
                  max_pages: int = None) -> list:
    """All plenary result entries between two dates (dd/mm/yyyy, inclusive;
    empty for an open end). Raises RuntimeError past `max_pages` pages."""
    first = fetch_list_page(period, date_from, date_to, 1, retry_count)
    if not first:
        return []
    total = int(first.get("intervenciones_encontradas") or 0)
    pages = math.ceil(total / PAGE_SIZE)
    if max_pages is not None and pages > max_pages:
        raise RuntimeError(f"{total} interventions ({pages} pages) from "
                           f"'{date_from or 'start'}' to '{date_to or 'now'}'; "
                           f"more than {max_pages} pages")
    logger.info(f"Intervention search: {total} plenary interventions, {pages} page(s)")
    entries = list((first.get("lista_intervenciones") or {}).values())
    for page in range(2, pages + 1):
        time.sleep(delay)
        payload = fetch_list_page(period, date_from, date_to, page, retry_count)
        entries.extend((payload.get("lista_intervenciones") or {}).values())
    if len(entries) != total:
        logger.warning(f"Intervention search listed {len(entries)} of {total} interventions")
    return entries


def _numbered(values: dict, prefix: str) -> list:
    """Values of `prefix01`, `prefix02`, … in key order."""
    found = []
    for key, value in (values or {}).items():
        m = _NUMBERED_RE.match(key)
        if m and m.group(1) == prefix and value:
            found.append((int(m.group(2)), value))
    return [value for _n, value in sorted(found)]


def to_feed_record(entry: dict, period: int) -> dict:
    """Map one search entry onto the open data record fields.

    A clip's id, `inicioNN` and `finNN` are numbered per clip; its MP4
    (`enlace_descargaNN`) and share link are numbered on a separate count
    (02, 04, …), so both are read in key order. As in the open data, the
    times are those of the first clip; they keep their seconds.
    """
    video = entry.get("video_intervencion") or {}
    sesion = entry.get("sesion") or {}
    initiative = entry.get("iniciativa") or {}
    fecha = entry.get("fecha") or ""
    starts, ends = _numbered(video, "inicio"), _numbered(video, "fin")
    authors = sorted((initiative.get("autores") or {}).values(),
                     key=lambda a: int(a.get("orden") or 0))
    edia, pdia = entry.get("edia") or "", entry.get("pdia") or ""
    return {
        "LEGISLATURA": f"Leg.{period}",
        "TIPOINICIATIVA": initiative.get("tipo") or "",
        "OBJETOINICIATIVA": initiative.get("objeto_iniciativa") or "",
        "NUMEXPEDIENTE": (initiative.get("enlace_expediente") or {}).get("id_iniciativa") or "",
        "AUTOR": " ".join(a.get("nombre") or "" for a in authors).strip(),
        "SESION": f"{fecha[6:8]}/{fecha[4:6]}/{fecha[:4]}" if len(fecha) == 8 else "",
        "ORGANO": sesion.get("nombre_sesion") or "",
        "FASE": sesion.get("fase1") or "",
        "TIPOINTERVENCION": entry.get("tipo_intervencion") or "Intervención",
        "ORADOR": entry.get("orador") or "",
        "CARGOORADOR": entry.get("cargo_orador") or "",
        "INICIOINTERVENCION": starts[0] if starts else "",
        "FININTERVENCION": ends[0] if ends else "",
        "ENLACEDIFERIDO": " ".join(_numbered(video, "enlace_emision")),
        "ENLACEDESCARGADIRECTA": "\n".join(_numbered(video, "enlace_descarga")),
        "ENLACETEXTOINTEGRO": (TEXT_URL.format(roman=_ROMAN.get(period, str(period)), edia=edia)
                               if edia else ""),
        "ENLACEPDF": PDF_BASE.format(period=period) + pdia if pdia else "",
    }


def search_records(period: int, date_from: str = "", date_to: str = "",
                   retry_count: int = 0, delay: float = PAGE_DELAY) -> list:
    """Open-data-shaped records for the plenary interventions between two
    dates, in chronological order. An open start lists the whole
    legislature and is capped at MAX_PAGES_UNBOUNDED pages."""
    max_pages = None if date_from else MAX_PAGES_UNBOUNDED
    entries = fetch_entries(period, date_from, date_to, retry_count, delay, max_pages)
    entries.sort(key=lambda e: (e.get("fecha") or "",
                                _numbered(e.get("video_intervencion"), "inicio")[:1],
                                int(e.get("doc") or 0)))
    return [to_feed_record(e, period) for e in entries]


def latest_session_date(media_dir: Path, period: int):
    """Sitting date of the newest raw session file of the period, else None."""
    files = sorted(Path(media_dir).glob(f"raw-{period}[0-9][0-9][0-9]-media.json"))
    for path in reversed(files):
        try:
            records = json.loads(path.read_text()).get("interventions") or []
        except (OSError, ValueError):
            continue
        dates = [datetime.strptime(r["SESION"].strip(), "%d/%m/%Y").date()
                 for r in records if (r.get("SESION") or "").strip()]
        if dates:
            return max(dates)
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List plenary interventions from the Congreso intervention search as open-data-shaped records.")
    parser.add_argument("--period", type=int, default=15, help="Legislature/period (default: 15)")
    parser.add_argument("--since", type=str, default="", help="First sitting date, dd/mm/yyyy")
    parser.add_argument("--until", type=str, default="", help="Last sitting date, dd/mm/yyyy")
    parser.add_argument("--retry-count", type=int, default=2)
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    json.dump(search_records(args.period, args.since, args.until, args.retry_count),
              sys.stdout, indent=2, ensure_ascii=False)
