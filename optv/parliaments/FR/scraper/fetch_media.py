#! /usr/bin/env python3
"""Resolve the séance video for each sitting from its HTML compte rendu.

The Syceron compte rendu XML already carries per-speech ``stime`` offsets into
the séance recording, so the media side only has to supply **one video per
séance** and the recording's start time. Both come from the HTML compte rendu
``/dyn/{leg}/comptes-rendus/seance/{uid}``, one request per sitting:

* the séance button (``data-targetEmbedId="crsVideoEmbed"`` without
  ``data-contentParameters``) carries the HLS master playlist URL, whose file
  name ``hemi_YYYYMMDDHHMMSS`` is the recording's local start time;
* every intervention block ``<div id="{id_syceron}" class="crs-inter">`` carries
  the same HLS URL and ``data-contentParameters='{"currentTime": …}'``, the
  intervention's offset. It equals the XML ``stime`` (rounded to 0.1 s above
  10000 s), so the mismatch count is a staleness check of the local XML.

A sitting is skipped without any request when

* its local compte rendu has no ``stime`` (a provisional version, which the
  complete version replaces, or one of the few complete ones without offsets):
  no speech could be placed on the video;
* it is older than ``VIDEO_MAX_AGE_DAYS`` (bulk mode only): the AN keeps
  recordings online for about 10–11 months;
* it is listed in ``original/media/_unavailable.json``: its recording was found
  offline (HTTP 404) on an earlier run, and offline recordings do not return.

Output: ``original/media/{session}-event.json``.
"""

from __future__ import annotations

import argparse
import datetime
import html as _html
import json
import logging
import re
from pathlib import Path
from typing import Optional

import lxml.etree as ET

from optv.parliaments.FR.common import (
    PARIS, Config, cr_page_url, recording_id, recording_start, seance_start,
)
from optv.parliaments.FR.scraper.common import http_get, http_status

logger = logging.getLogger(__name__)

NS = "{http://schemas.assemblee-nationale.fr/referentiel}"

# Recordings go offline after about 10–11 months; bulk mode does not request
# anything for older sittings.
VIDEO_MAX_AGE_DAYS = 365
# HTML offsets are rounded to 0.1 s above 10000 s.
OFFSET_TOLERANCE = 0.06

_SEANCE_REF_RE = re.compile(r"<seanceRef>\s*(RUANR5L\d+S\d{4}IDS\d+)\s*</seanceRef>")
_DATE_SEANCE_RE = re.compile(r"<dateSeance>\s*(\d{14,})\s*</dateSeance>")
_ETAT_RE = re.compile(r"<etat>\s*([^<\s]+)\s*</etat>")

_BUTTON_RE = re.compile(r"<button\b[^>]*>")
_ATTR_CONTENT_RE = re.compile(r'\bdata-content="([^"]*)"')
_ATTR_PARAMS_RE = re.compile(r"\bdata-contentParameters='([^']*)'")
_INTER_RE = re.compile(r'<div id="(\d+)"\s+class="crs-inter\b')


# ---- compte rendu (local XML) ----

def _read_cr_head(config: Config, session: str) -> Optional[dict]:
    """Cheap facts from the local compte rendu: réunion id, scheduled start,
    ``<etat>`` and whether any ``stime`` is present."""
    cr = config.raw_cr(session)
    if not cr.exists():
        logger.warning(f"[{session}] no compte rendu — cannot resolve video")
        return None
    text = cr.read_text(encoding="utf-8")
    ref = _SEANCE_REF_RE.search(text)
    date = _DATE_SEANCE_RE.search(text)
    etat = _ETAT_RE.search(text)
    return {
        "seanceRef": ref.group(1) if ref else None,
        "scheduled": seance_start(date.group(1)) if date else None,
        "etat": etat.group(1) if etat else None,
        "hasStime": 'stime="' in text,
    }


def _xml_stimes(config: Config, session: str) -> dict[str, float]:
    """``{id_syceron: stime}`` of every paragraphe carrying an offset."""
    root = ET.parse(str(config.raw_cr(session))).getroot()
    out: dict[str, float] = {}
    for para in root.iter(NS + "paragraphe"):
        texte = para.find(NS + "texte")
        raw = texte.get("stime") if texte is not None else None
        if not raw:
            continue
        try:
            out[para.get("id_syceron") or ""] = float(raw)
        except ValueError:
            continue
    return out


# ---- compte rendu (HTML page) ----

def parse_cr_page(page: str) -> tuple[Optional[str], dict[str, tuple[str, float]]]:
    """Return ``(seance_hls_url, {id_syceron: (hls_url, current_time)})``."""
    seance_url = None
    for tag in _BUTTON_RE.findall(page):
        if 'data-targetEmbedId="crsVideoEmbed"' not in tag or "data-contentParameters" in tag:
            continue
        m = _ATTR_CONTENT_RE.search(tag)
        if m and ".m3u8" in m.group(1):
            seance_url = _html.unescape(m.group(1))
            break

    starts = list(_INTER_RE.finditer(page))
    offsets: dict[str, tuple[str, float]] = {}
    for i, m in enumerate(starts):
        end = starts[i + 1].start() if i + 1 < len(starts) else len(page)
        for tag in _BUTTON_RE.findall(page, m.end(), end):
            params = _ATTR_PARAMS_RE.search(tag)
            content = _ATTR_CONTENT_RE.search(tag)
            if not params or not content:
                continue
            try:
                current = float(json.loads(_html.unescape(params.group(1)))["currentTime"])
            except (ValueError, KeyError, TypeError):
                continue
            offsets[m.group(1)] = (_html.unescape(content.group(1)), current)
            break
    return seance_url, offsets


def offset_mismatches(page_offsets: dict[str, tuple[str, float]],
                      stimes: dict[str, float]) -> int:
    """Interventions whose page offset differs from the local XML ``stime``."""
    return sum(1 for pid, (_url, t) in page_offsets.items()
               if pid in stimes and abs(stimes[pid] - t) > OFFSET_TOLERANCE)


# ---- offline-recording cache ----

def _unavailable_path(config: Config) -> Path:
    return config.dir("media", create=True) / "_unavailable.json"


def _load_unavailable(config: Config) -> dict[str, dict]:
    p = _unavailable_path(config)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except json.JSONDecodeError:
            return {}
    return {}


def _save_unavailable(config: Config, data: dict[str, dict]) -> None:
    _unavailable_path(config).write_text(
        json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True))


# ---- resolution ----

def _resolve_one(config: Config, session: str, args, unavailable: dict[str, dict],
                 *, explicit: bool) -> bool:
    out = config.raw_event(session)
    force = bool(getattr(args, "force", False))
    if out.exists() and not force:
        logger.debug(f"[{session}] video reference cached")
        return False
    if session in unavailable and not force:
        logger.debug(f"[{session}] recording offline (cached)")
        return False

    head = _read_cr_head(config, session)
    if head is None:
        return False
    if not head["hasStime"]:
        logger.info(f"[{session}] compte rendu ({head['etat'] or 'unknown state'}) has "
                    f"no per-speech offsets — not resolving video")
        return False
    scheduled = head["scheduled"]
    if (not explicit and scheduled is not None
            and (datetime.datetime.now(PARIS) - scheduled).days > VIDEO_MAX_AGE_DAYS):
        logger.debug(f"[{session}] older than {VIDEO_MAX_AGE_DAYS} days — "
                     f"recording no longer online")
        return False

    page_url = cr_page_url(session, args.period)
    page = http_get(page_url, retry_count=args.retry_count,
                    retry_delay_max=args.retry_delay_max)
    seance_url, page_offsets = parse_cr_page(page)
    urls = {u for u, _t in page_offsets.values()}
    if seance_url:
        urls.add(seance_url)
    if not urls:
        logger.warning(f"[{session}] compte rendu page carries no video URL")
        return False
    if len(urls) > 1:
        logger.warning(f"[{session}] compte rendu page names {len(urls)} recordings "
                       f"({', '.join(sorted(urls))}) — not supported, skipping")
        return False
    hls = urls.pop()

    mismatches = offset_mismatches(page_offsets, _xml_stimes(config, session))
    if mismatches:
        logger.warning(f"[{session}] {mismatches} offset(s) on the compte rendu page "
                       f"differ from the local XML — the local copy is older than "
                       f"the published version; re-run the download")

    status = http_status(hls, retry_count=args.retry_count,
                         retry_delay_max=args.retry_delay_max)
    if status in (404, 410):
        logger.info(f"[{session}] recording offline (HTTP {status}): {hls}")
        unavailable[session] = {"hlsUrl": hls,
                                "checked": datetime.date.today().isoformat()}
        _save_unavailable(config, unavailable)
        return False
    if status >= 400:
        logger.warning(f"[{session}] recording check returned HTTP {status}: {hls}")
        return False
    unavailable.pop(session, None)

    start = recording_start(hls)
    if start is None:
        logger.warning(f"[{session}] no recording start in {hls} — the merger will "
                       f"approximate clock times from the scheduled start")
    event = {
        "session": session,
        "seanceRef": head["seanceRef"],
        "hlsUrl": hls,
        "recordingId": recording_id(hls),
        "recordingStart": start.isoformat() if start else None,
        "crPage": page_url,
        "offsetMismatches": mismatches,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(event, indent=2, ensure_ascii=False))
    logger.info(f"[{session}] {event['recordingId'] or hls}")
    return True


def _target_sessions(config: Config, args) -> tuple[list[str], bool]:
    """Return ``(sessions, explicit)``; explicit when named via ``--session``."""
    fr_sessions = getattr(args, "fr_session", None) or []
    if fr_sessions:
        return fr_sessions, True
    limit = getattr(args, "limit_session", "") or ""
    sessions = config.sessions()
    if limit:
        def _ok(s):
            try:
                return bool(re.match(limit, s))
            except re.error:
                return limit == s
        sessions = [s for s in sessions if _ok(s)]
    return sessions, False


def download_media(config: Config, args) -> None:
    """Workflow hook: resolve séance videos for the requested sittings."""
    unavailable = _load_unavailable(config)
    sessions, explicit = _target_sessions(config, args)
    for session in sessions:
        try:
            _resolve_one(config, session, args, unavailable, explicit=explicit)
        except Exception as e:  # noqa: BLE001
            logger.error(f"[{session}] media resolution failed: {e}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--period", type=int, default=17)
    parser.add_argument("--fr-session", action="append", default=[])
    parser.add_argument("--limit-session", default="")
    parser.add_argument("--retry-count", type=int, default=10)
    parser.add_argument("--retry-delay-max", type=float, default=10.0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config(args.data_dir)
    download_media(config, args)


if __name__ == "__main__":
    main()
