#! /usr/bin/env python3
"""Resolve a Stortinget meeting id (``meid``) to its Qbrick video parts.

Long meetings are split into multiple "del" parts; each has its own qbvid
and ``custom.TC_in`` (UTC start time). The resolver follows two unauth GETs
per part:

1. The archive HTML page for ``?meid={meid}&del={N}`` carries a server-rendered
   ``"qbrickVideoId":"..."`` string. The same page lists every ``del`` in
   the meeting via ``meid={meid}&del=N`` links in its navigation block.
2. ``https://video.qbrick.com/api/v1/public/accounts/{ACCOUNT_ID}/medias/{qbvid}``
   returns asset metadata: MP4 renditions, HLS playlist, custom.TC_in /
   TC_out (UTC), custom.Del, custom.Dato, custom.Sesjon.

The Qbrick account id ``AccrjW9C7ikYk2xPM5xJ4Frag`` was identical for every
sampled meeting; we keep it as a constant with HTML rediscovery as fallback.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import asdict, dataclass
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

QBRICK_ACCOUNT_ID = "AccrjW9C7ikYk2xPM5xJ4Frag"
ARCHIVE_BASE = "https://www.stortinget.no/no/Hva-skjer-pa-Stortinget/videoarkiv/Arkiv-TV-sendinger/"
QBRICK_API = "https://video.qbrick.com/api/v1/public/accounts/{account}/medias/{qbvid}"

USER_AGENT = ("OpenParliamentTV-Tools/0.1 "
              "(+https://github.com/OpenParliamentTV)")


@dataclass
class VideoPart:
    """One contiguous video segment for a meeting."""
    moteid: int
    delnr: int                     # 1, 2, ...
    qbvid: str
    mp4_url: str                   # highest-quality MP4 (platform-facing)
    audio_mp4_url: str             # lowest-quality MP4 (same audio, less bandwidth)
    hls_url: str | None
    tc_in_utc: str                 # ISO 8601 UTC
    tc_out_utc: str | None
    duration_seconds: float | None # tc_out - tc_in (None if either missing)
    thumbnail_url: str | None
    raw: dict                      # full Qbrick payload for debugging

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("raw", None)
        return d


def _fetch(url: str, *, retry_count: int, retry_delay_max: float,
           accept: str = "*/*") -> bytes:
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(1, retry_count + 1):
        try:
            req = Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": accept,
                "Accept-Language": "en,nb;q=0.9",
            })
            with urlopen(req, timeout=30) as resp:
                return resp.read()
        except HTTPError as e:
            if 500 <= e.code < 600 and attempt < retry_count:
                logger.warning(f"HTTP {e.code} on {url}, retry {attempt}/{retry_count} after {delay:.1f}s")
                time.sleep(min(delay, retry_delay_max))
                delay = min(delay * 2, retry_delay_max)
                last_exc = e
                continue
            raise
        except (URLError, TimeoutError, ConnectionError) as e:
            if attempt < retry_count:
                logger.warning(f"{type(e).__name__} on {url}: {e}, retry {attempt}/{retry_count}")
                time.sleep(min(delay, retry_delay_max))
                delay = min(delay * 2, retry_delay_max)
                last_exc = e
                continue
            raise
    raise RuntimeError(f"Exhausted {retry_count} attempts for {url}: {last_exc}")


def _archive_url(moteid: int, delnr: int) -> str:
    # rtid/msid/del query parameters are populated by the SPA but the
    # server-side render path only needs meid+del to seed qbrickVideoId.
    return f"{ARCHIVE_BASE}?meid={moteid}&del={delnr}"


def _list_parts_in_html(html: str, moteid: int) -> list[int]:
    """All ``del=N`` part numbers referenced for ``meid``.

    Returns ``[1]`` when no other parts are referenced (single-part meeting).
    """
    # Navigation HREFs are JSON-encoded so ``&`` becomes ``&``.
    found = re.findall(rf"meid={moteid}(?:&|\\u0026)del=(\d+)", html)
    nrs = sorted({int(n) for n in found})
    return nrs if nrs else [1]


def _qbid_from_html(html: str) -> str | None:
    m = re.search(r'"qbrickVideoId":"([^"]+)"', html)
    return m.group(1) if m else None


def _qbrick_payload(qbvid: str, *, retry_count: int, retry_delay_max: float,
                    account: str = QBRICK_ACCOUNT_ID) -> dict:
    url = QBRICK_API.format(account=account, qbvid=qbvid)
    body = _fetch(url, retry_count=retry_count, retry_delay_max=retry_delay_max,
                  accept="application/json")
    return json.loads(body)


def _pick_best_mp4(payload: dict) -> tuple[str, str | None]:
    """Return ``(mp4_url, hls_url)`` extracted from the Qbrick asset block.

    Returns the **highest-quality** MP4 (for the platform-facing
    ``videoFileURI``); alignment paths use a lower-bitrate variant via
    :func:`pick_lowest_mp4` to save bandwidth when extracting audio.
    """
    return _pick_mp4(payload, rank=-1), _pick_hls(payload)


def pick_lowest_mp4(payload: dict) -> str:
    """Lowest-bitrate MP4. Used by align_prep — same audio track, ~95 % smaller."""
    return _pick_mp4(payload, rank=0)


def _pick_mp4(payload: dict, *, rank: int) -> str:
    """``rank=-1`` → highest height, ``rank=0`` → lowest height."""
    asset = payload.get("asset") or {}
    mp4_renditions: list[tuple[int, str]] = []
    for resource in asset.get("resources") or []:
        if resource.get("type") != "video":
            continue
        for rend in resource.get("renditions") or []:
            for link in rend.get("links") or []:
                href = link.get("href") or ""
                if link.get("mimeType") == "video/mp4" and href:
                    m = re.search(r"_(\d+)p\.mp4$", href)
                    height = int(m.group(1)) if m else 0
                    mp4_renditions.append((height, href))
    mp4_renditions.sort()
    return mp4_renditions[rank][1] if mp4_renditions else ""


def _pick_hls(payload: dict) -> str | None:
    asset = payload.get("asset") or {}
    for resource in asset.get("resources") or []:
        if resource.get("type") != "index":
            continue
        for rend in resource.get("renditions") or []:
            for link in rend.get("links") or []:
                if "mpegURL" in (link.get("mimeType") or ""):
                    return link.get("href")
    return None


def _pick_thumbnail(payload: dict) -> str | None:
    asset = payload.get("asset") or {}
    best: tuple[int, str] | None = None
    for resource in asset.get("resources") or []:
        if resource.get("type") != "image":
            continue
        if "thumbnail" not in (resource.get("rel") or []):
            continue
        for rend in resource.get("renditions") or []:
            for link in rend.get("links") or []:
                if link.get("mimeType") == "image/jpeg":
                    height = rend.get("height") or 0
                    if best is None or height > best[0]:
                        best = (height, link.get("href"))
    return best[1] if best else None


def _parse_tc(s: str | None) -> tuple[str | None, float | None]:
    """Return ``(iso_utc, epoch_seconds)`` from a Qbrick ``TC_in``/``TC_out``."""
    if not s:
        return None, None
    # Already ISO 8601 with timezone offset.
    try:
        import datetime as dt
        d = dt.datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        # Normalise to ``…+00:00`` for downstream string compares.
        utc = d.astimezone(dt.timezone.utc).isoformat(timespec="seconds")
        return utc, d.timestamp()
    except Exception:
        return None, None


def resolve_meeting_video(moteid: int, *,
                          retry_count: int = 10,
                          retry_delay_max: float = 10.0) -> list[VideoPart]:
    """Resolve every video part for a meeting. Returns ordered by ``delnr``."""
    # Fetch part-1 archive page to discover the part list (the nav block on
    # any part lists all parts).
    seed_url = _archive_url(moteid, 1)
    logger.info(f"GET {seed_url}")
    html = _fetch(seed_url, retry_count=retry_count,
                  retry_delay_max=retry_delay_max).decode("utf-8", errors="replace")
    parts = _list_parts_in_html(html, moteid)
    logger.info(f"  meid={moteid} → {len(parts)} part(s): {parts}")

    out: list[VideoPart] = []
    for delnr in parts:
        if delnr == 1:
            part_html = html
        else:
            part_url = _archive_url(moteid, delnr)
            logger.info(f"GET {part_url}")
            part_html = _fetch(part_url, retry_count=retry_count,
                               retry_delay_max=retry_delay_max).decode("utf-8", errors="replace")
        qbvid = _qbid_from_html(part_html)
        if not qbvid:
            logger.warning(f"  meid={moteid} del={delnr}: no qbrickVideoId in HTML — skipping part")
            continue
        payload = _qbrick_payload(qbvid, retry_count=retry_count,
                                  retry_delay_max=retry_delay_max)
        custom = payload.get("custom") or {}
        mp4_url, hls_url = _pick_best_mp4(payload)
        audio_mp4_url = pick_lowest_mp4(payload) or mp4_url
        tc_in, tc_in_epoch = _parse_tc(custom.get("TC_in"))
        tc_out, tc_out_epoch = _parse_tc(custom.get("TC_out"))
        duration = (tc_out_epoch - tc_in_epoch) if (tc_in_epoch and tc_out_epoch) else None
        if not mp4_url:
            logger.warning(f"  meid={moteid} del={delnr} qbvid={qbvid}: no MP4 rendition found")
        if not tc_in:
            logger.warning(f"  meid={moteid} del={delnr} qbvid={qbvid}: no TC_in in custom")
        out.append(VideoPart(
            moteid=moteid,
            delnr=delnr,
            qbvid=qbvid,
            mp4_url=mp4_url,
            audio_mp4_url=audio_mp4_url,
            hls_url=hls_url,
            tc_in_utc=tc_in or "",
            tc_out_utc=tc_out,
            duration_seconds=duration,
            thumbnail_url=_pick_thumbnail(payload),
            raw=payload,
        ))
    return out


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("meid", type=int)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parts = resolve_meeting_video(args.meid)
    print(json.dumps([p.to_dict() for p in parts], indent=2, ensure_ascii=False))
