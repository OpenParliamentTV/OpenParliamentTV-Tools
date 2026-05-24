"""Quality-control transcription via faster-whisper + Resemblyzer.

Standalone library used by the `whisper_diff` CLI. Not part of the production
pipeline — Whisper output is a ground-truth reference for comparing against
proceedings text, not a replacement for it.

Subprocess isolation pattern mirrors `optv.shared.align._aeneas_worker`: each
call runs in a spawn-context child so the heavy ML models never leak into the
parent and per-call timeouts are enforceable.
"""

import logging
import multiprocessing
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "large-v3"
# Parliamentary speeches routinely run 30–120 min; default per-call timeout
# must accommodate the longest plausible single intervention (102-min
# investiture opening observed in DE/ES corpora). Override per-call via
# transcribe_speech(timeout=...) or the CLI's --timeout flag.
DEFAULT_TIMEOUT = 14400
SPEAKER_CHANGE_THRESHOLD = 0.65
SPEAKER_WINDOW_S = 1.5
SPEAKER_HOP_S = 0.75
SPEAKER_DEDUP_WINDOW_S = 3.0


def _whisper_worker(audio_path: str, language: str, model_size: str,
                    out_queue) -> None:
    try:
        from faster_whisper import WhisperModel
        model = WhisperModel(model_size, device="cpu", compute_type="int8")
        segments_iter, info = model.transcribe(
            audio_path,
            language=language,
            vad_filter=True,
            word_timestamps=False,
        )
        segments = [
            {"start": float(s.start), "end": float(s.end), "text": s.text}
            for s in segments_iter
        ]
        text = "".join(s["text"] for s in segments).strip()
        out_queue.put({
            "ok": True,
            "text": text,
            "segments": segments,
            "language": info.language,
            "language_probability": float(info.language_probability),
            "duration": float(info.duration),
            "model": model_size,
        })
    except Exception as e:
        out_queue.put({"ok": False, "error": f"{type(e).__name__}: {e}"})


def transcribe_speech(audio_path: Path, language: str = "de",
                      model_size: str = DEFAULT_MODEL,
                      timeout: int = DEFAULT_TIMEOUT) -> Optional[dict]:
    """Transcribe a single audio file in an isolated child process.

    Returns None on timeout/error so the caller can skip and continue.
    """
    audio_path = Path(audio_path)
    if not audio_path.exists():
        logger.error(f"Audio file missing: {audio_path}")
        return None

    ctx = multiprocessing.get_context("spawn")
    q = ctx.Queue()
    p = ctx.Process(target=_whisper_worker,
                    args=(str(audio_path), language, model_size, q))
    p.start()
    p.join(timeout)
    if p.is_alive():
        p.terminate()
        p.join(5)
        logger.error(f"faster-whisper timed out after {timeout}s on {audio_path.name}")
        return None
    if q.empty():
        logger.error(f"faster-whisper produced no result for {audio_path.name}")
        return None
    result = q.get()
    if not result.get("ok"):
        logger.error(f"faster-whisper failed on {audio_path.name}: {result.get('error')}")
        return None
    return result


def _speaker_worker(audio_path: str, window_s: float, hop_s: float,
                    threshold: float, out_queue) -> None:
    try:
        from resemblyzer import VoiceEncoder, preprocess_wav
        import numpy as np

        wav = preprocess_wav(audio_path)
        encoder = VoiceEncoder("cpu", verbose=False)

        sr = 16000
        win = int(window_s * sr)
        hop = int(hop_s * sr)
        if len(wav) < win * 2:
            out_queue.put({"ok": True, "changes": [], "n_windows": 0})
            return

        starts = list(range(0, len(wav) - win, hop))
        embeds = []
        for start in starts:
            chunk = wav[start:start + win]
            embeds.append(encoder.embed_utterance(chunk))
        embeds = np.array(embeds)

        raw_changes = []
        for i in range(1, len(embeds)):
            sim = float(np.dot(embeds[i - 1], embeds[i]))
            if sim < threshold:
                t = (starts[i] + win // 2) / sr
                raw_changes.append({"time_seconds": round(t, 2),
                                    "similarity": round(sim, 3)})

        # Collapse change points that cluster within SPEAKER_DEDUP_WINDOW_S
        # of each other — applause/pauses can produce 3-4 detections in a row
        # for what is really one transition. Keep the lowest-similarity
        # (= strongest signal) point per cluster.
        deduped = []
        for ch in raw_changes:
            if deduped and ch["time_seconds"] - deduped[-1]["time_seconds"] < SPEAKER_DEDUP_WINDOW_S:
                if ch["similarity"] < deduped[-1]["similarity"]:
                    deduped[-1] = ch
            else:
                deduped.append(ch)

        out_queue.put({
            "ok": True,
            "changes": deduped,
            "raw_changes": raw_changes,
            "n_windows": len(starts),
            "threshold": threshold,
            "dedup_window_s": SPEAKER_DEDUP_WINDOW_S,
        })
    except Exception as e:
        out_queue.put({"ok": False, "error": f"{type(e).__name__}: {e}"})


def detect_speaker_changes(audio_path: Path,
                           window_s: float = SPEAKER_WINDOW_S,
                           hop_s: float = SPEAKER_HOP_S,
                           threshold: float = SPEAKER_CHANGE_THRESHOLD,
                           timeout: int = 600) -> Optional[dict]:
    """Detect speaker change points using Resemblyzer.

    Returns dict with `changes` (list of {time_seconds, similarity}) or None
    on error/timeout. Lower `threshold` = stricter (fewer changes flagged).
    """
    audio_path = Path(audio_path)
    if not audio_path.exists():
        logger.error(f"Audio file missing: {audio_path}")
        return None

    ctx = multiprocessing.get_context("spawn")
    q = ctx.Queue()
    p = ctx.Process(target=_speaker_worker,
                    args=(str(audio_path), window_s, hop_s, threshold, q))
    p.start()
    p.join(timeout)
    if p.is_alive():
        p.terminate()
        p.join(5)
        logger.error(f"resemblyzer timed out after {timeout}s on {audio_path.name}")
        return None
    if q.empty():
        logger.error(f"resemblyzer produced no result for {audio_path.name}")
        return None
    result = q.get()
    if not result.get("ok"):
        logger.error(f"resemblyzer failed on {audio_path.name}: {result.get('error')}")
        return None
    return result
