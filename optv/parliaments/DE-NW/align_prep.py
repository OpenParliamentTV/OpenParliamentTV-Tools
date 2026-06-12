#! /usr/bin/env python3
"""Stage per-speech DE-NW audio into per-speech MP3s for aeneas.

Each DE-NW speech is a ``<session-HLS-stream>#t=start,end`` media fragment: one
HLS master per Plenarsitzung, sliced per speech. The shared driver downloads the
session stream once and re-encode-slices each text-bearing speech into the cache
path ``optv.shared.align`` looks for. Text comes from the joined Plenarprotokoll
spine (``join_text_to_spine`` in the merger); only speeches that carry text are
staged. This is an experimental, unvalidated path (see manifest).
"""

from optv.shared.audio_prep import make_fragment_prepare

# One ~10 h HLS master per session — reconnect on flaky segments, generous timeout.
prepare_per_speech_audio = make_fragment_prepare(hls=True, reconnect=True)
