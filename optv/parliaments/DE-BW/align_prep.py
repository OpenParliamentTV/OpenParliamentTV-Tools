#! /usr/bin/env python3
"""Stage per-speech DE-BW audio into per-speech MP3s for aeneas.

Each DE-BW speech is a ``<session-mp4>#t=start,end`` media fragment: one MP4 per
session, sliced per speech (the offsets are video-relative). The shared driver
downloads the session MP4's audio once and re-encode-slices each text-bearing
speech into the cache path ``optv.shared.align`` looks for. Text comes from the
joined Plenarprotokoll spine (``join_text_to_spine`` in the merger); only
speeches that carry text are staged. Experimental, unvalidated path (see
manifest).
"""

from optv.shared.audio_prep import make_fragment_prepare

# Base mp4 (not HLS) — ffmpeg extracts the audio track with -vn.
prepare_per_speech_audio = make_fragment_prepare(hls=False)
