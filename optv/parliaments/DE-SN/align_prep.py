#! /usr/bin/env python3
"""Stage per-speech DE-SN audio into per-speech MP3s for aeneas.

Each DE-SN speech is a ``<daily-HLS-stream>#t=start,end`` media fragment: one
daily SMIL/HLS stream per sitting day, sliced per speech (end offsets clamped to
the next speech's start in ``media2json`` so the windows are disjoint). The
shared driver downloads each daily stream once and re-encode-slices each
text-bearing speech into the cache path ``optv.shared.align`` looks for. Text
comes from the joined Plenarprotokoll spine (``join_text_to_spine`` in the
merger); only speeches that carry text are staged. Experimental, unvalidated
path (see manifest).
"""

from optv.shared.audio_prep import make_fragment_prepare

prepare_per_speech_audio = make_fragment_prepare(hls=True, reconnect=True)
