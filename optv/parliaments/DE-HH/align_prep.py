#! /usr/bin/env python3
"""Stage per-speech DE-HH audio into per-speech MP3s for aeneas.

Each DE-HH speech is a ``<per-TOP-HLS-clip>#t=start,end`` media fragment: each
Tagesordnungspunkt is its own server-side HLS clip and several speeches share one
clip. The shared driver downloads each distinct clip once and re-encode-slices
each text-bearing speech into the cache path ``optv.shared.align`` looks for.
Text comes from the joined Plenarprotokoll spine (``join_text_to_spine`` in the
merger); only speeches that carry text are staged. Experimental, unvalidated
path (see manifest).
"""

from optv.shared.audio_prep import make_fragment_prepare

prepare_per_speech_audio = make_fragment_prepare(hls=True, reconnect=True)
