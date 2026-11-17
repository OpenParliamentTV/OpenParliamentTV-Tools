MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-DE-HH
MEDIADIR := $(DATADIR)/original/media
MERGEDDIR := $(DATADIR)/cache/merged

# Media is one intermediate JSON per Sitzung (23NNN-media.json), built from the
# mediathek session page (plus its per-speech subtitles). The Plenarprotokoll
# PDFs (ParlDok) are parsed by parsers/proceedings2json.py into
# original/proceedings/23NNN-proceedings.json; the merger reads both.
MEDIA_DEST := $(wildcard $(MEDIADIR)/23*-media.json)
MERGE_DEST := $(patsubst $(MEDIADIR)/%-media.json,$(MERGEDDIR)/%-merged.json,$(MEDIA_DEST))

all: download $(MERGE_DEST)

download:
	$(BASEDIR)/workflow.py --period=23 --download-original --no-merge-speeches "$(DATADIR)"

update:
	$(BASEDIR)/update "$(DATADIR)"

debug:
	-echo "MEDIA_DEST: $(MEDIA_DEST)"
	-echo "MERGE_DEST: $(MERGE_DEST)"

clean:
	$(RM) $(MEDIADIR)/23*-media.json
	$(RM) $(MERGEDDIR)/23*-merged.json
