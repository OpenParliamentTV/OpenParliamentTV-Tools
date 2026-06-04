MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-DE-NI
MEDIADIR := $(DATADIR)/original/media
MERGEDDIR := $(DATADIR)/cache/merged

# Media is one intermediate JSON per Sitzung (19NNN-media.json), built from the
# Plenar-TV REST API (api.plenartv.de). There is no proceedings stream merged for
# DE-NI v1 (VTT subtitles / PDF Plenarprotokoll available but not yet wired).
MEDIA_DEST := $(wildcard $(MEDIADIR)/19*-media.json)
MERGE_DEST := $(patsubst $(MEDIADIR)/%-media.json,$(MERGEDDIR)/%-merged.json,$(MEDIA_DEST))

all: download $(MERGE_DEST)

download:
	$(BASEDIR)/workflow.py --period=19 --download-original --no-merge-speeches "$(DATADIR)"

update:
	$(BASEDIR)/update "$(DATADIR)"

debug:
	-echo "MEDIA_DEST: $(MEDIA_DEST)"
	-echo "MERGE_DEST: $(MERGE_DEST)"

clean:
	$(RM) $(MEDIADIR)/19*-media.json
	$(RM) $(MERGEDDIR)/19*-merged.json
