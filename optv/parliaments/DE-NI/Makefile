MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-DE-NI
MEDIADIR := $(DATADIR)/original/media
MERGEDDIR := $(DATADIR)/cache/merged

# Media is one intermediate JSON per Sitzung ({WP}NNN-media.json, WP 18 and 19),
# built from the Plenar-TV REST API (api.plenartv.de); the merger attaches the
# Plenar-TV subtitle text (WP 19).
MEDIA_DEST := $(wildcard $(MEDIADIR)/1[89]*-media.json)
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
	$(RM) $(MEDIADIR)/1[89]*-media.json
	$(RM) $(MERGEDDIR)/1[89]*-merged.json
