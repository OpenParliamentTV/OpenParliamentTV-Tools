MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-DE-BW
MEDIADIR := $(DATADIR)/original/media
MERGEDDIR := $(DATADIR)/cache/merged

# Wahlperiode to download/merge (current WP by default; `make PERIOD=17`).
PERIOD ?= 18

# Media is one intermediate JSON per Sitzung ({PERIOD}NNN-media.json), built
# from the mediathek per-session chapter list; the Plenarprotokoll PDF is parsed
# separately (parsers/proceedings2json.py) and joined in the merger.
MEDIA_DEST := $(wildcard $(MEDIADIR)/$(PERIOD)*-media.json)
MERGE_DEST := $(patsubst $(MEDIADIR)/%-media.json,$(MERGEDDIR)/%-merged.json,$(MEDIA_DEST))

all: download $(MERGE_DEST)

download:
	$(BASEDIR)/workflow.py --period=$(PERIOD) --download-original --no-merge-speeches "$(DATADIR)"

update:
	$(BASEDIR)/update "$(DATADIR)"

debug:
	-echo "MEDIA_DEST: $(MEDIA_DEST)"
	-echo "MERGE_DEST: $(MERGE_DEST)"

clean:
	$(RM) $(MEDIADIR)/$(PERIOD)*-media.json
	$(RM) $(MERGEDDIR)/$(PERIOD)*-merged.json
