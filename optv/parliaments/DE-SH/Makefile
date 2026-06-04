MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-DE-SH
MEDIADIR := $(DATADIR)/original/media
MERGEDDIR := $(DATADIR)/cache/merged
METADIR := $(DATADIR)/metadata

# Media is one HTML file per Sitzung holding the result.php response for
# all speeches of that day, parsed into 20NNN-media.json. There is no
# proceedings stream for DE-SH (PDF-only Plenarprotokolle, no parser).
MEDIA_RAW := $(wildcard $(MEDIADIR)/20*.html)
MEDIA_DEST := $(patsubst %.html,%-media.json,$(MEDIA_RAW))
MERGE_DEST := $(patsubst $(MEDIADIR)/%-media.json,$(MERGEDDIR)/%-merged.json,$(MEDIA_DEST))

all: download $(MERGE_DEST)

download:
	$(BASEDIR)/workflow.py --period=20 --download-original --no-merge-speeches "$(DATADIR)"

update:
	$(BASEDIR)/update "$(DATADIR)"

debug:
	-echo "MEDIA_RAW: $(MEDIA_RAW)"
	-echo "MEDIA_DEST: $(MEDIA_DEST)"
	-echo "MERGE_DEST: $(MERGE_DEST)"

clean:
	$(RM) $(MEDIADIR)/20*-media.json
	$(RM) $(MERGEDDIR)/20*-merged.json
