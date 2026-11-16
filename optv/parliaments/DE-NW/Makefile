MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-DE-NW
MEDIADIR := $(DATADIR)/original/media
MERGEDDIR := $(DATADIR)/cache/merged

# Media is one intermediate JSON per Sitzung (18NNN-media.json), built from the
# mediathek video page. The HTML Plenarprotokoll (original/proceedings/18NNN.html)
# is parsed into 18NNN-proceedings.json and joined onto the media in the merger.
MEDIA_DEST := $(wildcard $(MEDIADIR)/18*-media.json)
MERGE_DEST := $(patsubst $(MEDIADIR)/%-media.json,$(MERGEDDIR)/%-merged.json,$(MEDIA_DEST))

all: download $(MERGE_DEST)

download:
	$(BASEDIR)/workflow.py --period=18 --download-original --no-merge-speeches "$(DATADIR)"

update:
	$(BASEDIR)/update "$(DATADIR)"

entity-dump:
	python3 -m optv.parliaments.DE-NW.scraper.build_entity_dump "$(DATADIR)"

debug:
	-echo "MEDIA_DEST: $(MEDIA_DEST)"
	-echo "MERGE_DEST: $(MERGE_DEST)"

clean:
	$(RM) $(MEDIADIR)/18*-media.json
	$(RM) $(MERGEDDIR)/18*-merged.json
