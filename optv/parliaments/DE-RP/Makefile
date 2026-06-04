MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-DE-RP
INBOXDIR := $(DATADIR)/inbox
MEDIAINBOX := $(DATADIR)/original/media/inbox
MEDIADIR := $(DATADIR)/original/media
PROCEEDINGSDIR := $(DATADIR)/original/proceedings
MERGEDDIR := $(DATADIR)/cache/merged

# Inputs are dropped into INBOXDIR (ePP XML) and MEDIAINBOX (saved OPAL HTML).
TRANSCRIPT_SOURCE := $(wildcard $(PROCEEDINGSDIR)/*-proceedings.xml)
MEDIA_SOURCE := $(wildcard $(MEDIADIR)/raw-*-media.json)

TRANSCRIPT_DEST := $(patsubst %.xml,%.json,$(TRANSCRIPT_SOURCE))
MEDIA_DEST := $(subst raw-,,$(MEDIA_SOURCE))
MERGE_DEST := $(subst /original/proceedings/,/cache/merged/,$(patsubst %-proceedings.json,%-merged.json,$(TRANSCRIPT_DEST)))

all: $(MEDIA_DEST) $(TRANSCRIPT_DEST) $(MERGE_DEST)

download:
	$(BASEDIR)/scraper/ingest_xml.py --inbox-dir "$(INBOXDIR)" --proceedings-dir "$(PROCEEDINGSDIR)"
	$(BASEDIR)/scraper/fetch_media.py --inbox-dir "$(MEDIAINBOX)" --media-dir "$(MEDIADIR)"

update: download all

debug:
	-echo "MEDIA_DEST: $(MEDIA_DEST)"
	-echo "MERGE_DEST: $(MERGE_DEST)"

%-merged.json: ../../original/proceedings/%-proceedings.json ../../original/media/%-media.json
	$(BASEDIR)/merger/merge_session.py --output=$(MERGEDDIR) $^

%-media.json: raw-%-media.json
	$(BASEDIR)/parsers/media2json.py $< > $@

%-proceedings.json: %-proceedings.xml
	$(BASEDIR)/parsers/proceedings2json.py $< > $@

clean:
	$(RM) $(MEDIADIR)/[0-9]*.json
	$(RM) $(PROCEEDINGSDIR)/*.json
	$(RM) $(PROCEEDINGSDIR)/*.sha
	$(RM) $(MERGEDDIR)/*.json
