MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BASEDIR := $(dir $(MAKEFILE_PATH))

DATADIR := $(BASEDIR)../../../../OpenParliamentTV-Data-DE
MEDIADIR := $(DATADIR)/original/media
PROCEEDINGSDIR := $(DATADIR)/original/proceedings
MERGEDDIR := $(DATADIR)/cache/merged

TRANSCRIPT_SOURCE := $(wildcard $(PROCEEDINGSDIR)/*.xml)
# MEDIA_SOURCE filelist will be updated by download
MEDIA_SOURCE := $(wildcard $(MEDIADIR)/raw-*-media.json)

TRANSCRIPT_DEST := $(patsubst %.xml,%.json,$(TRANSCRIPT_SOURCE))
# MEDIA_DEST filelist is build using MEDIA_SOURCE
MEDIA_DEST := $(subst raw-,,$(MEDIA_SOURCE))

# The following line requires that each media file has a corresponding proceedings file
#MERGE_DEST := $(subst /media/,/merged/,$(patsubst %-proceedings.json,%-merged.json,$(MEDIA_DEST)))

# The following line uses proceedings as reference
MERGE_DEST := $(subst /original/proceedings/,/cache/merged/,$(patsubst %-proceedings.json,%-merged.json,$(TRANSCRIPT_DEST)))

all: $(MEDIA_DEST) $(TRANSCRIPT_DEST) $(MERGE_DEST)

download:
	$(BASEDIR)/scraper/fetch_proceedings.py "$(PROCEEDINGSDIR)"
	# Fetch media files from period
	$(BASEDIR)/scraper/update_media.py --from-period 20 "$(MEDIADIR)" --save-raw-data
	# Fetch media files corresponding to proceedings files
	# $(BASEDIR)/scraper/update_media --from-proceedings "$(PROCEEDINGSDIR)" "$(MEDIADIR)" --save-raw-data

forcemerge:
	$(BASEDIR)/merger/merge_session.py --include-all-proceedings --second-stage-matching --advanced-rematch --output="$(MERGEDDIR)" "$(PROCEEDINGSDIR)" "$(MEDIADIR)"

update: download all

debug:
	-echo "MEDIA_DEST"
	-echo $(MEDIA_DEST)
	-echo "MERGE DEST"
	-echo $(MERGE_DEST)

%-merged.json: ../original/proceedings/%-proceedings.json ../original/media/%-media.json
	$(BASEDIR)/merger/merge_session.py --output=$(DATADIR)/merged $^

%-media.json: %-media.xml
	$(BASEDIR)/parsers/media2json.py $< > $@

%-media.json: raw-%-media.json
	$(BASEDIR)/parsers/media2json.py $< > $@

%-proceedings.json: %-proceedings.xml
	$(BASEDIR)/parsers/proceedings2json.py --include-nas $< > $@

clean:
	$(RM) $(MEDIADIR)/[0-9]*.json
	$(RM) $(PROCEEDINGSDIR)/*.json
	$(RM) $(MERGEDDIR)/*.json
