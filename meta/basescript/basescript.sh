#! /bin/sh

BASEDIR=$(dirname "$(readlink -f "$0")")
DATADIR=/var/www/html/OpenParliamentTV-Parsers/parliaments/DE
LOCKFILE=/var/lock/optv-update.lock
LOGFILE=${BASEDIR}/optv-update.log

log () {
   echo "$@"
   echo "$(date -Is) $*" >> "$LOGFILE"
}

(
    flock -n 9 || { log "Update already running. Exiting" ; exit 1 ; }
    # The rest of the commands is executed under lock
    log "Starting update"
    cd "$DATADIR" || { log "Cannot cd to $DATADIR. Exiting" ; exit 1 ; }
    python3 update_and_merge.py --save-raw-data --from-period=20 --second-stage-matching --advanced-rematch --retry-count=20 data >> "$LOGFILE" 2>&1
    cd "/var/www/html/OpenParliamentTV-Alignment/" || { log "Cannot cd to alignment dir"; exit 1 ; }
    log "Starting index_local"
    php /var/www/html/OpenParliamentTV-Alignment/index_local.php >> "$LOGFILE" 2>&1

    rm "$LOCKFILE"
) 9>${LOCKFILE}
