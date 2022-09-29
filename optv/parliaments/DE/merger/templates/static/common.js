// js module with common functions
// for dashboard and transcript

// Debounce function
const debounce = (callback, wait) => {
  let timeoutId = null;
  return (...args) => {
    window.clearTimeout(timeoutId);
    timeoutId = window.setTimeout(() => {
      callback.apply(null, args);
    }, wait);
  };
};

// Return list of available session data (as session numbers)
let get_session_list = () => {
    let basedir = get_basedir();
    if (basedir.includes('raw.githubusercontent')) {
        // gh-pages deployment. We use the Contents API to get the file listing.
        return fetch('https://api.github.com/repos/openparliamenttv/OpenParliamentTV-Data-DE/contents/processed')
              .then(resp => resp.json())
              .then(dircontent => dircontent.map(item => item.name));
    } else {
        return fetch(`${basedir}/processed`)
            .then(resp => resp.text())
            .then(dircontent => [ ...new Set([ ...dircontent.matchAll(/(\d+-merged.json)/g) ].map(a => a[0])) ]);
    }
};

// Return the basedir for the content data
let get_basedir = () => {
    if (location.host.includes('github.io')) {
        // Hardcoding the URL here.
        return 'https://raw.githubusercontent.com/OpenParliamentTV/OpenParliamentTV-Data-DE/main/';
    } else {
        // Localhost server deployment.
        // We assume that the OpenParliamentTV-Tools and
        // OpenParliamentTV-Data-DE clones are in the same directory
        return '../../../../../../../OpenParliamentTV-Data-DE/';
    }
};

// Get the session data URL
let get_session_url = (session, version) => {
    let basedir = get_basedir();
    if (!version)
        version = 'processed';
    return `${basedir}/${version}/${session}-merged.json`;
}
// Get the media data URL
let get_media_url = (session) => {
    let basedir = get_basedir();
    return `${basedir}/original/media/${session}-media.json`;
}
// Get the proceedings data URL
let get_proceedings_url = (session) => {
    let basedir = get_basedir();
    return `${basedir}/original/proceedings/${session}-data.json`;
}

let normalized_data = (data) => {
    return data.map(s => {
        let pi = s.agendaItem.proceedingIndex || 1000;
        pi = pi - (pi >= 1000 ? 1000 : 0);
        let sn = `${s.session.number}`.padStart(3, "0");
        let session = `${s.electoralPeriod.number}${sn}`;
        return {
            "proceeding": pi,
            "media": (s.agendaItem.mediaIndex || 0),
            "title": s.agendaItem.officialTitle,
            "speaker": s.people[0].label,
            "url": `#speech${s.agendaItem.speechIndex}`,
            "matching": (pi == 0 ? 'media_only' : ((s.agendaItem.mediaIndex || 0) == 0 ? 'proceeding_only' : 'matching')),
            "char_count": s.textContents ? d3.sum(s.textContents.map(tc => d3.sum(tc.textBody.map(tb => tb.text.length)))) : 0,
            "word_count": s.textContents ? d3.sum(s.textContents.map(tc => d3.sum(tc.textBody.map(tb => tb.text.split(' ').length)))) : 0,
            "duration": s.media ? s.media.duration : 0,
            "version": s.version,
            "session": session
        }
    });
};

