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
            .then(dircontent => [ ...new Set([ ...dircontent.matchAll(/(\d+-session.json)/g) ].map(a => a[0])) ]);
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
        return '../../../../../OpenParliamentTV-Data-DE/';
    }
};

// Get the session data URL
let get_session_url = (session, version) => {
    let basedir = get_basedir();
    if (!version)
        version = 'processed';
    return `${basedir}/${version}/${session}-session.json`;
}
// Get the media data URL
let get_media_url = (session) => {
    let basedir = get_basedir();
    return `${basedir}/original/media/${session}-media.json`;
}
// Get the proceedings data URL
let get_proceedings_url = (session) => {
    let basedir = get_basedir();
    return `${basedir}/original/proceedings/${session}-proceedings.json`;
}

let normalized_data = (data) => {
    return data.map(item => {
        let pis = item.agendaItem.proceedingIndexes;
        if (pis === undefined) {
            pis = [ item.agendaItem.proceedingIndex || 0 ];
        };
        // Normalize proceedingIndex (starting at 0 rather than 1000)
        pis = pis.map(pi => pi - (pi >= 1000 ? 1000 : 0));

        let sn = `${item.session.number}`.padStart(3, "0");
        let session = `${item.electoralPeriod.number}${sn}`;

        return pis.map(pi => ({
            "proceeding": pi,
            "media": (item.agendaItem.mediaIndex || 0),
            "title": item.agendaItem.officialTitle,
            "speaker": item.people[0].label || "",
            "url": `#speech${item.agendaItem.speechIndex}`,
            "matching": (pi == 0 ? 'media_only' : ((item.agendaItem.mediaIndex || 0) == 0 ? 'proceeding_only' : 'matching')),
            "char_count": item.textContents ? d3.sum(item.textContents.map(tc => d3.sum(tc.textBody.map(tb => tb.text.length)))) : 0,
            "word_count": item.textContents ? d3.sum(item.textContents.map(tc => d3.sum(tc.textBody.map(tb => tb.text.split(' ').length)))) : 0,
            "duration": item.media ? item.media.duration : 0,
            "version": item.version,
            "session": session,
            "data": item
        }));
    }).flat();
};

// Fast Levenshtein implementation
// from https://github.com/gustf/js-levenshtein/blob/master/index.js
function _min(d0, d1, d2, bx, ay)
{
    return d0 < d1 || d2 < d1
        ? d0 > d2
        ? d2 + 1
        : d0 + 1
        : bx === ay
        ? d1
        : d1 + 1;
}

function levenshtein(a, b)
{
    if (a === b) {
        return 0;
    }

    if (a.length > b.length) {
        var tmp = a;
        a = b;
        b = tmp;
    }

    var la = a.length;
    var lb = b.length;

    while (la > 0 && (a.charCodeAt(la - 1) === b.charCodeAt(lb - 1))) {
        la--;
        lb--;
    }

    var offset = 0;

    while (offset < la && (a.charCodeAt(offset) === b.charCodeAt(offset))) {
        offset++;
    }

    la -= offset;
    lb -= offset;

    if (la === 0 || lb < 3) {
        return lb;
    }

    var x = 0;
    var y;
    var d0;
    var d1;
    var d2;
    var d3;
    var dd;
    var dy;
    var ay;
    var bx0;
    var bx1;
    var bx2;
    var bx3;

    var vector = [];

    for (y = 0; y < la; y++) {
        vector.push(y + 1);
        vector.push(a.charCodeAt(offset + y));
    }

    var len = vector.length - 1;

    for (; x < lb - 3;) {
        bx0 = b.charCodeAt(offset + (d0 = x));
        bx1 = b.charCodeAt(offset + (d1 = x + 1));
        bx2 = b.charCodeAt(offset + (d2 = x + 2));
        bx3 = b.charCodeAt(offset + (d3 = x + 3));
        dd = (x += 4);
        for (y = 0; y < len; y += 2) {
            dy = vector[y];
            ay = vector[y + 1];
            d0 = _min(dy, d0, d1, bx0, ay);
            d1 = _min(d0, d1, d2, bx1, ay);
            d2 = _min(d1, d2, d3, bx2, ay);
            dd = _min(d2, d3, dd, bx3, ay);
            vector[y] = dd;
            d3 = d2;
            d2 = d1;
            d1 = d0;
            d0 = dy;
        }
    }

    for (; x < lb;) {
        bx0 = b.charCodeAt(offset + (d0 = x));
        dd = ++x;
        for (y = 0; y < len; y += 2) {
            dy = vector[y];
            vector[y] = dd = _min(dy, d0, dd, bx0, vector[y + 1]);
            d0 = dy;
        }
    }

    return dd;
};
