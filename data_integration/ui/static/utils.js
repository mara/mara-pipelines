/** Utility functions that are needed both on node pages and the run page */


/** Creates links to a node and all its parents */
function nodeLinks(baseUrl, nodePath, addColonAtEnd) {
    var node_url = baseUrl;
    var span = $('<span/>');
    for (var i in nodePath) {
        node_url += '/' + nodePath[i];
        span.append($('<a>').attr('href', node_url).append(nodePath[i]));
        if (i < nodePath.length - 1) {
            span.append(' / ');
        } else if (addColonAtEnd) {
            span.append(': ');
        }
    }
    return span;
}

/** Applies formatting to node output */
function formatNodeOutput(message, format, is_error) {
    return $('<span>').addClass(format).addClass(is_error ? 'error' : '').text(message);
}

/**
 * Formats a duration in human readable form
 * Examples:
 *   120: 120ms
 *   5600: 5.6s
 *   70000: 1:10m
 *   4000000: 1:06h

 * @param duration A duration in milliseconds
 * @returns {string}
 */
function formatDuration(duration) {
    if (duration in formatDuration.cache) {
        return formatDuration.cache[duration];
    }
    var original_duration = duration;
    var hours = Math.floor(duration / 3600000);
    duration -= 3600000 * hours;

    var minutes = Math.floor(duration / 60000);
    duration -= 60000 * minutes;

    var seconds = Math.floor(duration / 1000);
    duration -= 1000 * seconds;

    var milliseconds = duration;

    var result = '';
    if (hours) {
        result = hours + ':' + ('0' + minutes).slice(-2) + 'h';
    } else if (minutes) {
        result = minutes + ':' + ('0' + seconds).slice(-2) + 'm';
    } else if (seconds) {
        result = seconds + '.' + ('0' + Math.round(milliseconds / 100)).slice(-1) + 's';
    } else {
        result = milliseconds + 'ms';
    }

    formatDuration.cache[original_duration] = result;
    return result;
}

formatDuration.cache = {};
