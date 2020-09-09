/** Utility functions that are needed both on node pages and the run page */


/** Creates links to a node and all its parents */
function nodeLinks(baseUrl, nodePath, startLevel, addColonAtEnd) {
    var node_url = baseUrl;
    var span = $('<span/>');
    for (var i in nodePath) {
        node_url += '/' + nodePath[i];
        if (i >= startLevel) {
            span.append($('<a>').attr('href', node_url).append(nodePath[i]));
            if (i < nodePath.length - 1) {
                span.append(' / ');
            } else if (addColonAtEnd) {
                span.append(': ');
            }
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



/**
 * Computes meaningful tick positions for a duration axis based on time range and available space
 * @param availableSpace The width or height of the chart axis in pixels
 * @param minTickDistance the minimum distance between two ticks in pixels
 * @param totalDuration The overal length of the time range in ms
 * @returns {*} An array of `[position, duration, label]` entries
 */
function durationAxisTicks(availableSpace, minTickDistance, totalDuration) {

    // generate a tick at every multiple of divisor, stop at max width
    function generate_ticks(multiple, divisor, suffix) {
        var result = [];
        var t = 0;
        while (true) {
            result.push([Math.round(availableSpace * t / totalDuration), t, t / divisor + suffix]);
            if (t > totalDuration) {
                break;
            }
            t += multiple * divisor;
        }
        return result;
    }

    var magnitudes = [[10, 1, 'ms'], [20, 1, 'ms'], [50, 1, 'ms'], [100, 1, 'ms'], [200, 1, 'ms'],
        [1, 1000, 's'], [2, 1000, 's'], [5, 1000, 's'], [10, 1000, 's'], [20, 1000, 's'],
        [1, 60 * 1000, 'm'], [2, 60 * 1000, 'm'], [5, 60 * 1000, 'm'], [10, 60 * 1000, 'm'], [20, 60 * 1000, 'm'],
        [1, 60 * 60 * 1000, 'h'], [2, 60 * 60 * 1000, 'h'], [4, 60 * 60 * 1000, 'h'], [12, 60 * 60 * 1000, 'h']];

    for (var i in magnitudes) {
        [multiple, divisor, suffix] = magnitudes[i];
        if (minTickDistance * totalDuration / (multiple * divisor) < availableSpace) {
            return generate_ticks(multiple, divisor, suffix);
        }
    }
    // by default, display days
    return generate_ticks(1, 24 * 60 * 60 * 1000, 'd');

}

