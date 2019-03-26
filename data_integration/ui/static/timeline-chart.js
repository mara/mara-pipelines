/**
 * Draws a timeline of parallel nodes in html
 *
 * @param divId The dom element into which to draw the chart
 * @param nodes An array of dictionaries with the following keys:
 *  - label: The name of the node
 *  - status: 'running', 'succeeded' or 'failed
 *  - type: 'task' or 'pipeline'
 *  - url: the url of the node
 *  - start: the timestamp when the node was started
 *  - end: the timestamp when the node run finished
 */
function drawTimelineChart(divId, nodes) {
    if (nodes.length == 0) {
        return;
    }

    // turn timestamps into `Date` objects
    for (var i in nodes) {
        nodes[i].start = new Date(nodes[i].start);
        nodes[i].end = new Date(nodes[i].end);
    }

    // get time range
    var minTime = Math.min.apply(null, nodes.map(function (x) {
        return x.start
    }));

    var maxTime = Math.max.apply(null, nodes.map(function (x) {
        return x.end
    }));

    // sort nodes by start time
    function sortNodes(nodes) {

        return nodes.sort(function (t1, t2) {
            return t1.start - t2.start;
        });
    }

    // separate pipelines and tasks
    pipelines = sortNodes(nodes.filter(function (node) {
        return node.type == 'pipeline'
    }));

    tasks = sortNodes(nodes.filter(function (node) {
        return node.type != 'pipeline'
    }));

    /** distribute nodes across multiple lines to avoid overlap */
    function distributeNodes(nodes) {
        var lines = [];
        for (var i in nodes) {
            var found = false;
            for (var j in lines) {
                if (!found && lines[j][lines[j].length - 1].end <= nodes[i].start) {
                    found = true;
                    lines[j].push(nodes[i]);
                }
            }
            if (!found) {
                lines.push([nodes[i]]);
            }
        }
        return lines;
    }

    taskLines = distributeNodes(tasks);
    pipelineLines = distributeNodes(pipelines);


    var div = $('#' + divId).css('width', '100%');

    /** draw lines */
    function draw() {
        availableWidth = div.innerWidth();

        var container = $('<div class="timeline"/>');

        function drawLines(lines) {
            for (var i in lines) {
                var lineDiv = $('<div class="timeline-line">&nbsp;</div>');
                for (var j in lines[i]) {
                    node = lines[i][j];
                    lineDiv.append($('<a/>')
                        .addClass('timeline-node')
                        .addClass('status-' + node.status)
                        .addClass('type-' + node.type)
                        .attr('href', node.url)
                        .attr('title', node.label + ':\n ' + formatDuration(node.end - node.start))
                        .attr('data-toggle', 'tooltip').attr('data-container', 'body').attr('data-placement', 'bottom')
                        .css('left', ((node.start - minTime) / (maxTime - minTime)) * availableWidth + 'px')
                        .css('width', ((node.end - node.start) / (maxTime - minTime)) * availableWidth + 'px')
                        .append(node.label));
                }
                container.append(lineDiv);
            }
        }

        drawLines(taskLines);
        drawLines(pipelineLines);


        var axis = $('<div class="timeline-axis">&nbsp;</div>');
        var _ticks = ticks(availableWidth, (maxTime - minTime));
        for (pos in _ticks) {
            // container.append($('<div class="timeline-grid-line"/>').css('left', pos + 'px'));
            axis.append($('<div class="timeline-axis-label"/>').css('left', pos + 'px').text(_ticks[pos]));
        }
        container.append(axis);
        div.find('[data-toggle="tooltip"]').tooltip('dispose');
        div.empty().append(container);
        $('[data-toggle="tooltip"]').tooltip();
    }

    draw();

    $(window).resize(function () {
        draw();
    });
}


/**
 * Computes meaningful tick positions for the time axis based on time range and available width
 * @param availableWidth The width of the chart in pixels
 * @param totalDuration The overal length of the time range in ms
 * @returns {*} An dictionary of `{position: label}` entries
 */
function ticks(availableWidth, totalDuration) {

    // generate a tick at every multiple of divisor, stop at max width
    function generate_ticks(multiple, divisor, suffix) {
        var result = {};
        var t = 0;
        while (true) {
            if (t > totalDuration) {
                break;
            }
            result[Math.round(availableWidth * t / totalDuration)] = t / divisor + suffix;
            t += multiple * divisor;
        }
        return result;
    }

    // the minimum distance between two ticks in pixels
    var minTickDistance = 75;

    var magnitudes = [[10, 1, 'ms'], [20, 1, 'ms'], [50, 1, 'ms'], [100, 1, 'ms'], [200, 1, 'ms'],
        [1, 1000, 's'], [2, 1000, 's'], [5, 1000, 's'], [10, 1000, 's'], [20, 1000, 's'],
        [1, 60 * 1000, 'm'], [2, 60 * 1000, 'm'], [5, 60 * 1000, 'm'], [10, 60 * 1000, 'm'], [20, 60 * 1000, 'm']];

    for (var i in magnitudes) {
        [multiple, divisor, suffix] = magnitudes[i];
        if (minTickDistance * totalDuration / (multiple * divisor) < availableWidth) {
            return generate_ticks(multiple, divisor, suffix);
        }
    }
    // by default, display hours
    return generate_ticks(1, 60 * 60 * 1000, 'h');

}

