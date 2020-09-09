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
        durationAxisTicks(availableWidth, 75, (maxTime - minTime)).forEach(function(tick) {
            axis.append($('<div class="timeline-axis-label"/>').css('left', tick[0] + 'px').text(tick[2]));
        });
        container.append(axis);
        div.find('[data-toggle="tooltip"]').tooltip('dispose');
        div.empty().append(container);
        $('[data-toggle="tooltip"]').tooltip();
    }

    draw();
    window.requestAnimationFrame(draw);

    $(window).resize(function () {
        draw();
    });
}


