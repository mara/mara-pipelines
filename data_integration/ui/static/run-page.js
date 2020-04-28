/**
 * Visualization of an ongoing pipeline run
 *
 * @param baseUrl The url prefix of the data integration blueprint
 * @param streamUrl The url for pulling pipeline events
 * @param nodePath The path of the node
 */
function processRunEvents(baseUrl, streamUrl, nodePath) {

    /** the area where all text messages go to */
    var mainOutputArea = $('#main-output-area');

    /** cards of all currently running tasks */
    var cardsForRunningTasks = [];

    /** start times of each node (in browser time) */
    var nodeStartTimes = [];

    /** run time  and status of all nodes (times in server time) */
    var nodes = {};


    /** Scrolling output areas to new output synchronously would be too much work for the browser.
     * Therefore, keep track of divs that need scrolling and handle them in the timer below.
     * Contains a set of jquery selectors
     */
    var scrollContainersWithNewOutput = new Set();


    /** flag to stop the redrawing cycle */
    var pipelineIsRunning = true;

    /** visualization of system statistics */
    var systemStatsChart = new SystemStatsChart('system-stats-chart');

    /** show new data in ui */
    var updateUI = function () {
        var now = new Date();

        // update end time of running cards
        for (var node_id in nodes) {
            if (nodes[node_id].status == 'running') {
                nodes[node_id].end = now;
            }
        }

        // update run time on cards
        for (var id in cardsForRunningTasks) {
            cardsForRunningTasks[id].find('.card-header-right > i').html(
                Math.floor((now - nodeStartTimes[id]) / 1000) + ' seconds');
        }
        // scroll down all output containers with new output
        for (var divId of scrollContainersWithNewOutput) {
            var div = $(divId);
            if (div[0]) {
                div.clearQueue();
                div.animate({scrollTop: div[0].scrollHeight}, 100, 'linear');
            }
        }
        scrollContainersWithNewOutput.clear();

        // update timeline
        drawTimelineChart('timeline-chart', Object.values(nodes));

        // update the UI asynchronously, only when resources allow
        if (pipelineIsRunning) {
            setTimeout(function () {
                window.requestAnimationFrame(updateUI);
            }, 500)

        }
    };

    window.requestAnimationFrame(updateUI);


    /** processes events from the running pipeline */
    var source = new EventSource(streamUrl);

    source.addEventListener('Output', function (e) {
        var output = JSON.parse(e.data);
        var prefix = nodeLinks(baseUrl, output.node_path, nodePath.length, true);
        var id = output.node_path.join('_');
        var message = formatNodeOutput(output.message, output.format, output.is_error);

        mainOutputArea.append($('<div/>').append(prefix.clone()).append(message.clone()));
        scrollContainersWithNewOutput.add('#main-output-area');

        if (id in cardsForRunningTasks) {
            cardsForRunningTasks[id].find('.run-output').append($('<div/>').append(message.clone()));
            scrollContainersWithNewOutput.add('#' + id + ' .run-output');
        }
    }, false);

    source.addEventListener('NodeStarted', function (e) {
        var event = JSON.parse(e.data);
        if (event.node_path.length == nodePath.length) {
            return;
        }
        var id = event.node_path.join('_');

        if (!event.is_pipeline) {
            var card = $('#card-template').clone();
            card.attr('id', id);
            card.find('.card-header-left').html(nodeLinks(baseUrl, event.node_path, nodePath.length, false));
            card.find('.card-header-right').append($('<i/>').text('0 seconds'));

            $('#running-tasks-container').append(card);
            card.show();

            cardsForRunningTasks[id] = card;
        }
        var now = new Date();
        nodeStartTimes[id] = now;

        nodes[id] = {
            label: event.node_path.slice(nodePath.length).join(' / '),
            status: 'running',
            type: event.is_pipeline ? 'pipeline' : 'node',
            url: baseUrl + '/' + event.node_path.join('/'),
            start: now,
            end: now
        };

    }, false);

    source.addEventListener('NodeFinished', function (e) {
        var event = JSON.parse(e.data);
        if (event.node_path.length == nodePath.length) {
            return;
        }

        var id = event.node_path.join('_');
        var now = new Date();
        nodes[id].status = event.succeeded ? 'succeeded' : 'failed';
        nodes[id].end = now;

        if (id in cardsForRunningTasks) {
            card = cardsForRunningTasks[id].detach();
            if (!event.succeeded) {
                card.find('.card-header-right').css('color', 'red');
                $('#failed-tasks-container').append(card);
                var div = card.find('.run-output');
                div.scrollTop(div[0].scrollHeight);
            }
            delete cardsForRunningTasks[id];
        }
    }, false);


    source.addEventListener('RunStarted', function (e) {
        var event = JSON.parse(e.data);
    }, false);

    source.addEventListener('RunFinished', function (e) {
        $('span.action-buttons > *').css('display', 'inline-block');
        pipelineIsRunning = false;
    }, false);

    source.addEventListener('SystemStatistics', function (e) {
        var event = JSON.parse(e.data);
        systemStatsChart.addMeasurements([[
            event.timestamp, event.cpu_usage, event.disc_read, event.disc_write,
            event.net_recv, event.net_sent, event.mem_usage, event.swap_usage, event.iowait]]);
    }, false);


// close the source on the first error (should never happen)
    source.addEventListener('error', function (e) {
        source.close();
    }, false);

    function resize() {
        // adapt main output area height
        var height = $(window).height() - mainOutputArea.offset().top
            - (mainOutputArea.parent().parent().outerHeight() - mainOutputArea.outerHeight());
        mainOutputArea.css('height', height);
        mainOutputArea.css('min-height', height);
    }

    $(window).resize(resize);

    resize();
}