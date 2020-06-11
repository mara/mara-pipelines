/** Event handlers and interactivity for node pages */

/** Updates the run button urls at the top of a pipeline node list */
function PipelineRunButtons() {
    var self = this;
    self.runWithUpstreamsUrl = $('#run-with-upstreams-button').attr('href');
    self.runUrl = $('#run-button').attr('href');

    function update() {
        nodes = '';
        $('.pipeline-node-checkbox').each(function (_, input) {
            if (input.checked) {
                nodes += '/' + input.value;
            }
        });
        $('#run-with-upstreams-button').attr('href', self.runWithUpstreamsUrl + nodes);
        $('#run-button').attr('href', self.runUrl + nodes);
    }

    update();

    return {update: update};
}


/** Manages the content of the #last-runs-card div */
function NodePage(baseUrl, nodePath) {
    var self = this;
    self.runId = null;

    function loadOutput(nodeUrlPath, limited) {
        $('#run-output').css('height', $('#run-output').height());
        $('#run-output').empty().append(spinner());
        loadContentAsynchronously('run-output', baseUrl
            + (nodeUrlPath ? '/' + nodeUrlPath : '')
            + '/run-output' + (limited ? '-limited' : '') + (self.runId ? '/' + self.runId : ''));
    }

    function loadSystemStats(nodeUrlPath) {
        $('#system-stats').css('height', $('#system-stats').height());
        $('#system-stats').empty().append(spinner());
        loadContentAsynchronously('system-stats', baseUrl
            + (nodeUrlPath ? '/' + nodeUrlPath : '')
            + '/system-stats' + (self.runId ? '/' + self.runId : ''));
    }

    function loadTimeline(nodeUrlPath) {
        $('#timeline-chart').css('height', $('#system-stats').height());
        $('#timeline-chart').empty().append(spinner());
        loadContentAsynchronously('timeline-chart', baseUrl
            + (nodeUrlPath ? '/' + nodeUrlPath : '')
            + '/timeline-chart' + (self.runId ? '/' + self.runId : ''));
    }

    return {
        showOutput: function (output, nodeUrlPath, outputLongerThanLimit) {
            var lines = [];
            var path, message, format, is_error;
            for (var i in output) {
                [path, message, format, is_error] = output[i];
                lines.push($('<div/>').append(nodeLinks(baseUrl, path, nodePath.length, true))
                    .append(formatNodeOutput(message, format, is_error)));
            }

            $('#run-output').addClass('run-output');
            $('#run-output').empty().append(lines);

            if (outputLongerThanLimit) { // the output is too long to be shown in full by default
                $('#run-output')
                    .append($('<div><span>...</div></div>')
                        .append($('<div/>').append($('<a href="#">Show all</a>').click(function () {
                            loadOutput(nodeUrlPath, false);
                            return false;
                        }))));
            }
        },

        showSystemStats: function (data) {
            var systemStatsChart = new SystemStatsChart('system-stats-chart');
            systemStatsChart.addMeasurements(data);
        },

        switchRun: function (newRunId, nodeUrlPath) {
            self.runId = newRunId;
            $('#last-runs-selector').val(self.runId);
            loadSystemStats(nodeUrlPath);
            loadTimeline(nodeUrlPath);
            loadOutput(nodeUrlPath, true);
        }
    }
}




