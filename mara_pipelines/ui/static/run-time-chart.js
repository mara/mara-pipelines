/**
 * Displays the runtime of a node and its children over time
 * @param divId The id in which to render the chart
 * @param nodeUrlPath The url path of the current pipeline node, e.g. '/foo/bar'
 * @param runs
 */
function drawRunTimeChart(divId, nodeUrlPath, runs) {
    /* Load the Visualization API and the corechart package */
    google.charts.load('current', {'packages': ['corechart']});

    /* Set a callback to run when the Google Visualization API is loaded */
    google.charts.setOnLoadCallback(initializeChart);

    var options = {
        // no margins
        chartArea: {left: 0, top: 0, width: '100%', height: '100%'},
        legend: {position: 'off'},
        curveType: 'function',
        fontName: '"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif',
        fontSize: '14',
        focusTarget: "datum",
        tooltip: {showColorCode: true, textStyle: {bold: false, color: '#333'}},
        series: {
            0: {lineWidth: 1.5, color: '#0275d8'}
        },
        interpolateNulls: true,

        hAxis: {
            format: 'MMM d', textPosition: 'in', gridlines: {color: 'transparent'}, textStyle: {color: '#888'}
        },
        vAxis: {
            textPosition: 'in', format: 'short', gridlines: {color: 'transparent'},
            textStyle: {color: '#888'},
            baselineColor: 'transparent', viewWindow: {min: 0},
            ticks: []
        }
    };

    var chart = null;
    var data = null;

    function initializeChart() {
        data = new google.visualization.DataTable();
        data.addColumn('datetime', 'Time');

        data.addColumn('number', runs[0].node_name);
        data.addColumn({type: 'boolean', role: 'certainty'});

        if (runs[0].child_names) {
            var number_of_child_nodes = runs[0].child_names.length;
            var colors = new KolorWheel("#008800").rel(-50, -100, 40, number_of_child_nodes);

            for (var i = 0; i < number_of_child_nodes; i++) {
                data.addColumn('number', runs[0].child_names[i]);
                data.addColumn({type: 'boolean', role: 'certainty'});

                options.series[i + 1] = {lineWidth: 1, color: colors.get(i).getHex()};
            }
        }

        // the run IDs for each row
        var runIDs = [];
        for (var n in runs) {
            var run = runs[n];
            runIDs.push(run.run_id);
            var row = [new Date(run.start_time)];

            function add_node(node_run) {
                if (node_run) {
                    var label = (node_run.succeeded ? 'succeeded' : 'failed') + ', ' + formatDuration(1000 * node_run.duration);
                    row = row.concat([{v: node_run.duration, f: label}, node_run.succeeded]);
                } else {
                    row = row.concat([null, null]);
                }
            }

            add_node(run.node_run);
            for (var child in run.child_runs) {
                add_node(run.child_runs[child]);
            }
            data.addRow(row);
        }

        durationAxisTicks($('#' + divId).innerHeight(), 40, data.getColumnRange(1).max * 1000)
            .forEach(function (tick) {
                options.vAxis.ticks.push({v: tick[1] / 1000, f: tick[2]});
            });


        chart = new google.visualization.LineChart(document.getElementById(divId));
        chart.draw(data, options);
        google.visualization.events.addListener(chart, 'select', function () {
            var selectedItem = chart.getSelection()[0];
            if (selectedItem) {
                var runID = runIDs[selectedItem.row];
                nodePage.switchRun(runID, nodeUrlPath);
                $('html, body').animate({
                    scrollTop: $('#last-runs-card').offset().top - 73,
                    scrollLeft: 0
                });
            }
        });

        $(window).resize(function () {
            chart.draw(data, options);
        });
    }
}


