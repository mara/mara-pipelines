/** Displays cpu & memory usage and io metrics that are collected during an ETL run */

var SystemStatsChart = function (divId) {
    // Load the Visualization API and the corechart package
    google.charts.load('current', {'packages': ['corechart']});

    // Set a callback to run when the Google Visualization API is loaded
    google.charts.setOnLoadCallback(initializeChart);

    /** the data container and chart object, will be set by initialize-chart */
    var data = null;
    var chart = null;

    /** Rows that arrived before that chart was initialized */
    var initialRows = [];

    var options = {
        // no margins
        chartArea: {left: 0, top: 18, width: '100%', height: '100%'},

        // legend at the top
        legend: {position: 'top', textStyle: {color: '#888'}},

        fontName: '"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif',
        fontSize: '14',

        // less heavy tooltips
        tooltip: {showColorCode: true, textStyle: {bold: false, color: '#333'}},

        // curved lines
        curveType: 'function',

        // show a combined tooltips for all metrics
        focusTarget: "category",

        // assign different axes and style to each series
        series: {
            0: {targetAxisIndex: 0, color: '#0275d8', lineWidth: 1.5}, // CPU
            1: {targetAxisIndex: 1, color: '#008800', lineWidth: 1, lineDashStyle: [2, 2]}, // Disc read
            2: {targetAxisIndex: 1, color: '#008800', lineWidth: 1, lineDashStyle: [6, 4]}, // Disc write
            3: {targetAxisIndex: 1, color: '#880000', lineWidth: 1, lineDashStyle: [2, 2]}, // Net received
            4: {targetAxisIndex: 1, color: '#880000', lineWidth: 1, lineDashStyle: [6, 4]}, // Net sent
            5: {targetAxisIndex: 0, color: '#008800', lineWidth: 1},// Mem
            6: {targetAxisIndex: 0, color: 'pink', lineWidth: 1}, // Swap
            7: {targetAxisIndex: 0, color: 'cyan', lineWidth: 1, lineDashStyle: [2, 2]} // Iowait
        },

        vAxis: {
            textPosition: 'none',
            gridlines: {color: 'transparent'},
            baselineColor: 'transparent',
            viewWindow: {min: 0}
        },

        hAxis: {
            textPosition: 'in',
            gridlines: {color: 'transparent'},
            textStyle: {color: '#888'}
        }

    };

    /** called when the google charts api is loaded */
    function initializeChart() {
        data = new google.visualization.DataTable();
        data.addColumn('datetime', 'Time');
        var measures = ['CPU', 'Disc read', 'Disc write', 'Net in', 'Net out', 'Mem', 'Swap', 'IOWait'];
        for (i in measures) {
            data.addColumn('number', measures[i]);
            data.addColumn({type: 'string', role: 'tooltip'});
        }

        if (initialRows) {
            data.addRows(initialRows);
        }

        chart = new google.visualization.LineChart(document.getElementById(divId));
        chart.draw(data, options);

        $(window).resize(function () {
            chart.draw(data, options);
        });

    }


    return {
        /** Adds a list of measurements to the chart */
        addMeasurements: function (rows) {
            var processed_rows = rows.map(function (row) {
                [timestamp, cpuUsage, discRead, discWrite, netRecv, netSent, memUsage, swapUsage, ioWait] = row;
                return [new Date(timestamp),
                    cpuUsage, cpuUsage != null ? cpuUsage.toFixed(1) + ' %' : '',
                    discRead, discRead != null ? discRead.toFixed(1) + ' MB/s' : '',
                    discWrite, discWrite != null ? discWrite.toFixed(1) + ' MB/s' : '',
                    netRecv, netRecv != null ? netRecv.toFixed(1) + ' MB/s' : '',
                    netSent, netSent != null ? netSent.toFixed(1) + ' MB/s' : '',
                    memUsage, memUsage != null ? memUsage.toFixed(1) + ' %' : '',
                    swapUsage, swapUsage != null ? swapUsage.toFixed(1) + ' %' : '',
                    ioWait, ioWait != null ? ioWait.toFixed(1) + ' %' : '']
            });
            if (data) {
                data.addRows(processed_rows);
                chart.draw(data, options);
            } else {
                initialRows = processed_rows;
            }
        }
    };
};

