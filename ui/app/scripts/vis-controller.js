/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * The MIT License (MIT)
 *
 * Copyright (C) 2016-2017 Appsicle
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR
 * ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

/*globals jQuery:false */
/*globals qdb:false */
/*globals echarts:false */
/*globals eChartsMacarons:false */

(function ($) {
    'use strict';
    const panels = $('.js-vis-panel');
    const columnContainer = panels.find('.vis-columns');
    const canvas = panels.find('#vis-canvas');
    const menu = panels.find('#vis-menu');
    const forms = panels.find('#vis-forms');
    const footer = $('.footer')[0];
    const seriesList = $('#vis-series-list');
    const ulSeriesList = seriesList.find('ul');
    const seriesName = $('#js-vis-series-name');
    const seriesMap = {};

    const frmName = $('#_vis_frm_ser_name')[0];
    const frmChartType = $('#_vis_frm_ser_chart_type')[0];
    const frmQuery = $('#_vis_frm_ser_query')[0];
    const frmFields = $('#_vis_frm_ser_fields')[0];
    const frmStack = $('#_vis_frm_ser_stack')[0];
    const frmColour = $('#_vis_frm_ser_colour')[0];

    const xAxisType = $('#_vis_axis_x_type')[0];
    const xAxisField = $('#_vis_axis_x_field')[0];
    // const xAxisValues = $('#_vis_axis_x_values')[0];
    //
    const yAxisType = $('#_vis_axis_y_type')[0];
    const yAxisField = $('#_vis_axis_y_field')[0];
    // const yAxisValues = $('#_vis_axis_y_values')[0];

    let msgBus;
    let activeSeries;

    const chartTypeMap = {
        'Line': 'line',
        'Area': 'area',
        'Bar': 'bar'
    };

    const queryDiv = $('#vis-query')[0];

    const queryRequest = {
        q: null,
        callback: null
    };
    const edit = qdb.createEditor(queryDiv);
    let chart;
    let formsHeight = 300;
    let xSelect;
    let ySelect;
    let visible = true;

    function updateSeriesName(id, name) {
        seriesName.text(name);
        $('#' + id).text(name);
    }

    function clearSeriesName() {
        seriesName.text('');
    }

    function storeActiveSeries(flag) {
        if (activeSeries) {
            const series = seriesMap[activeSeries];
            series.name = frmName.value;
            series.chartType = frmChartType.value;
            series.query = frmQuery.value;
            series.fields = frmFields.value;
            series.stack = frmFields.value;
            series.colour = frmColour.value;
            // flag would be undefined when called back from list items
            if (!flag) {
                updateSeriesName(activeSeries, series.name);
            }
        }
    }

    function switchToSeries(evt) {
        storeActiveSeries(true);
        activeSeries = evt.target.getAttribute('id');
        const series = seriesMap[activeSeries];
        if (series) {
            updateSeriesName(activeSeries, series.name);
            frmName.value = series.name;
            frmChartType.value = series.chartType;
            frmQuery.value = series.query;
            frmFields.value = series.fields;
            frmStack.value = series.stack;
            frmColour.value = series.colour;
            seriesName.html = series.name;
        } else {
            clearSeriesName();
        }
    }

    function addSeries() {
        const next = ulSeriesList.find('li').length + 1;
        const id = '_li_ser_' + next;
        const name = 'Series ' + next;
        const html = $(`<li id="${id}">${name}</li>`);

        seriesMap[id] = {
            name: name,
            chartType: 'Line',
            query: '',
            fields: '',
            stack: '',
            colour: ''
        };

        html.click(switchToSeries);
        html.appendTo(ulSeriesList);
        html.click();
    }

    function setupAutoSave() {
        frmName.onfocusout = storeActiveSeries;
        frmChartType.onfocusout = storeActiveSeries;
        frmQuery.onfocusout = storeActiveSeries;
        frmFields.onfocusout = storeActiveSeries;
        frmFields.onfocusout = storeActiveSeries;
        frmColour.onfocusout = storeActiveSeries;
    }

    function toggleVisibility(x, name) {
        if (name === 'visualisation') {
            visible = true;
            panels.show();
            resize();
        } else {
            visible = false;
            panels.hide();
        }
    }

    function extractData(response, names) {
        const indices = [];
        // find column indices
        if (names && names.length > 0) {
            const columns = response.columns;
            for (let i = 0; i < columns.length; i++) {
                for (let j = 0; j < names.length; j++) {
                    if (columns[i].name === names[j]) {
                        indices.push(i);
                    }
                }
            }
        }

        const d = response.dataset;
        const data = new Array(d.length);
        const columnCount = indices.length;

        switch (columnCount) {
            case 0:
                break;
            case 1:
                const index = indices[0];
                for (let i = 0; i < d.length; i++) {
                    data[i] = d[i][index];
                }
                break;
            default:
                for (let i = 0; i < d.length; i++) {
                    const row = [];
                    for (let j = 0; j < indices.length; j++) {
                        row.push(d[i][indices[j]]);
                    }
                    data[i] = row;
                }
                break;
        }
        return data;
    }

    function generateChart() {
        let ready = true;
        // temporarily take first response as input for axis
        let firstResponse;

        for (const name in seriesMap) {
            const series = seriesMap[name];
            if (!series.queryResponse) {
                ready = false;
                break;
            }

            if (!firstResponse) {
                firstResponse = series.queryResponse;
            }
        }

        if (ready) {
            const options = {};
            const xAxis = {};
            const yAxis = {};

            options.xAxis = [xAxis];
            options.yAxis = [yAxis];

            switch (xAxisType.value) {
                case 'Category field name':
                    xAxis.data = extractData(firstResponse, xAxisField.value.split(','));
                    xAxis.type = 'category';
                    break;
                case 'Series value':
                    xAxis.type = 'value';
                    break;
                default:
                    break;
            }

            switch (yAxisType.value) {
                case 'Category field name':
                    yAxis.data = extractData(firstResponse, yAxisField.value.split(','));
                    yAxis.type = 'category';
                    break;
                case 'Series value':
                    yAxis.type = 'value';
                    break;
                default:
                    break;
            }

            options.series = [];
            for (const name in seriesMap) {
                const series = seriesMap[name];
                options.series.push({
                    type: chartTypeMap[series.chartType],
                    name: series.name,
                    data: series.data
                });
            }

            console.log(options);
            chart.setOption(options);
        }
    }

    function queryCallback(response, series) {
        series.data = extractData(response, series.fields.split(','));
        series.queryResponse = response;
        generateChart();
    }

    function fetchData() {
        console.log('fetching?');

        for (const name in seriesMap) {
            const series = seriesMap[name];
            const query = series.query;
            const request = {
                q: query,
                callback: queryCallback,
                callbackData: series
            };
            msgBus.trigger(qdb.MSG_QUERY_EXEC, request);
        }
    }

    function toColumnSupertype(type) {
        switch (type) {
            case 'STRING':
            case 'SYMBOL':
                return '<span class="label label-danger">L</span>';
            case 'DATE':
                return '<span class="label label-success">D</span>';
            default:
                return '<span class="label label-warning-light">N</span>';
        }
    }

    // function createColumnPicker(id) {
    //     return $(id).selectize({
    //         persist: false,
    //         maxItems: null,
    //         valueField: 'name',
    //         labelField: 'name',
    //         searchField: ['name'],
    //         options: []
    //     })[0].selectize;
    // }

    function refreshColumnPicker(select, columns) {
        select.clearOptions();
        select.addOption(columns);
        select.refreshOptions(false);
    }

    function processDataSet(dataSet) {
        const cols = dataSet.columns;
        let html = '';
        for (let i = 0, n = cols.length; i < n; i++) {
            const col = cols[i];
            html += `<li>${col.name}${toColumnSupertype(col.type)}</li>`;
        }
        columnContainer.html(html);
        refreshColumnPicker(xSelect, cols);
        refreshColumnPicker(ySelect, cols);
    }

    function resize() {
        if (visible) {
            const h = window.innerHeight;
            const menuHeight = menu[0].offsetHeight;
            qdb.setHeight(forms, formsHeight);
            qdb.setHeight(canvas, h - menuHeight - formsHeight - footer.offsetHeight);
            chart.resize();
        }
    }

    function setup(bus) {
        $(window).bind('resize', resize);
        msgBus = bus;
        bus.on(qdb.MSG_ACTIVE_PANEL, toggleVisibility);

        bus.on('splitter.vis.resize', function (x, delta) {
            formsHeight -= delta;
            $(window).trigger('resize');
        });

        chart = echarts.init(canvas[0], eChartsMacarons);

        // add series button
        $('#vis-btn-add-series').click(addSeries);
        setupAutoSave();

        $('#btnVisRefresh').click(function () {
            queryRequest.q = edit.getValue();
            queryRequest.callback = processDataSet;
            bus.trigger(qdb.MSG_QUERY_EXEC, queryRequest);
        });

        $('#vis-splitter').splitter(bus, 'vis', 300, 0);
        $('#btnVisFetch').click(fetchData);

        $('#btnVisBuild').click(function () {
            let options = {
                xAxis: [
                    {
                        type: 'category',
                        boundaryGap: false,
                        data: [1, 2, 3, 4, 5, 6, 7]
                    }
                ],
                yAxis: [
                    {
                        type: 'value'
                    }
                ],
                series: [
                    {
                        name: '最高气温',
                        type: 'line',
                        data: [11, 11, 15, 13, 12, 13, 10]
                    },
                    {
                        name: '最低气温',
                        type: 'line',
                        data: [1, -2, 2, 5, 3, 2, 0]
                    }
                ]
            };

            chart.setOption(options);
        });

        // xSelect = createColumnPicker('#vis-x-axis');
        // ySelect = createColumnPicker('#vis-y-axis');
    }

    $.extend(true, window, {
        qdb: {
            setupVisualisationController: setup
        }
    });
}(jQuery));
