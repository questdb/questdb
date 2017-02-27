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
    let query;

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

    function showQueryText(e, qry) {
        query = qry.q;
        edit.setValue(query);
    }

    function setup(bus) {
        $(window).bind('resize', resize);
        bus.on(qdb.MSG_ACTIVE_PANEL, toggleVisibility);
        bus.on('query.text', showQueryText);

        bus.on('splitter.vis.resize', function (x, delta) {
            formsHeight -= delta;
            $(window).trigger('resize');
        });

        chart = echarts.init(canvas[0], eChartsMacarons);

        $('#btnVisRefresh').click(function () {
            queryRequest.q = edit.getValue();
            queryRequest.callback = processDataSet;
            bus.trigger(qdb.MSG_QUERY_EXEC, queryRequest);
        });

        $('#vis-splitter').splitter(bus, 'vis', 300, 0);

        $('#btnVisBuild').click(function () {
            let options = {
                xAxis: [
                    {
                        type: 'category value',
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
