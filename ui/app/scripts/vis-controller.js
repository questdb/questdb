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
/*globals VisBuilder:false */

(function ($) {
        'use strict';
        const panels = $('.js-vis-panel');
        const canvas = panels.find('#vis-canvas');
        const menu = panels.find('#vis-menu');
        const forms = panels.find('#vis-forms');
        const footer = $('.footer')[0];

        let visBuilder;
        let chart;
        let visible = false;
        let formsHeight = 450;
        let lastSaveTimestamp = -1;

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

        function resize() {
            if (visible) {
                const h = window.innerHeight;
                const menuHeight = menu[0].offsetHeight;
                qdb.setHeight(forms, formsHeight);
                qdb.setHeight(canvas, h - menuHeight - formsHeight - footer.offsetHeight);
                chart.resize();
            }
        }

        function splitterResize(x, delta) {
            formsHeight -= delta;
            $(window).trigger('resize');
        }

        function saveState(item) {
            if (item === undefined || lastSaveTimestamp < item.timestamp) {
                const state = visBuilder.serializeState();
                if (item) {
                    lastSaveTimestamp = item.timestamp;
                }

                console.log('serializing');
                console.log(state);
                localStorage.setItem('vis', JSON.stringify(state));
            }
        }

        function buildChartWhenReady(status, options) {
            switch (status) {
                case 'done':
                    if (options) {
                        console.log('options arrived');
                        console.log(JSON.stringify(options));
                        chart.setOption(options);
                    }
                    break;
                default:
                    console.log(status);
                    break;
            }
        }

        function buildChart() {
            visBuilder.generateOptions(buildChartWhenReady);
        }

        function loadState(queryForm, axisForm, seriesForm) {
            if (typeof (Storage) !== 'undefined') {
                const vis = localStorage.getItem('vis');

                if (vis) {
                    const state = JSON.parse(vis);
                    if (state) {
                        console.log('loading');
                        console.log(state);
                        if (state.queries) {
                            queryForm.setMap(state.queries);
                        }

                        if (state.axis) {
                            axisForm.setMap(state.axis);
                        }

                        if (state.series) {
                            seriesForm.setMap(state.series);
                        }
                    }
                }

                queryForm.onUpdate(saveState);
                axisForm.onUpdate(saveState);
                seriesForm.onUpdate(saveState);
            }
        }

        function setup(bus) {
            $(window).bind('resize', resize);
            bus.on(qdb.MSG_ACTIVE_PANEL, toggleVisibility);
            bus.on('splitter.vis.resize', splitterResize);
            chart = echarts.init(canvas[0], eChartsMacarons);
            $('#vis-splitter').splitter(bus, 'vis', 300, formsHeight);

            const queryForm = $('#vis-tab-queries').queryForm();
            const seriesForm = $('#vis-tab-series').seriesForm();
            const axisForm = $('#vis-tab-axis').axisForm();

            visBuilder = new VisBuilder(queryForm.getMap(), seriesForm.getMap(), axisForm.getMap());
            loadState(queryForm, axisForm, seriesForm);

            $('#btnVisFetch').click(buildChart);
            $('#btnVisReload').click(function () {
                loadState(queryForm, seriesForm, axisForm);
            });
        }

        $.extend(true, window, {
                qdb: {
                    setupVisualisationController: setup
                }
            }
        );

    }(jQuery)
);
