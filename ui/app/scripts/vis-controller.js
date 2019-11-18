/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
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

        function resize() {
            if (visible) {
                const h = window.innerHeight;
                const menuHeight = menu[0].offsetHeight;
                qdb.setHeight(forms, formsHeight);
                qdb.setHeight(canvas, h - menuHeight - formsHeight - footer.offsetHeight);
                chart.resize();
            }
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
                        chart.setOption(options, true);
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
                loadState(queryForm, axisForm, seriesForm);
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
