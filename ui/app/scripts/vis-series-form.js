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

(function ($) {
        'use strict';
        $.fn.seriesForm = function () {
            const div = $(this);
            const fName = div.find('#_vis_frm_ser_name')[0];
            const fChartType = div.find('#_vis_frm_ser_chart_type')[0];
            const fColumns = div.find('#_vis_frm_ser_columns')[0];
            const fStack = div.find('#_vis_frm_ser_stack')[0];
            const fColor = div.find('#_vis_frm_ser_color')[0];
            const fAxis = div.find('#_vis_frm_ser_axis')[0];

            let last;

            function newQuery(index) {
                return {
                    id: '_li_series_' + index,
                    name: 'series' + index
                };
            }

            function copyToForm(series) {
                last = series;

                fName.value = series.name;
                fChartType.value = series.chartType ? series.chartType : 'Line';
                fColumns.value = series.columns ? series.columns : '';
                fStack.value = series.stack ? series.stack : '';
                fColor.value = series.color ? series.color : '';
                fAxis.value = series.axis ? series.axis : '';
            }

            function copyToMem(series) {
                let changed = false;
                if (series.name !== fName.value) {
                    series.name = fName.value;
                    changed = true;
                }

                if (series.chartType !== fChartType.value) {
                    series.chartType = fChartType.value;
                    changed = true;
                }

                if (series.columns !== fColumns.value) {
                    series.columns = fColumns.value;
                    changed = true;
                }

                if (series.stack !== fStack.value) {
                    series.stack = fStack.value;
                    changed = true;
                }

                if (series.color !== fColor.value) {
                    series.color = fColor.value;
                    changed = true;
                }

                if (series.axis !== fAxis.value) {
                    series.axis = fAxis.value;
                    changed = true;
                }

                if (changed) {
                    series.timestamp = new Date().getTime();
                }

                if (series.callback) {
                    series.callback();
                }

                return true;
            }

            function clear() {
                fName.value = '';
                fChartType.value = 'Line';
                fColumns.value = '';
                fStack.value = '';
                fColor.value = '';
                fAxis.value = '';
            }

            function copyToLast() {
                if (last) {
                    copyToMem(last);
                }
            }

            fName.onfocusout = copyToLast;
            fChartType.onfocusout = copyToLast;
            fColumns.onfocusout = copyToLast;
            fStack.onfocusout = copyToLast;
            fColor.onfocusout = copyToLast;
            fAxis.onfocusout = copyToLast;

            return div.listManager(newQuery, copyToForm, copyToMem, clear);
        };
    }(jQuery)
);
