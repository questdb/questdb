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


(function ($) {
    'use strict';
    const panels = $('.js-vis-panel');
    const w = $(window);
    const container = $('#visualisation-top');
    const footerHeight = $('.footer')[0].offsetHeight;
    const columnContainer = panels.find('.vis-columns');
    const canvas = panels.find('#vis-canvas')[0];

    let visible = true;

    function toggleVisibility(x, name) {
        if (name === 'visualisation') {
            panels.show();
            visible = true;
            w.trigger('resize');
        } else {
            visible = false;
            panels.hide();
        }
    }

    function processDataSet(x, dataSet) {
        console.log(dataSet);
        const cols = dataSet.columns;
        let html = '';
        for (let i = 0, n = cols.length; i < n; i++) {
            html += '<li>' + cols[i].name + '</li>';
        }
        columnContainer.html(html);
    }

    function resize() {
        if (visible) {
            qdb.setHeight(container, window.innerHeight - footerHeight - 80);
        }
    }

    function setup(bus) {
        $(window).bind('resize', resize);
        bus.on(qdb.MSG_ACTIVE_PANEL, toggleVisibility);
        bus.on(qdb.MSG_QUERY_DATASET, processDataSet);

        echarts.init(canvas);
    }

    $.extend(true, window, {
        qdb: {
            setupVisualisationController: setup
        }
    });
}(jQuery));
