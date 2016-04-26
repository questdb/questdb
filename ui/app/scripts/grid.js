/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2016 Appsicle
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

/*globals $:false */
/*globals jQuery:false */

(function ($) {
    'use strict';
    $.fn.grid = function () {
        var defaults = {
            minColumnWidth: 60,
            rowHeight: 28
        };
        var $style;
        var div = $(this);
        var viewport;
        var canvas;
        var header;
        var colMax;
        var data;
        var totalWidth = -1;
        var stretched = 0;
        // number of divs in "rows" cache, has to be power of two
        var dc = 128;
        var dcn = dc - 1;

        // viewport height
        var vp = 400;
        // row height in px
        var rh = defaults.rowHeight;
        // virtual row count in grid
        var r;
        // max virtual y (height) of grid canvas
        var yMax;
        // current virtual y of grid canvas
        var y;
        // actual height of grid canvas
        var h;
        // last scroll top
        var top;
        // yMax / h - ratio between virtual and actual height
        var M;
        // offset to bring virtual y inline with actual y
        var o;
        // row div cache
        var rows = [];

        function addRows() {
            r += data.result.length;
            yMax = r * rh;
            if (yMax < 10000000) {
                h = yMax;
            } else {
                h = 10000000;
            }
            M = Math.ceil(yMax / h);
            canvas.css('height', h === 0 ? 1 : h);
        }

        function renderRow(row, n) {
            if (row.questIndex !== n) {
                var d = data.result[n];
                var l = d.length;
                for (var k = 0; k < l; k++) {
                    row.childNodes[k].innerHTML = d[k] !== null ? d[k].toString() : 'null';
                }
                row.style.top = (n * rh - o) + 'px';
                row.questIndex = n;
            }
        }

        function renderViewport() {
            // calculate the viewport + buffer
            var t = Math.max(0, Math.floor((y - vp) / rh));
            var b = Math.min(yMax / rh, Math.ceil((y + vp + vp) / rh));

            for (var i = t; i < b; i++) {
                renderRow(rows[i & dcn], i);
            }
        }

        function getColumnAlignment(i) {
            switch (data.columns[i].type) {
                case 'STRING':
                case 'SYMBOL':
                    return 'text-align: left;';
                default:
                    return '';
            }
        }

        function generatePxWidth(rules) {
            for (var i = 0; i < colMax.length; i++) {
                rules.push('.qg-w' + i + '{width:' + colMax[i] + 'px;' + getColumnAlignment(i) + '}');
            }
            rules.push('.qg-r{width:' + totalWidth + 'px;}');
            rules.push('.qg-canvas{width:' + totalWidth + 'px;}');
            stretched = 2;
        }

        function generatePctWidth(rules) {
            for (var i = 0; i < colMax.length; i++) {
                rules.push('.qg-w' + i + '{width:' + colMax[i] * 100 / totalWidth + '%;' + getColumnAlignment(i) + '}');
            }
            rules.push('.qg-r{width:100%;}');
            rules.push('.qg-canvas{width:100%;}');
            stretched = 1;
        }

        function createCss() {
            if (data) {
                var viewportWidth = viewport.offsetWidth;
                var f = null;
                if (totalWidth < viewportWidth && stretched !== 1) {
                    f = generatePctWidth;
                } else if (totalWidth > viewportWidth && stretched !== 2) {
                    f = generatePxWidth;
                }

                if (f) {
                    if ($style) {
                        $style.remove();
                    }
                    $style = $('<style type="text/css" rel="stylesheet"/>').appendTo($('head'));
                    var rules = [];

                    f(rules);

                    rules.push('.qg-c{height:' + rh + 'px;}');
                    if ($style[0].styleSheet) { // IE
                        $style[0].styleSheet.cssText = rules.join(' ');
                    } else {
                        $style[0].appendChild(document.createTextNode(rules.join(' ')));
                    }
                }
            }
        }

        function computeColumnWidths() {
            colMax = [];
            var i, k, w;
            totalWidth = 0;
            for (i = 0; i < data.columns.length; i++) {
                var c = data.columns[i];
                var col = $('<div class="qg-header qg-w' + i + '">' + c.name + '</div>').appendTo(header);
                switch (c.type) {
                    case 'STRING':
                    case 'SYMBOL':
                        col.addClass('qg-header-l');
                        break;
                }
                w = Math.max(defaults.minColumnWidth, Math.ceil(c.name.length * 8 * 1.2 + 8));
                colMax.push(w);
                totalWidth += w;
            }

            var max = data.result.length > 100 ? 100 : data.result.length;
            for (i = 0; i < max; i++) {
                var row = data.result[i];
                var sum = 0;
                for (k = 0; k < row.length; k++) {
                    var cell = row[k];
                    var str = cell !== null ? cell.toString() : 'null';
                    w = Math.max(defaults.minColumnWidth, str.length * 8 + 8);
                    colMax[k] = Math.max(w, colMax[k]);
                    sum += colMax[k];
                }
                totalWidth = Math.max(totalWidth, sum);
            }
        }

        function clear() {
            top = 0;
            y = 0;
            o = 0;
            r = 0;

            if ($style) {
                $style.remove();
            }
            header.empty();
            canvas.empty();
            rows = [];
            stretched = 0;
            data = null;
        }

        function viewportScroll(force) {
            header.scrollLeft(viewport.scrollLeft);

            var scrollTop = viewport.scrollTop;
            if (scrollTop !== top || force) {
                if (Math.abs(scrollTop - top) > vp) {
                    // near scroll
                    y = scrollTop === 0 ? 0 : Math.ceil((scrollTop + vp) * M - vp);
                    top = scrollTop;
                    o = y - top;
                } else {
                    // jump
                    y += scrollTop - top;
                    top = scrollTop;
                }
                renderViewport();
            }
        }

        function resize() {
            vp = Math.round((window.innerHeight - viewport.getBoundingClientRect().top)) - 90;
            viewport.style.height = vp + 'px';
            div.css('height', Math.round((window.innerHeight - div[0].getBoundingClientRect().top)) - 90);
            createCss();
            viewportScroll(true);
        }

        function addColumns() {
            for (var i = 0; i < dc; i++) {
                var rowDiv = $('<div class="qg-r"/>');
                for (var k = 0; k < data.columns.length; k++) {
                    $('<div class="qg-c qg-w' + k + '"/>').appendTo(rowDiv);
                }
                rowDiv.css({top: -100, height: rh}).appendTo(canvas);
                rows.push(rowDiv[0]);
            }
        }

        //noinspection JSUnusedLocalSymbols
        function update(x, m) {
            clear();
            data = m.r;
            addColumns();
            addRows();
            computeColumnWidths();
            viewport.scrollTop = 0;
            resize();
        }

        function bind() {
            header = div.find('.qg-header-row');
            viewport = div.find('.qg-viewport')[0];
            viewport.onscroll = viewportScroll;
            canvas = div.find('.qg-canvas');
            $(document).on('query.ok', update);
            $(window).resize(resize);
        }

        bind();
        resize();
    };
}(jQuery));

$(document).ready(function () {
    'use strict';
    $('#grid').grid();
});

