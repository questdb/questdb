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
        var rows = {};

        function addRows(n) {
            r += n;
            yMax = r * rh;
            if (yMax < 10000000) {
                h = yMax;
            } else {
                h = 10000000;
            }
            M = Math.ceil(yMax / h);
            canvas.css('height', h === 0 ? 1 : h);
        }

        function renderRow(row) {
            var d = data.result[row];
            var rowDiv = $('<div class="qg-r"/>');
            for (var k = 0; k < d.length; k++) {
                var str = d[k] !== null ? d[k].toString() : 'null';
                $('<div class="qg-c qg-w' + k + '">' + str + '</div>').appendTo(rowDiv);
            }
            return rowDiv.css({
                    top: row * rh - o,
                    height: rh
                })
                .appendTo(canvas);
        }

        function renderViewport() {
            // calculate the viewport + buffer
            var t = Math.max(0, Math.floor((y - vp) / rh));
            var b = Math.min(yMax / rh - 1, Math.ceil((y + vp + vp) / rh));

            // remove rows no longer in the viewport
            for (var i in rows) {
                if (i < t || i > b) {
                    rows[i].remove();
                    delete rows[i];
                }
            }

            // add new rows
            for (i = t; i <= b; i++) {
                if (!rows[i]) {
                    rows[i] = renderRow(i);
                }
            }
        }

        function viewportScroll(force) {
            header.scrollLeft(viewport.scrollLeft());

            var scrollTop = viewport.scrollTop();
            if (scrollTop !== top || force) {
                if (Math.abs(scrollTop - top) > vp) {
                    onJump(scrollTop);
                } else {
                    onNearScroll(scrollTop);
                }
                renderViewport();
            }
        }

        function renderPermMarkup() {
            header = div.find('.qg-header-row');
            viewport = div.find('.qg-viewport');
            viewport.scroll(viewportScroll);
            canvas = div.find('.qg-canvas');
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
                var viewportWidth = viewport.width();
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

            console.log('computed: ' + totalWidth);
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
            rows = {};
            stretched = 0;
            data = null;
        }

        function resizeViewport() {
            var t = viewport[0].getBoundingClientRect().top;
            vp = Math.round((window.innerHeight - t)) - 90;
            viewport.css('height', vp);
            createCss();
            viewportScroll(true);
        }

        function resizeDiv() {
            var t = div[0].getBoundingClientRect().top;
            div.css('height', Math.round((window.innerHeight - t)) - 90);
        }

        function resize() {
            resizeViewport();
            resizeDiv();
        }

        function onNearScroll(scrollTop) {
            y += scrollTop - top;
            top = scrollTop;
        }

        function removeAllRows() {
            for (var i in rows) {
                rows[i].remove();
                delete rows[i];
            }
        }

        function onJump(scrollTop) {
            y = scrollTop === 0 ? 0 : Math.ceil((scrollTop + vp) * M - vp);
            top = scrollTop;
            o = y - top;
            removeAllRows();
        }

        //noinspection JSUnusedLocalSymbols
        function update(x, m) {
            clear();
            data = m.r;
            addRows(data.result.length);
            computeColumnWidths();
            resize();
            viewport[0].scrollTop = 0;
            viewportScroll(true);
        }

        function bind() {
            $(document).on('query.ok', update);
            $(window).resize(resize);
        }

        bind();
        renderPermMarkup();
        resize();
    };
}(jQuery));

$(document).ready(function () {
    'use strict';
    $('#grid').grid();
});

