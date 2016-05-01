/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * The MIT License (MIT)
 *
 * Copyright (C) 2016 Appsicle
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

/*globals $:false */
/*globals jQuery:false */
/*globals qdb:false */

(function ($) {
    'use strict';
    $.fn.grid = function () {
        var defaults = {
            minColumnWidth: 60,
            rowHeight: 28,
            divCacheSize: 128,
            viewportHeight: 400,
            yMaxThreshold: 10000000,
            maxRowsToAnalyze: 100,
            bottomMargin: 90
        };
        var $style;
        var div = $(this);
        var viewport;
        var canvas;
        var header;
        var colMax;
        var columns = [];
        var data = [];
        var totalWidth = -1;
        var stretched = 0;
        // number of divs in "rows" cache, has to be power of two
        var dc = defaults.divCacheSize;
        var dcn = dc - 1;
        var pageSize = qdb.queryBatchSize;
        var oneThirdPage = Math.floor(pageSize / 3);
        var twoThirdsPage = oneThirdPage * 2;
        var loPage;
        var hiPage;
        var query;
        var moreExists = true;
        var renderTimer;

        // viewport height
        var vp = defaults.viewportHeight;
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

        function addRows(n) {
            r += n;
            yMax = r * rh;
            if (yMax < defaults.yMaxThreshold) {
                h = yMax;
            } else {
                h = defaults.yMaxThreshold;
            }
            M = Math.ceil(yMax / h);
            canvas.css('height', h === 0 ? 1 : h);
        }

        function renderRow(row, n) {
            if (row.questIndex !== n) {
                var rowData = data[Math.floor(n / pageSize)];
                var offset = n % pageSize;
                var k;
                if (rowData) {
                    var d = rowData[offset];
                    for (k = 0; k < columns.length; k++) {
                        row.childNodes[k].innerHTML = d[k] !== null ? d[k].toString() : 'null';
                    }
                    row.questIndex = n;
                } else {
                    for (k = 0; k < columns.length; k++) {
                        row.childNodes[k].innerHTML = '';
                    }
                }
                row.style.top = (n * rh - o) + 'px';
            }
        }

        function renderViewportNoCompute() {
            // calculate the viewport + buffer
            var t = Math.max(0, Math.floor((y - vp) / rh));
            var b = Math.min(yMax / rh, Math.ceil((y + vp + vp) / rh));

            for (var i = t; i < b; i++) {
                renderRow(rows[i & dcn], i);
            }
        }

        function delayedRenderViewportNoCompute() {
            if (renderTimer) {
                clearTimeout(renderTimer);
            }
            renderTimer = setTimeout(renderViewportNoCompute, 50);
        }

        function purgeOutlierPages() {
            for (var i = 0; i < data.length; i++) {
                if ((i < loPage || i > hiPage) && data[i]) {
                    delete data[i];
                }
            }
        }

        function loadOnePage(pageToLoad) {
            purgeOutlierPages();
            var append = (pageToLoad + 1) * pageSize > r;
            var lo = pageToLoad * pageSize;
            var hi = lo + pageSize;
            $.get('/js', {query, limit: lo + ',' + hi, withCount: false})
                .done(function (response) {
                    data[pageToLoad] = response.result;
                    if (append && moreExists) {
                        addRows(response.result.length);
                        moreExists = response.moreExist;
                    }
                    delayedRenderViewportNoCompute();
                })
                .fail(function () {
                    console.error('fetch failed');
                });
        }

        function loadTwoPages(p1, p2) {
            purgeOutlierPages();
            var lo = p1 * pageSize;
            var hi = lo + pageSize * (p2 - p1 + 1);
            $.get('/js', {query, limit: lo + ',' + hi, withCount: false})
                .done(function (response) {
                    var l = response.result.length;
                    data[p1] = response.result.splice(0, pageSize);
                    data[p2] = response.result;
                    // work out if we need to extend grid's virtual space
                    if (r < lo + l) {
                        addRows(lo + l - r);
                    }
                    delayedRenderViewportNoCompute();
                })
                .fail(function () {
                    console.error('fetch failed');
                });
        }

        function computePages(direction, t, b) {

            if (t !== t || b !== b) {
                return;
            }

            var tp; // top page
            var tr; // top remaining
            var bp; // bottom page
            var br; // bottom remaining

            tp = Math.floor(t / pageSize);
            bp = Math.floor(b / pageSize);

            if (direction > 0) {
                br = b % pageSize;

                if (tp >= loPage && bp < hiPage) {
                    return;
                }

                if (bp === hiPage) {
                    if (br > twoThirdsPage) {
                        hiPage = bp + 1;
                        loPage = bp;
                        loadOnePage(bp + 1);
                    }
                    return;
                }

                if (tp < bp) {
                    loadTwoPages(tp, bp);
                    loPage = tp;
                    hiPage = bp;
                } else if (br > twoThirdsPage) {
                    loadTwoPages(bp, bp + 1);
                    loPage = bp;
                    hiPage = bp + 1;
                } else {
                    hiPage = tp;
                    loPage = tp;
                    loadOnePage(tp);
                }
            } else {
                tr = t % pageSize;

                if (tp > loPage && bp <= hiPage) {
                    return;
                }

                if (tp === loPage) {
                    if (tr < oneThirdPage && loPage > 0) {
                        loPage = Math.max(0, tp - 1);
                        hiPage = tp;
                        loadOnePage(tp - 1);
                    }
                    return;
                }

                if (tp < bp) {
                    loadTwoPages(tp, bp);
                    loPage = tp;
                    hiPage = bp;
                } else if (tr < oneThirdPage && tp > 0) {
                    loadTwoPages(tp - 1, tp);
                    loPage = Math.max(0, tp - 1);
                    hiPage = tp;
                } else {
                    loPage = tp;
                    hiPage = tp;
                    loadOnePage(tp);
                }
            }
        }

        function renderViewport(direction) {
            // calculate the viewport + buffer
            var t = Math.max(0, Math.floor((y - vp) / rh));
            var b = Math.min(yMax / rh, Math.ceil((y + vp + vp) / rh));

            if (direction !== 0) {
                computePages(direction, t, b);
            }

            for (var i = t; i < b; i++) {
                renderRow(rows[i & dcn], i);
            }
        }

        function getColumnAlignment(i) {
            switch (columns[i].type) {
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
            if (data.length > 0) {
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
            for (i = 0; i < columns.length; i++) {
                var c = columns[i];
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

            var max = data[0].length > defaults.maxRowsToAnalyze ? defaults.maxRowsToAnalyze : data[0].length;
            for (i = 0; i < max; i++) {
                var row = data[0][i];
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
            data = [];
            query = null;
            moreExists = true;
        }

        function viewportScroll(force) {
            header.scrollLeft(viewport.scrollLeft);

            var scrollTop = viewport.scrollTop;
            if (scrollTop !== top || force) {
                var oldY = y;
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
                renderViewport(y - oldY);
            }
        }

        function resize() {
            vp = Math.round((window.innerHeight - viewport.getBoundingClientRect().top)) - defaults.bottomMargin;
            viewport.style.height = vp + 'px';
            div.css('height', Math.round((window.innerHeight - div[0].getBoundingClientRect().top)) - defaults.bottomMargin);
            createCss();
            viewportScroll(true);
        }

        function addColumns() {
            for (var i = 0; i < dc; i++) {
                var rowDiv = $('<div class="qg-r"/>');
                for (var k = 0; k < columns.length; k++) {
                    $('<div class="qg-c qg-w' + k + '"/>').appendTo(rowDiv);
                }
                rowDiv.css({top: -100, height: rh}).appendTo(canvas);
                rows.push(rowDiv[0]);
            }
        }

        //noinspection JSUnusedLocalSymbols
        function update(x, m) {
            clear();
            loPage = 0;
            hiPage = 0;
            query = m.r.query;
            data.push(m.r.result);
            columns = m.r.columns;
            moreExists = m.r.moreExist;
            addColumns();
            addRows(m.r.result.length);
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

