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

(function ($) {
    'use strict';
    $.fn.grid = function (msgBus) {
        var defaults = {
            minColumnWidth: 60,
            rowHeight: 28,
            divCacheSize: 128,
            viewportHeight: 400,
            yMaxThreshold: 10000000,
            maxRowsToAnalyze: 100,
            bottomMargin: 75,
            minVpHeight: 120,
            minDivHeight: 160
        };
        var bus = msgBus;
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
        var queryTimer;
        var dbg;
        var downKey = [];

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
        // active (highlighted) row
        var activeRow = -1;
        // div that is highlighted
        var activeRowContainer;
        // active cell
        var activeCell = -1;
        var activeCellContainer;
        // rows in current view
        var rowsInView;

        function addRows(n) {
            r += n;
            yMax = r * rh;
            if (yMax < defaults.yMaxThreshold) {
                h = yMax;
            } else {
                h = defaults.yMaxThreshold;
            }
            M = yMax / h;
            canvas.css('height', h === 0 ? 1 : h);
        }

        function renderRow(rowContainer, n) {
            if (rowContainer.questIndex !== n) {
                var rowData = data[Math.floor(n / pageSize)];
                var offset = n % pageSize;
                var k;
                if (rowData) {
                    var d = rowData[offset];
                    if (d) {
                        for (k = 0; k < columns.length; k++) {
                            rowContainer.childNodes[k].innerHTML = d[k] !== null ? d[k].toString() : 'null';
                        }
                    }
                    rowContainer.questIndex = n;
                } else {
                    for (k = 0; k < columns.length; k++) {
                        rowContainer.childNodes[k].innerHTML = '';
                    }
                    rowContainer.questIndex = -1;
                }
                rowContainer.style.top = (n * rh - o) + 'px';
                if (rowContainer === activeRowContainer) {
                    if (n === activeRow) {
                        rowContainer.className = 'qg-r qg-r-active';
                        rowContainer.childNodes[activeCell].className += ' qg-c-active';
                    } else {
                        rowContainer.className = 'qg-r';
                        rowContainer.childNodes[activeCell].className = 'qg-c qg-w' + activeCell;
                    }
                }
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

        function purgeOutlierPages() {
            for (var i = 0; i < data.length; i++) {
                if ((i < loPage || i > hiPage) && data[i]) {
                    delete data[i];
                }
            }
        }

        function empty(x) {
            return data[x] === null || data[x] === undefined || data[x].length === 0;
        }

        function loadPages(p1, p2) {
            purgeOutlierPages();

            var lo;
            var hi;
            var f;

            if (p1 !== p2 && empty(p1) && empty(p2)) {
                lo = p1 * pageSize;
                hi = lo + pageSize * (p2 - p1 + 1);
                f = function (response) {
                    data[p1] = response.dataset.splice(0, pageSize);
                    data[p2] = response.dataset;
                    renderViewportNoCompute();
                };
            } else if (empty(p1) && (!empty(p2) || p1 === p2)) {
                lo = p1 * pageSize;
                hi = lo + pageSize;
                f = function (response) {
                    data[p1] = response.dataset;
                    renderViewportNoCompute();
                };
            } else if ((!empty(p1) || p1 === p2) && empty(p2)) {
                lo = p2 * pageSize;
                hi = lo + pageSize;
                f = function (response) {
                    data[p2] = response.dataset;
                    renderViewportNoCompute();
                };
            } else {
                renderViewportNoCompute();
                return;
            }
            $.get('/exec', {query, limit: lo + ',' + hi, nm: true}).done(f);
        }

        function loadPagesDelayed(p1, p2) {
            if (queryTimer) {
                clearTimeout(queryTimer);
            }
            queryTimer = setTimeout(function () {
                loadPages(p1, p2);
            }, 75);
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
                        loadPagesDelayed(bp, bp + 1);
                    }
                    return;
                }

                if (tp < bp) {
                    loadPagesDelayed(tp, bp);
                    loPage = tp;
                    hiPage = bp;
                } else if (br > twoThirdsPage) {
                    loadPagesDelayed(bp, bp + 1);
                    loPage = bp;
                    hiPage = bp + 1;
                } else {
                    hiPage = tp;
                    loPage = tp;
                    loadPagesDelayed(tp, tp);
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
                        loadPagesDelayed(tp - 1, tp);
                    }
                    return;
                }

                if (tp < bp) {
                    loadPagesDelayed(tp, bp);
                    loPage = tp;
                    hiPage = bp;
                } else if (tr < oneThirdPage && tp > 0) {
                    loadPagesDelayed(tp - 1, tp);
                    loPage = Math.max(0, tp - 1);
                    hiPage = tp;
                } else {
                    loPage = tp;
                    hiPage = tp;
                    loadPagesDelayed(tp, tp);
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

            if (t === 0) {
                b = dc;
            } else if (b > r - 2) {
                t = Math.max(0, b - dc);
            }

            for (var i = t; i < b; i++) {
                var row = rows[i & dcn];
                if (row) {
                    renderRow(row, i);
                }
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
            loPage = 0;
            hiPage = 0;
            downKey = [];
            activeRowContainer = null;
            activeCellContainer = null;
            activeRow = 0;
            activeCell = 0;
        }

        function logDebug() {
            if (dbg) {
                dbg.empty();
                dbg.append('time = ' + new Date() + '<br>');
                dbg.append('y = ' + y + '<br>');
                dbg.append('M = ' + M + '<br>');
                dbg.append('o = ' + o + '<br>');
                dbg.append('h = ' + h + '<br>');
                dbg.append('vp = ' + vp + '<br>');
                dbg.append('yMax = ' + yMax + '<br>');
                dbg.append('top = ' + top + '<br>');
                dbg.append('activeRow = ' + activeRow + '<br>');
            }
        }

        function activeCellOff() {
            activeCellContainer.className = 'qg-c qg-w' + activeCell;
        }

        function activeCellOn(focus) {
            activeCellContainer = activeRowContainer.childNodes[activeCell];
            activeCellContainer.className += ' qg-c-active';

            if (focus) {
                var w;
                w = Math.max(0, activeCellContainer.offsetLeft - 5);
                if (w < viewport.scrollLeft) {
                    viewport.scrollLeft = w;
                } else {
                    w = activeCellContainer.offsetLeft + activeCellContainer.clientWidth + 5;
                    if (w > viewport.scrollLeft + viewport.clientWidth) {
                        viewport.scrollLeft = w - viewport.clientWidth;
                    }
                }

            }
        }

        function activeRowUp(n) {
            if (activeRow > 0) {
                activeRow = Math.max(0, activeRow - n);
                activeRowContainer.className = 'qg-r';
                activeCellOff();
                activeRowContainer = rows[activeRow & dcn];
                activeRowContainer.className = 'qg-r qg-r-active';
                activeCellOn();
                var scrollTop = activeRow * rh - o;
                if (scrollTop < viewport.scrollTop) {
                    viewport.scrollTop = Math.max(0, scrollTop);
                }
            }
        }

        function activeRowDown(n) {
            if (activeRow > -1 && activeRow < r - 1) {
                activeRow = Math.min(r - 1, activeRow + n);
                activeRowContainer.className = 'qg-r';
                activeCellOff();
                activeRowContainer = rows[activeRow & dcn];
                activeRowContainer.className = 'qg-r qg-r-active';
                activeCellOn();
                var scrollTop = activeRow * rh - vp + rh - o;
                if (scrollTop > viewport.scrollTop) {
                    viewport.scrollTop = scrollTop;
                }
            }
        }

        function viewportScroll(force) {
            header.scrollLeft(viewport.scrollLeft);

            var scrollTop = viewport.scrollTop;
            if (scrollTop !== top || force) {
                var oldY = y;
                if (Math.abs(scrollTop - top) > 4 * vp) {
                    y = scrollTop === 0 ? 0 : Math.min(Math.ceil((scrollTop + vp) * M - vp), yMax - vp);
                    top = scrollTop;
                    o = y - top;
                } else if (h - vp > 0) {
                    // if grid content fits in viewport we don't need to adjust activeRow
                    if (scrollTop >= h - vp) {
                        // final leap to bottom of grid
                        // this happens when container div runs out of vertical height
                        // and we artificially force leap to bottom
                        y = Math.max(0, yMax - vp);
                        top = scrollTop;
                        o = y - top;
                        activeRowDown(r - activeRow);
                    } else {
                        if (scrollTop === 0 && top > 0) {
                            // this happens when grid is coming slowly back up after being scrolled down harshly
                            // because 'y' is much greater than top, we have to jump to top artificially.
                            y = 0;
                            o = 0;
                            activeRowUp(activeRow);
                        } else {
                            y += scrollTop - top;
                        }
                        top = scrollTop;
                    }
                }
                renderViewport(y - oldY);
            }
            logDebug();
        }

        function resize() {
            var wh = window.innerHeight - $(window).scrollTop();
            vp = Math.round((wh - viewport.getBoundingClientRect().top)) - defaults.bottomMargin;
            vp = Math.max(vp, defaults.minVpHeight);
            rowsInView = Math.floor(vp / rh);
            viewport.style.height = vp + 'px';
            div.css('height', Math.max(Math.round((wh - div[0].getBoundingClientRect().top)) - defaults.bottomMargin, defaults.minDivHeight) + 'px');
            createCss();
            viewportScroll(true);
        }

        function rowClick() {
            if (activeRowContainer) {
                activeRowContainer.className = 'qg-r';
            }
            this.focus();
            activeRowContainer = this.parentElement;
            activeRowContainer.className += ' qg-r-active';
            activeRow = activeRowContainer.questIndex;

            if (activeCellContainer) {
                activeCellContainer.className = 'qg-c qg-w' + activeCell;
            }
            activeCellContainer = this;
            activeCell = this.cellIndex;
            activeCellContainer.className += ' qg-c-active';
        }


        function activeCellRight() {
            if (activeCell > -1 && activeCell < columns.length - 1) {
                activeCellOff();
                activeCell++;
                activeCellOn(true);
            }
        }

        function activeCellLeft() {
            if (activeCell > 0) {
                activeCellOff();
                activeCell--;
                activeCellOn(true);
            }
        }

        function activeCellHome() {
            if (activeCell > 0) {
                activeCellOff();
                activeCell = 0;
                activeCellOn(true);
            }
        }

        function activeCellEnd() {
            if (activeCell > -1 && activeCell !== columns.length - 1) {
                activeCellOff();
                activeCell = columns.length - 1;
                activeCellOn(true);
            }
        }


        function onKeyUp(e) {
            delete downKey[(('which' in e) ? e.which : e.keyCode)];
        }

        function onKeyDown(e) {
            var keyCode = ('which' in e) ? e.which : e.keyCode;
            var preventDefault = true;
            switch (keyCode) {
                case 33: // page up
                    activeRowUp(rowsInView);
                    break;
                case 38: // arrow up
                    if (downKey[91]) {
                        activeRowUp(activeRow);
                    } else {
                        activeRowUp(1);
                    }
                    break;
                case 40: // arrow down
                    if (downKey[91]) {
                        activeRowDown(r - activeRow);
                    } else {
                        activeRowDown(1);
                    }
                    break;
                case 34: // arrow down
                    activeRowDown(rowsInView);
                    break;
                case 39: // arrow right
                    activeCellRight();
                    break;
                case 37: // arrow left
                    activeCellLeft();
                    break;
                case 35: // end? Fn+arrow right on mac
                    if (downKey[17]) {
                        activeRowDown(r - activeRow);
                    } else {
                        activeCellEnd();
                    }
                    break;
                case 36: // home ? Fn + arrow left on mac
                    if (downKey[17]) {
                        activeRowUp(activeRow);
                    } else {
                        activeCellHome();
                    }
                    break;
                case 113:
                    unfocusCell();
                    bus.trigger('editor.focus');
                    break;
                default:
                    downKey[keyCode] = true;
                    preventDefault = false;
                    break;
            }

            if (preventDefault) {
                e.preventDefault();
            }
        }

        function addColumns() {
            for (var i = 0; i < dc; i++) {
                var rowDiv = $('<div class="qg-r" tabindex="' + i + '"/>');
                if (i === 0) {
                    activeRowContainer = rowDiv;
                }
                for (var k = 0; k < columns.length; k++) {
                    var cell = $('<div class="qg-c qg-w' + k + '"/>').click(rowClick).appendTo(rowDiv)[0];
                    if (i === 0 && k === 0) {
                        activeCellContainer = cell;
                    }
                    cell.cellIndex = k;
                }
                rowDiv.css({top: -100, height: rh}).appendTo(canvas);
                rows.push(rowDiv[0]);
            }
        }

        function focusCell() {
            if (activeCellContainer && activeRowContainer) {
                activeCellContainer.click();
                activeRowContainer.focus();
            }
        }

        function unfocusCell() {
            if (activeCellContainer) {
                activeCellOff();
            }
        }

        //noinspection JSUnusedLocalSymbols
        function update(x, m) {
            clear();
            query = m.query;
            data.push(m.dataset);
            columns = m.columns;
            addColumns();
            addRows(m.count);
            computeColumnWidths();
            viewport.scrollTop = 0;
            resize();
            focusCell();
        }

        function publishQuery() {
            if (query) {
                bus.trigger('grid.query', encodeURIComponent(query));
            }
        }

        function refreshQuery() {
            if (query) {
                bus.trigger(qdb.MSG_QUERY_EXEC, {q: query});
            }
        }

        function bind() {
            dbg = $('#debug');
            header = div.find('.qg-header-row');
            viewport = div.find('.qg-viewport')[0];
            viewport.onscroll = viewportScroll;
            canvas = div.find('.qg-canvas');
            canvas.bind('keydown', onKeyDown);
            canvas.bind('keyup', onKeyUp);
            $(window).resize(resize);
            bus.on(qdb.MSG_QUERY_DATASET, update);
            bus.on('grid.focus', focusCell);
            bus.on('grid.refresh', refreshQuery);
            bus.on('grid.publish.query', publishQuery);
        }

        bind();
        resize();
    };
}(jQuery));
