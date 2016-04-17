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

(function ($) {
    'use strict';
    $.fn.grid = function () {
        var defaults = {
            minColumnWidth: 60
        };
        var $style;
        var div = $(this);
        var canvas;
        var header;
        var colMax;

        function renderPermMarkup() {
            header = $('<div class="qg-header-row"></div>');
            header.appendTo(div);
            canvas = $('<div class="qg-canvas"></div>');
            canvas.appendTo(div);

            canvas.bind('scroll', function () {
                header.scrollLeft(canvas.scrollLeft());
            });
        }

        function createCss() {
            $style = $('<style type="text/css" rel="stylesheet"/>').appendTo($('head'));

            var rules = [];
            var sum = 0;
            for (var i = 0; i < colMax.length; i++) {
                var w = Math.max(defaults.minColumnWidth, colMax[i] * 8 + 8);
                rules.push('.qg-w' + i + '{width:' + w + 'px;}');
                sum += w;
            }
            rules.push('.qg-r{width:' + sum + 'px;}');

            if ($style[0].styleSheet) { // IE
                $style[0].styleSheet.cssText = rules.join(' ');
            } else {
                $style[0].appendChild(document.createTextNode(rules.join(' ')));
            }
        }

        function render(r) {
            colMax = [];
            var i, k;
            for (i = 0; i < r.columns.length; i++) {
                var c = r.columns[i];
                $('<div class="qg-header qg-w' + i + '">' + c.name + '</div>').appendTo(header);
                colMax.push(0);
            }

            for (i = 0; i < r.result.length; i++) {
                var row = r.result[i];
                var rowDiv = $('<div class="qg-r"></div>');
                for (k = 0; k < row.length; k++) {
                    var cell = row[k];
                    var str = cell !== null ? cell.toString() : 'null';
                    colMax[k] = Math.max(str.length, colMax[k]);
                    $('<div class="qg-c qg-w' + k + '">' + str + '</div>').appendTo(rowDiv);
                }
                rowDiv.appendTo(canvas);
            }
        }

        function clear() {
            if ($style) {
                $style.remove();
            }
            header.empty();
            canvas.empty();
        }

        function resizeCanvas() {
            var top = canvas[0].getBoundingClientRect().top;
            var h = Math.round((window.innerHeight - top));
            h = h - 90;
            canvas[0].style.height = h + 'px';
        }

        function resizeDiv() {
            var top = div[0].getBoundingClientRect().top;
            var h = Math.round((window.innerHeight - top));
            h = h - 90;
            div[0].style.height = h + 'px';
        }

        function resize() {
            resizeCanvas();
            resizeDiv();
        }

        //noinspection JSUnusedLocalSymbols
        function update(x, m) {
            clear();
            render(m.r);
            createCss();
            resize();
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

