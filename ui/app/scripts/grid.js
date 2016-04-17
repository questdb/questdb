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
        var $style;
        var div = $(this);
        var canvas;
        var header;

        function createCss(columns) {
            $style = $('<style type="text/css" rel="stylesheet"/>').appendTo($('head'));

            var rules = [];
            var cw = 8;
            var rowWidth = 0;

            for (var i = 0; i < columns.length; i++) {
                var c = columns[i];
                var headWidth = c.name.length * cw;
                var typeWidth = 0;
                switch (c.type) {
                    case 'BOOLEAN':
                    case 'BYTE':
                        typeWidth = 30;
                        break;
                    case 'DOUBLE':
                    case 'FLOAT':
                    case 'LONG':
                    case 'DATE':
                        typeWidth = 150;
                        break;
                    case 'INT':
                    case 'SHORT':
                        typeWidth = 100;
                        break;
                    case 'STRING':
                    case 'SYMBOL':
                        typeWidth = 0;
                        break;
                    default:
                        break;
                }
                var w = Math.max(headWidth, typeWidth);
                rules.push('.qg-w' + i + '{width:' + w + 'px;}');
                rowWidth += w;
            }
            rules.push('.qg-r{width:' + rowWidth + 'px;}');

            if ($style[0].styleSheet) { // IE
                $style[0].styleSheet.cssText = rules.join(' ');
            } else {
                $style[0].appendChild(document.createTextNode(rules.join(' ')));
            }
        }

        function renderPermMarkup() {
            header = $('<div class="qg-header-row"></div>');
            header.appendTo(div);
            canvas = $('<div class="qg-canvas"></div>');
            canvas.appendTo(div);

            canvas.bind('scroll', function () {
                header.scrollLeft(canvas.scrollLeft());
            });
        }

        function render(r) {

            var i, k;
            for (i = 0; i < r.columns.length; i++) {
                var c = r.columns[i];
                $('<div class="qg-header qg-w' + i + '">' + c.name + '</div>').appendTo(header);
            }

            for (i = 0; i < r.result.length; i++) {
                var row = r.result[i];
                var rowDiv = $('<div class="qg-r"></div>');
                for (k = 0; k < row.length; k++) {
                    $('<div class="qg-c qg-w' + k + '">' + row[k] + '</div>').appendTo(rowDiv);
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

        function update(x, m) {
            clear();
            createCss(m.r.columns);
            render(m.r);
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

