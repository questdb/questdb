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
    $.fn.splitter = function (msgBus, pMinTop, pMinBottom) {
        var bus = $(msgBus);
        var div = $(this);
        var ghost;
        var start;
        var end;
        var styleMain;
        var minTop = pMinTop;
        var minBottom = pMinBottom;

        function drag(e) {
            e.preventDefault();
            if (e.pageY > minTop && e.pageY < ((window.innerHeight + $(window).scrollTop()) - minBottom)) {
                end = e.pageY;
                ghost[0].style = styleMain + 'top: ' + e.pageY + 'px;';
            }
        }

        function endDrag() {
            $(document).off('mousemove', drag);
            $(document).off('mouseup', endDrag);
            ghost[0].style = 'display: none';
            div.removeClass('qs-dragging');
            bus.trigger('splitter.resize', (end - start));
        }

        function beginDrag() {
            var rect = div[0].getBoundingClientRect();
            start = rect.top + $(window).scrollTop();
            styleMain = 'position: absolute; left: ' + rect.left + 'px; width: ' + rect.width + 'px; height: ' + rect.height + 'px;';
            if (!ghost) {
                ghost = $('<div class="qs-ghost"></div>');
                ghost.appendTo('body');
            }
            ghost[0].style = styleMain + 'top: ' + start + 'px;';
            div.addClass('qs-dragging');
            $(document).mousemove(drag);
            $(document).mouseup(endDrag);
        }

        $(this).mousedown(beginDrag);
    };
}(jQuery));
