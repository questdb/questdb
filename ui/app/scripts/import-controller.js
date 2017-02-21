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

/*globals qdb:false */
/*globals jQuery:false */


(function ($) {
    'use strict';

    const divImportPanel = $('.js-import-panel');
    const importTopPanel = $('#import-top');
    const canvasPanel = importTopPanel.find('.ud-canvas');
    const w = $(window);
    let visible = false;

    let upperHalfHeight = 450;

    function resize() {
        if (visible) {
            let r1 = importTopPanel[0].getBoundingClientRect();
            let r2 = canvasPanel[0].getBoundingClientRect();
            qdb.setHeight(importTopPanel, upperHalfHeight);
            qdb.setHeight(canvasPanel, upperHalfHeight - (r2.top - r1.top) - 10);
        }
    }

    function toggleVisibility(x, name) {
        if (name === 'import') {
            visible = true;
            divImportPanel.show();
            w.trigger('resize');
        } else {
            visible = false;
            divImportPanel.hide();
        }
    }

    function splitterResize(x, p) {
        upperHalfHeight += p;
        w.trigger('resize');
    }

    function setup(bus) {
        w.bind('resize', resize);

        $('#dragTarget').dropbox(bus);
        $('#import-file-list').importManager(bus);
        $('#import-detail').importEditor(bus);
        $('#sp2').splitter(bus, 'import', 470, 300);

        bus.on('splitter.import.resize', splitterResize);
        bus.on(qdb.MSG_ACTIVE_PANEL, toggleVisibility);
    }

    $.extend(true, window, {
        qdb: {
            setupImportController: setup
        }
    });
}(jQuery));
