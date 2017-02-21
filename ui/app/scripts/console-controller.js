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
/*globals Clipboard:false */

(function ($) {
    'use strict';

    const divSqlPanel = $('.js-sql-panel');
    const divExportUrl = $('.js-export-url');
    const editor = $('#editor');
    const sqlEditor = $('#sqlEditor');
    const consoleTop = $('#console-top');
    const wrapper = $('#page-wrapper');
    const msgPanel = editor.find('.js-query-message-panel');
    const navbar = $('nav.navbar-default');
    const win = $(window);

    let topHeight = 350;
    const bottomHeight = 350;
    let visible = false;

    function resize() {
        if (visible) {
            const navbarHeight = navbar.height();
            const wrapperHeight = wrapper.height();
            const msgPanelHeight = msgPanel.height();
            let h;

            if (navbarHeight > wrapperHeight) {
                h = navbarHeight;
            }

            if (navbarHeight < wrapperHeight) {
                h = win.height();
            }

            if (h) {
                if (h < topHeight + bottomHeight) {
                    h = topHeight + bottomHeight;
                }
                // qdb.setHeight(wrapper, h - 1);
            }

            qdb.setHeight(consoleTop, topHeight);
            qdb.setHeight(editor, topHeight);
            qdb.setHeight(sqlEditor, topHeight - msgPanelHeight - 60);
        }
    }

    function switchToGrid() {
        // $('#editor').show();
        $('#js-toggle-chart').removeClass('active');
        $('#js-toggle-grid').addClass('active');
    }

    function loadSplitterPosition() {
        if (typeof (Storage) !== 'undefined') {
            const n = localStorage.getItem('splitter.position');
            if (n) {
                topHeight = parseInt(n);
            }
        }
    }

    function saveSplitterPosition() {
        if (typeof (Storage) !== 'undefined') {
            localStorage.setItem('splitter.position', topHeight);
        }
    }

    function toggleVisibility(x, name) {
        if (name === 'console') {
            visible = true;
            divSqlPanel.show();
        } else {
            visible = false;
            divSqlPanel.hide();
        }
    }


    function setup(bus) {
        win.bind('resize', resize);
        bus.on(qdb.MSG_QUERY_DATASET, function (e, m) {
            divExportUrl.val(qdb.toExportUrl(m.query));
        });

        divExportUrl.click(function () {
            this.select();
        });

        /* eslint-disable no-new */
        new Clipboard('.js-export-copy-url');
        $('.js-query-refresh').click(function () {
            bus.trigger('grid.refresh');
        });

        // named splitter
        bus.on('splitter.console.resize', function (x, e) {
            topHeight += e;
            win.trigger('resize');
            bus.trigger('preferences.save');
        });

        bus.on('preferences.save', saveSplitterPosition);
        bus.on('preferences.load', loadSplitterPosition);
        bus.on(qdb.MSG_ACTIVE_PANEL, toggleVisibility);

        bus.query();
        bus.domController();

        sqlEditor.editor(bus);

        $('#grid').grid(bus);
        $('#sp1').splitter(bus, 'console', 200, 0);

        switchToGrid();

        // wire query publish
        $('#js-toggle-chart').click(function () {
            bus.trigger('query.publish');
        });
    }

    $.extend(true, window, {
        qdb: {
            setupConsoleController: setup,
            switchToGrid
        }
    });
}(jQuery));
