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

/*globals $:false */
/*globals qdb:false */
/*globals jQuery:false */
/*globals Clipboard:false */

(function ($) {
    'use strict';

    const divSqlPanel = $('.js-sql-panel');
    const divImportPanel = $('.js-import-panel');
    const divExportUrl = $('.js-export-url');
    const editor = $('#editor');
    const sqlEditor = $('#sqlEditor');
    const chart = $('#chart');
    const consoleTop = $('#console-top');
    const wrapper = $('#page-wrapper');
    const navbar = $('nav.navbar-default');
    const msgPanel = editor.find('.js-query-message-panel');
    let topHeight = 350;
    const bottomHeight = 350;
    let ebus;

    function resize() {
        const navbarHeight = navbar.height();
        const wrapperHeight = wrapper.height();
        const msgPanelHeight = msgPanel.height();
        let h;

        if (navbarHeight > wrapperHeight) {
            h = navbarHeight;
        }

        if (navbarHeight < wrapperHeight) {
            h = $(window).height();
        }

        if (h) {

            if (h < topHeight + bottomHeight) {
                h = topHeight + bottomHeight;
            }

            wrapper.css('height', h + 'px');
            wrapper.css('min-height', h + 'px');
        }

        consoleTop.css('height', topHeight + 'px');
        consoleTop.css('min-height', topHeight + 'px');
        editor.css('height', topHeight + 'px');
        editor.css('min-height', topHeight + 'px');
        sqlEditor.css('height', (topHeight - msgPanelHeight - 60) + 'px');
        sqlEditor.css('min-height', (topHeight - msgPanelHeight - 60) + 'px');
        chart.css('height', topHeight + 'px');
        chart.css('min-height', topHeight + 'px');
    }

    function switchToEditor() {
        divSqlPanel.show();
        divImportPanel.hide();
        ebus.trigger('active.panel', 'console');
    }

    function switchToImport() {
        divSqlPanel.hide();
        divImportPanel.show();
        ebus.trigger('active.panel', 'import');
    }

    function switchToGrid() {
        $('#chart').hide();
        $('#editor').show();
        $('#js-toggle-chart').removeClass('active');
        $('#js-toggle-grid').addClass('active');
    }

    function switchToChart() {
        $('#chart').show();
        $('#editor').hide();
        $('#js-toggle-chart').addClass('active');
        $('#js-toggle-grid').removeClass('active');
        $(document).trigger('chart.draw');
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

    function setup(b) {
        ebus = b;

        $('#side-menu').metisMenu();
        $(window).bind('resize', resize);
        $('a#sql-editor').click(switchToEditor);
        $('a#file-upload').click(switchToImport);
        b.on('query.build.execute', switchToEditor);
        b.on(qdb.MSG_QUERY_DATASET, function (e, m) {
            divExportUrl.val(qdb.toExportUrl(m.query));
        });
        divExportUrl.click(function () {
            this.select();
        });

        /* eslint-disable no-new */
        new Clipboard('.js-export-copy-url');
        $('.js-query-refresh').click(function () {
            b.trigger('grid.refresh');
        });
        $('#js-toggle-chart').click(switchToChart);
        $('#js-toggle-grid').click(switchToGrid);

        b.on('splitter.resize', function (x, e) {
            topHeight += e;
            $(window).trigger('resize');
            b.trigger('preferences.save');
        });

        b.on('preferences.save', saveSplitterPosition);
        b.on('preferences.load', loadSplitterPosition);
    }

    $.extend(true, window, {
        qdb: {
            setup,
            switchToGrid
        }
    });
}(jQuery));

let bus;

$(document).ready(function () {
    'use strict';
    bus = $({});
    qdb.setup(bus);

    bus.query();
    bus.domController();

    $('#sqlEditor').editor(bus);
    $('#grid').grid(bus);
    $('#dragTarget').dropbox(bus);
    $('#import-file-list').importManager(bus);
    $('#import-detail').importEditor(bus);
    $('#chart').chart(document);

    qdb.switchToGrid();
    $('#sp1').splitter(bus, 200, 0);
    bus.trigger('preferences.load');
});

$(window).load(function () {
    'use strict';
    $(window).trigger('resize');
    bus.trigger('editor.focus');
});
