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
/*globals qdb:false */
/*globals jQuery:false */
/*globals Clipboard:false */

(function ($) {
    'use strict';

    var divSqlPanel = $('.js-sql-panel');
    var divImportPanel = $('.js-import-panel');
    var divExportUrl = $('.js-export-url');
    var editor = $('#editor');
    var sqlEditor = $('#sqlEditor');
    var chart = $('#chart');
    var consoleTop = $('#console-top');
    var wrapper = $('#page-wrapper');
    var navbar = $('nav.navbar-default');
    // var body = $('body');
    var msgPanel = editor.find('.js-query-message-panel');

    function resize() {
        // if ($(this).width() < 769) {
        //     body.addClass('body-small');
        // } else {
        //     body.removeClass('body-small');
        // }

        var navbarHeight = navbar.height();
        var wrapperHeight = wrapper.height();
        var msgPanelHeight = msgPanel.height();
        var h;

        if (navbarHeight > wrapperHeight) {
            h = navbarHeight;
        }

        if (navbarHeight < wrapperHeight) {
            h = $(window).height();
        }

        if (h) {
            wrapper.css('height', h + 'px');
            wrapper.css('min-height', h + 'px');
        }

        var topHeight = 350;
        consoleTop.css('height', topHeight + 'px');
        editor.css('height', topHeight + 'px');
        sqlEditor.css('height', (topHeight - msgPanelHeight - 60) + 'px');
        chart.css('height', topHeight + 'px');
    }

    function switchToEditor() {
        divSqlPanel.show();
        divImportPanel.hide();
        $(document).trigger('active.panel', 'console');
    }

    function switchToImport() {
        divSqlPanel.hide();
        divImportPanel.show();
        $(document).trigger('active.panel', 'import');
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

    function setup(bus) {
        $('#side-menu').metisMenu();
        $(window).bind('resize', resize);
        $('a#sql-editor').click(switchToEditor);
        $('a#file-upload').click(switchToImport);
        $(bus).on('query.build.execute', switchToEditor);
        $(bus).on(qdb.MSG_QUERY_DATASET, function (e, m) {
            divExportUrl.val(qdb.toExportUrl(m.query));
        });
        divExportUrl.click(function () {
            this.select();
        });

        /* eslint-disable no-new */
        new Clipboard('.js-export-copy-url');
        $('.js-query-refresh').click(function () {
            $(bus).trigger('grid.refresh');
        });
        $('#js-toggle-chart').click(switchToChart);
        $('#js-toggle-grid').click(switchToGrid);
    }

    $.extend(true, window, {
        qdb: {
            setup
        }
    });
}(jQuery));

$(document).ready(function () {
    'use strict';
    var bus = {};
    qdb.setup(bus);
    $(bus).query();
    $(bus).domController();
    $('#sqlEditor').editor(bus);
    $('#grid').grid(bus);
    $('#dragTarget').dropbox();
    $('#import-file-list').importManager();
    $('#import-detail').importEditor(bus);
    $('#chart').chart(document);
});

$(window).load(function () {
    'use strict';
    $(window).trigger('resize');
});
