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

    function fixHeight() {
        var b = $('body');
        var navbarHeight = $('nav.navbar-default').height();
        var pw = $('#page-wrapper');
        var wrapperHeight = pw.height();

        if (navbarHeight > wrapperHeight) {
            pw.css('min-height', navbarHeight + 'px');
        }

        if (navbarHeight < wrapperHeight) {
            pw.css('min-height', $(window).height() + 'px');
        }

        if (b.hasClass('fixed-nav')) {
            if (navbarHeight > wrapperHeight) {
                pw.css('min-height', navbarHeight - 60 + 'px');
            } else {
                pw.css('min-height', $(window).height() - 60 + 'px');
            }
        }
    }

    function fixBodyClass() {
        if ($(this).width() < 769) {
            $('body').addClass('body-small');
        } else {
            $('body').removeClass('body-small');
        }
    }

    function toggleMenu() {
        $('body').toggleClass('mini-navbar');
        $('.navbar-default').addClass('animated slideInLeft').one('webkitAnimationEnd mozAnimationEnd MSAnimationEnd oanimationend animationend', function () {
            $(this).removeClass('animated slideInLeft');
        });
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

    function setup() {
        $('#side-menu').metisMenu();
        $('.navbar-minimalize').click(toggleMenu);

        fixBodyClass();
        fixHeight();

        $(window).bind('load resize scroll', function () {
            if (!$('body').hasClass('body-small')) {
                fixHeight();
            }
        });
        $(window).bind('resize', fixBodyClass);
        $('a#sql-editor').click(switchToEditor);
        $('a#file-upload').click(switchToImport);
        $(document).on('query.build.execute', switchToEditor);
        $(document).on('query.grid', function (e, m) {
            divExportUrl.val(qdb.toExportUrl(m.r.query));
        });
        divExportUrl.click(function () {
            this.select();
        });

        /* eslint-disable no-new */
        new Clipboard('.js-export-copy-url');
        $('.js-query-refresh').click(function () {
            $(document).trigger('grid.refresh');
        });
    }

    $.extend(true, window, {
        qdb: {
            setup
        }
    });
}(jQuery));

$(document).ready(function () {
    'use strict';
    qdb.setup();
});
