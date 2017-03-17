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
/*globals ace:false */

(function ($) {
    'use strict';

    const queryBatchSize = 1000;
    const MSG_QUERY_EXEC = 'query.in.exec';
    const MSG_QUERY_CANCEL = 'query.in.cancel';
    const MSG_QUERY_RUNNING = 'query.out.running';
    const MSG_QUERY_OK = 'query.out.ok';
    const MSG_QUERY_ERROR = 'query.out.error';
    const MSG_QUERY_DATASET = 'query.out.dataset';
    const MSG_QUERY_FIND_N_EXEC = 'query.build.execute';
    const MSG_ACTIVE_PANEL = 'active.panel';

    function toExportUrl(query) {
        return window.location.protocol + '//' + window.location.host + '/exp?query=' + encodeURIComponent(query);
    }

    function setHeight(element, height) {
        element.css('height', height + 'px');
        element.css('min-height', height + 'px');
    }

    function createEditor(div) {
        const edit = ace.edit(div);
        edit.getSession().setMode('ace/mode/questdb');
        edit.setTheme('ace/theme/merbivore_soft');
        edit.setShowPrintMargin(false);
        edit.setDisplayIndentGuides(false);
        edit.setHighlightActiveLine(false);
        edit.$blockScrolling = Infinity;

        $(window).on('resize', function () {
            edit.resize();
        });

        return edit;
    }

    function parseColumns(columns) {
        const colArray = columns.split(',');
        const columnMap = new Map();
        const n = colArray.length;
        for (let i = 0; i < n; i++) {
            columnMap.set(colArray[i].trim(), null);
        }
        return columnMap;
    }

    $.extend(true, window, {
        qdb: {
            queryBatchSize,
            MSG_QUERY_EXEC,
            MSG_QUERY_CANCEL,
            MSG_QUERY_RUNNING,
            MSG_QUERY_OK,
            MSG_QUERY_ERROR,
            MSG_QUERY_DATASET,
            MSG_ACTIVE_PANEL,
            MSG_QUERY_FIND_N_EXEC,
            toExportUrl,
            setHeight,
            createEditor,
            parseColumns
        }
    });
}(jQuery));
