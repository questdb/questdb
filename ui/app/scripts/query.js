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
/*globals qdb:false */
/*globals ace:false */

(function ($) {
    'use strict';

    function query() {
        var qry;
        var hActiveRequest = null;
        var hPendingRequest = null;
        var time;
        var batchSize = qdb.queryBatchSize;
        var btn;

        var requestParams = {
            'query': '',
            'limit': ''
        };

        function abortActive() {
            if (hActiveRequest !== null) {
                hActiveRequest.abort();
                hActiveRequest = null;
            }
        }

        function abortPending() {
            if (hPendingRequest !== null) {
                clearTimeout(hPendingRequest);
                hPendingRequest = null;
            }
        }

        function readyToExecuteAgain() {
            hActiveRequest = null;
            btn.html('<i class="fa fa-play"></i>Run');
            btn.removeClass('js-query-cancel').addClass('js-query-run');
        }

        function handleServerResponse(r) {
            $(document).trigger('query.ok', {
                delta: (new Date().getTime() - time),
                count: r.count
            });
            if (r.result) {
                $(document).trigger('query.grid',
                    {
                        r,
                        count: r.count
                    }
                );
            }
            readyToExecuteAgain();
        }

        function handleServerError(jqXHR) {
            $(document).trigger('query.error',
                {
                    query: qry,
                    r: jqXHR.responseJSON,
                    status: jqXHR.status,
                    statusText: jqXHR.statusText,
                    delta: (new Date().getTime() - time)
                }
            );
            readyToExecuteAgain();
        }

        function qq() {
            abortActive();
            btn.html('<i class="fa fa-stop"></i>Cancel');
            btn.removeClass('js-query-run').addClass('js-query-cancel');
            requestParams.query = qry.q;
            requestParams.limit = '0,' + batchSize;
            requestParams.count = true;
            time = new Date().getTime();
            hActiveRequest = $.get('/js', requestParams).done(handleServerResponse).fail(handleServerError);
        }

        function sendQuery(q) {
            qry = q;
            abortPending();
            hPendingRequest = setTimeout(qq, 50);
        }

        function executeToggle() {
            if (hActiveRequest !== null) {
                abortActive();
            } else {
                $(document).trigger('editor.execute');
            }
        }

        function bind() {
            $(document).on(qdb.MSG_QUERY_EXEC, function (x, q) {
                sendQuery(q);
            });

            $(document).on('query.toggle', function () {
                executeToggle();
            });

            btn = $('.js-query-run');
            btn.click(executeToggle);
        }

        bind();
    }

    function spinner() {
        var div = $('.js-query-spinner');
        var divMsg = $('.js-query-message-panel');
        var divTime = $('.js-query-message-panel .js-query-time');
        var divMsgText = $('.js-query-message-panel .js-query-message-text');
        var timer;

        function delayedStart() {
            div.addClass('query-progress-animated', 100);
            divMsg.addClass('query-message-ok');
            divTime.html('-');
            divMsgText.html('Running...');
        }

        function start() {
            timer = setTimeout(delayedStart, 500);
        }

        function toTextPosition(q, pos) {
            var r = 0, c = 0, n = Math.min(pos, q.q.length);
            for (var i = 0; i < n; i++) {
                if (q.q.charAt(i) === '\n') {
                    r++;
                    c = 0;
                } else {
                    c++;
                }
            }

            return {
                r: r + 1 + q.r,
                c: (r === 0 ? c + q.c : c) + 1
            };
        }

        //noinspection JSUnusedLocalSymbols
        function error(x, m) {
            clearTimeout(timer);
            div.removeClass('query-progress-animated');
            divMsg.removeClass('query-message-ok').addClass('query-message-error');
            divTime.html('failed after <strong>' + (m.delta / 1000) + 's</strong>');
            if (m.statusText === 'abort') {
                divMsgText.html('Cancelled by user');
            } else if (m.r) {
                var pos = toTextPosition(m.query, m.r.position);
                divMsgText.html('<strong>' + pos.r + ':' + pos.c + '</strong>&nbsp;&nbsp;' + m.r.error);
                $(document).trigger('editor.show.error', pos);
            } else if (m.status === 0) {
                divMsgText.html('Server down?');
            } else {
                divMsgText.html('Server error: ' + m.status);
            }
        }

        //noinspection JSUnusedLocalSymbols
        function ok(x, m) {
            clearTimeout(timer);
            div.removeClass('query-progress-animated');
            divMsg.removeClass('query-message-error').addClass('query-message-ok');
            divTime.html('read in <strong>' + (m.delta / 1000) + 's</strong>');
            if (m.count) {
                divMsgText.html(m.count.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',') + ' rows');
            } else {
                divMsgText.html('done');
            }
        }

        function bind() {
            $(document).on(qdb.MSG_QUERY_EXEC, start);
            $(document).on('query.error', error);
            $(document).on('query.ok', ok);
        }

        bind();
    }

    function editor() {
        var edit;
        var item = 'query.text';
        var Range = ace.require('ace/range').Range;
        var marker;
        var searchOpts = {
            wrap: true,
            caseSensitive: true,
            wholeWord: false,
            regExp: false,
            preventScroll: false
        };


        function clearMarker() {
            if (marker) {
                edit.session.removeMarker(marker);
                marker = null;
            }
        }

        function setup() {
            edit = ace.edit('sqlEditor');
            edit.getSession().setMode('ace/mode/sql');
            edit.setTheme('ace/theme/merbivore_soft');
            edit.setShowPrintMargin(false);
            edit.setDisplayIndentGuides(false);
            edit.setHighlightActiveLine(false);
            edit.session.on('change', clearMarker);
            edit.$blockScrolling = Infinity;
            $('#sqlEditor').css('height', '240px');
        }

        function load() {
            if (typeof (Storage) !== 'undefined') {
                var q = localStorage.getItem(item);
                if (q) {
                    edit.setValue(q);
                }
            }
        }

        function save() {
            if (typeof (Storage) !== 'undefined') {
                localStorage.setItem(item, edit.getValue());
            }
        }

        function computeQueryTextFromCursor() {
            var text = edit.getValue();
            var pos = edit.getCursorPosition();
            var r = 0;
            var c = 0;

            var startRow = 0;
            var startCol = 0;
            var startPos = -1;
            var sql = null;
            var inQuote = false;


            for (var i = 0; i < text.length; i++) {
                var char = text.charAt(i);

                switch (char) {
                    case ';':
                        if (inQuote) {
                            c++;
                            continue;
                        }

                        if (r < pos.row || (r === pos.row && c < pos.column)) {
                            startRow = r;
                            startCol = c;
                            startPos = i + 1;
                            c++;
                        } else {
                            if (startPos === -1) {
                                sql = text.substring(0, i);
                            } else {
                                sql = text.substring(startPos, i);
                            }
                        }
                        break;
                    case '\n':
                        r++;
                        c = 0;
                        break;
                    case '\'':
                        inQuote = !inQuote;
                        c++;
                        break;
                    default:
                        c++;
                        break;
                }

                if (sql !== null) {
                    break;
                }
            }

            if (sql === null) {
                if (startPos === -1) {
                    sql = text;
                } else {
                    sql = text.substring(startPos);
                }
            }

            if (sql.length === 0) {
                return null;
            }

            return {q: sql, r: startRow, c: startCol};
        }

        function computeQueryTextFromSelection() {
            var q = edit.getSelectedText();
            var n = q.length;
            var c;
            while (n > 0 && ((c = q.charAt(n)) === ' ' || c === '\n' || c === ';')) {
                n--;
            }

            if (n > 0) {
                q = q.substr(0, n + 1);
                var range = edit.getSelectionRange();
                return {q, r: range.start.row, c: range.start.column};
            }

            return null;
        }

        function submitQuery() {
            save();
            clearMarker();
            var q;
            if (edit.getSelectedText() === '') {
                q = computeQueryTextFromCursor();
            } else {
                q = computeQueryTextFromSelection();
            }

            if (q) {
                $(document).trigger(qdb.MSG_QUERY_EXEC, q);
            }
        }

        //noinspection JSUnusedLocalSymbols
        function showError(x, pos) {
            var token = edit.session.getTokenAt(pos.r - 1, pos.c);
            marker = edit.session.addMarker(
                new Range(pos.r - 1, pos.c - 1, pos.r - 1, pos.c + token.value.length - 1),
                'js-syntax-error',
                'text',
                true);

            edit.gotoLine(pos.r, pos.c - 1);
            edit.focus();
        }

        function toggleInvisibles() {
            edit.renderer.setShowInvisibles(!edit.renderer.getShowInvisibles());
        }

        function queryToggle() {
            $(document).trigger('query.toggle');
        }

        function focusGrid() {
            $(document).trigger('grid.focus');
        }

        //noinspection JSUnusedLocalSymbols
        function findOrInsertQuery(e, q) {
            // "select" existing query or append text of new one
            // "find" will select text if anything is found, so we just
            // execute whats there
            if (!edit.find('\'' + q + '\'', searchOpts)) {
                var row = edit.session.getLength();
                var text = '\n\'' + q + '\';';
                edit.session.insert({
                    row,
                    column: 0
                }, text);
                edit.selection.moveCursorToPosition({
                    row: row + 1,
                    column: 0
                });
                edit.selection.selectLine();
            }
            submitQuery();
        }

        function bind() {
            $(document).on('editor.execute', submitQuery);
            $(document).on('editor.show.error', showError);
            $(document).on('editor.toggle.invisibles', toggleInvisibles);
            $(document).on('query.build.execute', findOrInsertQuery);
            $(document).on('editor.focus', function () {
                edit.focus();
            });

            edit.commands.addCommand({
                name: 'editor.execute',
                bindKey: 'F9',
                exec: queryToggle
            });

            edit.commands.addCommand({
                name: 'editor.focus.grid',
                bindKey: 'F2',
                exec: focusGrid
            });
        }

        setup();
        load();
        bind();
    }

    $.extend(true, window, {
        qdb: {
            query,
            spinner,
            editor
        }
    });
}(jQuery));

$(document).ready(function () {
    'use strict';

    qdb.query();
    qdb.spinner();
    qdb.editor();

    $('.js-editor-toggle-invisible').click(function () {
        $(document).trigger('editor.toggle.invisibles');
    });
});

