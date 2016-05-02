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

        function handleServerResponse(r) {
            $(document).trigger('query.ok',
                {
                    r,
                    delta: (new Date().getTime() - time)
                }
            );
        }

        function handleServerError(jqXHR) {
            $(document).trigger('query.error',
                {
                    query: qry,
                    r: jqXHR.responseJSON,
                    status: jqXHR.status,
                    delta: (new Date().getTime() - time)
                }
            );
        }

        function qq() {
            abortActive();
            requestParams.query = qry.q;
            requestParams.limit = '0,' + batchSize;
            time = new Date().getTime();
            hActiveRequest = $.get('/js', requestParams).done(handleServerResponse).fail(handleServerError);
        }

        function sendQuery(q) {
            qry = q;
            abortPending();
            hPendingRequest = setTimeout(qq, 50);
        }

        function bind() {
            $(document).on('query.execute', function (x, q) {
                sendQuery(q);
            });
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

            if (m.statusText === 'abort') {
                return;
            }

            div.removeClass('query-progress-animated');
            divMsg.removeClass('query-message-ok').addClass('query-message-error');
            divTime.html('failed after <strong>' + (m.delta / 1000) + 's</strong>');
            if (m.r) {
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
            divMsgText.html('OK');
        }

        function bind() {
            $(document).on('query.execute', start);
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

        function submitQuery() {
            save();
            clearMarker();
            var q = edit.getSelectedText();
            var r;
            var c;
            if (q == null || q === '') {
                q = edit.getValue();
                r = 0;
                c = 0;
            } else {
                var range = edit.getSelectionRange();
                r = range.start.row;
                c = range.start.column;
            }
            $(document).trigger('query.execute', {q, r, c});
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
            console.log('about to ask');
            edit.renderer.setShowInvisibles(!edit.renderer.getShowInvisibles());
        }

        function bind() {
            $(document).on('editor.execute', submitQuery);
            $(document).on('editor.show.error', showError);
            $(document).on('editor.toggle.invisibles', toggleInvisibles);

            edit.commands.addCommand({
                name: 'editor.execute',
                bindKey: 'F9',
                exec: submitQuery
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

    $('.js-query-run').click(function () {
        $(document).trigger('editor.execute');
    });

    $('.js-editor-toggle-invisible').click(function () {
        $(document).trigger('editor.toggle.invisibles');
    });
});

