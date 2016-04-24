/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2016 Appsicle
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

        var requestParams = {
            'query': '',
            'limit': '',
            'withCount': false
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
            requestParams.limit = '0,1000';
            requestParams.withCount = false;
            time = new Date().getTime();
            hActiveRequest = $.get('/js', requestParams).done(handleServerResponse).fail(handleServerError);
        }

        function sendQuery(q) {
            qry = q;
            abortPending();
            setTimeout(qq, 50);
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

        function bind() {
            $(document).on('editor.execute', submitQuery);
            $(document).on('editor.show.error', showError);

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
});

