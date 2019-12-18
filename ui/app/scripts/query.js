/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

/*globals jQuery:false */
/*globals qdb:false */
/*globals ace:false */

(function ($) {
    'use strict';

    $.fn.query = function () {
        const bus = $(this);
        let qry;
        let hActiveRequest = null;
        let hPendingRequest = null;
        let time;
        const batchSize = qdb.queryBatchSize;

        const requestParams = {
            'query': '',
            'limit': ''
        };

        function cancelActiveQuery() {
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
            bus.trigger(qdb.MSG_QUERY_OK, {
                delta: (new Date().getTime() - time),
                count: r.count
            });

            if (r.dataset) {
                bus.trigger(qdb.MSG_QUERY_DATASET, r);
            }
            hActiveRequest = null;
        }

        function handleServerError(jqXHR) {
            bus.trigger(qdb.MSG_QUERY_ERROR,
                {
                    query: qry,
                    r: jqXHR.responseJSON,
                    status: jqXHR.status,
                    statusText: jqXHR.statusText,
                    delta: (new Date().getTime() - time)
                }
            );
            hActiveRequest = null;
        }

        function sendQueryDelayed() {
            cancelActiveQuery();
            requestParams.query = qry.q;
            requestParams.limit = '0,' + batchSize;
            requestParams.count = true;
            time = new Date().getTime();
            hActiveRequest = $.get('/exec', requestParams);
            bus.trigger(qdb.MSG_QUERY_RUNNING);
            hActiveRequest.done(handleServerResponse).fail(handleServerError);
        }

        //noinspection JSUnusedLocalSymbols
        function sendQuery(x, q) {
            qry = q;
            abortPending();
            hPendingRequest = setTimeout(sendQueryDelayed, 50);
        }

        function publishCurrentQuery() {
            bus.trigger('query.text', qry);
        }

        bus.on(qdb.MSG_QUERY_EXEC, sendQuery);
        bus.on(qdb.MSG_QUERY_CANCEL, cancelActiveQuery);
        bus.on('query.publish', publishCurrentQuery);
    };

    $.fn.domController = function () {
        const div = $('.js-query-spinner');
        const divMsg = $('.js-query-message-panel');
        const divTime = $('.js-query-message-panel .js-query-time');
        const divMsgText = $('.js-query-message-panel .js-query-message-text');
        let timer;
        let runBtn;
        let running = false;
        const bus = $(this);

        function delayedStart() {
            div.addClass('query-progress-animated', 100);
            divMsg.addClass('query-message-ok');
            divTime.html('-');
            divMsgText.html('Running...');
        }

        function start() {
            running = true;
            runBtn.html('<i class="fa fa-stop"></i>Cancel');
            runBtn.removeClass('js-query-run').addClass('js-query-cancel');
            timer = setTimeout(delayedStart, 500);
        }

        function stop() {
            runBtn.html('<i class="fa fa-play"></i>Run');
            runBtn.removeClass('js-query-cancel').addClass('js-query-run');
            clearTimeout(timer);
            div.removeClass('query-progress-animated');
            running = false;
        }

        function toTextPosition(q, pos) {
            let r = 0, c = 0, n = Math.min(pos, q.q.length);
            for (let i = 0; i < n; i++) {
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
            stop();
            divMsg.removeClass('query-message-ok').addClass('query-message-error');
            divTime.html('failed after <strong>' + (m.delta / 1000) + 's</strong>');
            if (m.statusText === 'abort') {
                divMsgText.html('Cancelled by user');
            } else if (m.r) {
                const pos = toTextPosition(m.query, m.r.position);
                divMsgText.html('<strong>' + pos.r + ':' + pos.c + '</strong>&nbsp;&nbsp;' + m.r.error);
                bus.trigger('editor.show.error', pos);
            } else if (m.status === 0) {
                divMsgText.html('Server down?');
            } else {
                divMsgText.html('Server error: ' + m.status);
            }
        }

        //noinspection JSUnusedLocalSymbols
        function ok(x, m) {
            stop();
            divMsg.removeClass('query-message-error').addClass('query-message-ok');
            divTime.html('read in <strong>' + (m.delta / 1000) + 's</strong>');
            if (m.count) {
                divMsgText.html(m.count.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',') + ' rows');
            } else {
                divMsgText.html('done');
            }
        }

        function toggleRunBtn() {
            if (running) {
                bus.trigger(qdb.MSG_QUERY_CANCEL);
            } else {
                bus.trigger('editor.execute');
            }
        }

        function exportClick(e) {
            e.preventDefault();
            bus.trigger('grid.publish.query');
        }

        //noinspection JSUnusedLocalSymbols
        function exportQuery(x, query) {
            if (query) {
                window.location.href = '/exp?query=' + query;
            }
        }

        function bind() {
            runBtn = $('.js-query-run');
            runBtn.click(toggleRunBtn);
            bus.on(qdb.MSG_QUERY_ERROR, error);
            bus.on(qdb.MSG_QUERY_OK, ok);
            bus.on(qdb.MSG_QUERY_RUNNING, start);
            bus.on('grid.query', exportQuery);

            $('.js-editor-toggle-invisible').click(function () {
                bus.trigger('editor.toggle.invisibles');
            });
            $('.js-query-export').click(exportClick);
        }

        bind();
    };

    $.fn.editor = function (msgBus) {
        let edit;
        const storeKeys = {
            text: 'query.text',
            line: 'editor.line',
            col: 'editor.col'
        };

        const Range = ace.require('ace/range').Range;
        let marker;
        const searchOpts = {
            wrap: true,
            caseSensitive: true,
            wholeWord: false,
            regExp: false,
            preventScroll: false
        };
        const bus = msgBus;
        const element = this;

        function clearMarker() {
            if (marker) {
                edit.session.removeMarker(marker);
                marker = null;
            }
        }

        function setup() {
            edit = qdb.createEditor(element[0]);
            edit.session.on('change', clearMarker);
        }

        function loadPreferences() {
            if (typeof (Storage) !== 'undefined') {
                const q = localStorage.getItem(storeKeys.text);
                if (q) {
                    edit.setValue(q);
                }

                const row = localStorage.getItem(storeKeys.line);
                const col = localStorage.getItem(storeKeys.col);

                if (row && col) {
                    edit.gotoLine(row, col);
                }
            }
        }

        function savePreferences() {
            if (typeof (Storage) !== 'undefined') {
                localStorage.setItem(storeKeys.text, edit.getValue());
                localStorage.setItem(storeKeys.line, edit.getCursorPosition().row + 1);
                localStorage.setItem(storeKeys.col, edit.getCursorPosition().column);
            }
        }

        function computeQueryTextFromCursor() {
            const text = edit.getValue();
            const pos = edit.getCursorPosition();
            let r = 0;
            let c = 0;

            let startRow = 0;
            let startCol = 0;
            let startPos = -1;
            let sql = null;
            let inQuote = false;

            // console.log('offset=' + edit.getSession().getDocument().positionToIndex(pos, 0));

            for (let i = 0; i < text.length; i++) {
                const char = text.charAt(i);

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
                                sql = text.substring(0, i).trim();
                            } else {
                                sql = text.substring(startPos, i).trim();
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
                    sql = text.substring(startPos).trim();
                }
            }

            if (sql.length === 0) {
                return null;
            }

            return {q: sql, r: startRow, c: startCol};
        }

        function computeQueryTextFromSelection() {
            let q = edit.getSelectedText();
            let n = q.length;
            let c;
            while (n > 0 && ((c = q.charAt(n)) === ' ' || c === '\n' || c === ';')) {
                n--;
            }

            if (n > 0) {
                q = q.substr(0, n + 1);
                const range = edit.getSelectionRange();
                return {q, r: range.start.row, c: range.start.column};
            }

            return null;
        }

        function submitQuery() {
            bus.trigger('preferences.save');
            clearMarker();
            let q;
            if (edit.getSelectedText() === '') {
                q = computeQueryTextFromCursor();
            } else {
                q = computeQueryTextFromSelection();
            }

            if (q) {
                bus.trigger(qdb.MSG_QUERY_EXEC, q);
            }
        }

        //noinspection JSUnusedLocalSymbols
        function showError(x, pos) {
            const token = edit.session.getTokenAt(pos.r - 1, pos.c);
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

        function focusGrid() {
            bus.trigger('grid.focus');
        }

        //noinspection JSUnusedLocalSymbols
        function findOrInsertQuery(e, q) {
            // "select" existing query or append text of new one
            // "find" will select text if anything is found, so we just
            // execute whats there
            if (!edit.find('\'' + q + '\'', searchOpts)) {
                const row = edit.session.getLength();
                const text = '\n\'' + q + '\';';
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

        function insertColumn(e, q) {
            edit.insert(', ' + q);
            edit.focus();
        }

        function bind() {
            bus.on('editor.execute', submitQuery);
            bus.on('editor.show.error', showError);
            bus.on('editor.toggle.invisibles', toggleInvisibles);
            bus.on(qdb.MSG_QUERY_FIND_N_EXEC, findOrInsertQuery);
            bus.on('editor.insert.column', insertColumn);
            bus.on('editor.focus', function () {
                edit.scrollToLine(edit.getCursorPosition().row + 1, true, true, function () {
                });
                edit.focus();
            });

            edit.commands.addCommand({
                name: 'editor.execute',
                bindKey: 'F9',
                exec: submitQuery
            });

            edit.commands.addCommand({
                name: 'editor.focus.grid',
                bindKey: 'F2',
                exec: focusGrid
            });

            bus.on('preferences.load', loadPreferences);
            bus.on('preferences.save', savePreferences);
        }

        setup();
        bind();
    };
}(jQuery));
