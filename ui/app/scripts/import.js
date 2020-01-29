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
/*eslint no-use-before-define: 0*/


/**
 * @return {string}
 */
function s4() {
    'use strict';
    return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
}

function guid() {
    'use strict';
    return (s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4());
}

function fmtNumber(n) {
    'use strict';
    return n.toFixed(0).replace(/./g, function (c, i, a) {
        return i && c !== '.' && ((a.length - i) % 3 === 0) ? ',' + c : c;
    });
}

function toSize(x) {
    'use strict';
    if (x < 1024) {
        return x;
    }

    if (x < 1024 * 1024) {
        return Math.round(x / 1024) + 'KB';
    }

    if (x < 1024 * 1024 * 1024) {
        return Math.round(x / 1024 / 1024) + 'MB';
    }

    return Math.round(x / 1024 / 1024 / 1024) + 'GB';
}

function nopropagation(e) {
    'use strict';

    e.stopPropagation();
    if (e.preventDefault) {
        e.preventDefault();
    }
}

(function ($) {
    'use strict';

    $.fn.importManager = function (editorBus) {
        const ACTION_DEFAULT = 0;
        const ACTION_APPEND = 1;
        const ACTTION_OVERWRITE = 2;

        const dict = {};
        const container = this;
        const ebus = editorBus;
        let canvas;
        let top = 0;
        let uploadQueue = [];
        let current = null;
        let currentHtml = null;
        const rowHeight = 35;
        let xhr = null;

        function updateProgress(event) {
            if (event.lengthComputable) {
                const pos = event.loaded || event.position;
                currentHtml.find(' > .ud-progress').css('width', (pos * 100 / current.size) + '%');
            }
        }

        function updateButtons() {
            let selected = false;
            let retry = false;

            for (let id in dict) {
                if (dict.hasOwnProperty(id)) {
                    const e = dict[id];
                    if (e.selected) {
                        selected = true;
                        if (e.retry) {
                            retry = true;
                            break;
                        }
                    }
                }
            }
            $('#btnImportClearSelected').attr('disabled', !selected);
            $('#btnRetry').attr('disabled', !retry);
        }

        function renderActions(e, element) {
            switch (e.retry) {
                case ACTION_APPEND:
                    element.find('.js-row-append').addClass('label-danger');
                    element.find('.js-row-overwrite').removeClass('label-danger');
                    break;
                case ACTTION_OVERWRITE:
                    element.find('.js-row-append').removeClass('label-danger');
                    element.find('.js-row-overwrite').addClass('label-danger');
                    break;
                default:
                    element.find('.js-row-append').removeClass('label-danger');
                    element.find('.js-row-overwrite').removeClass('label-danger');
                    break;
            }

            if (e.selected) {
                element.find('.js-row-toggle').removeClass('fa-square-o').addClass('fa-check-square-o');
            } else {
                element.find('.js-row-toggle').removeClass('fa-check-square-o').addClass('fa-square-o');
            }

            if (e.forceHeader) {
                element.find('.js-row-toggle-header').addClass('label-success');
            } else {
                element.find('.js-row-toggle-header').removeClass('label-success');
            }

            updateButtons();
        }

        //noinspection JSUnusedLocalSymbols
        function renderRowAsOverwrite(x, e) {
            e.retry = ACTTION_OVERWRITE;
            renderActions(e, $('#' + e.id + ' > .ud-c0'));
        }

        //noinspection JSUnusedLocalSymbols
        function renderRowAsAppend(x, e) {
            e.retry = ACTION_APPEND;
            renderActions(e, $('#' + e.id + ' > .ud-c0'));
        }

        //noinspection JSUnusedLocalSymbols
        function renderRowAsCancel(x, e) {
            e.retry = ACTION_DEFAULT;
            renderActions(e, $('#' + e.id + ' > .ud-c0'));
        }

        function setupUploadProgressCallback() {
            const xhrobj = $.ajaxSettings.xhr();
            if (xhrobj.upload) {
                xhrobj.upload.addEventListener('progress', updateProgress, false);
            }
            return xhrobj;
        }

        const importRequest = {
            xhr: setupUploadProgressCallback,
            url: '/imp?fmt=json',
            type: 'POST',
            contentType: false,
            processData: false,
            cache: false
        };

        const existenceCheckRequest = {
            type: 'GET',
            contentType: false,
            processData: false,
            cache: false
        };

        function updateBtnImportCancel() {
            $('#btnImportCancel').attr('disabled', current === null);
        }

        function toggleRow() {
            const btn = $(this);
            const e = dict[btn.parent().parent().attr('id')];
            e.selected = !e.selected;
            renderActions(e, btn.parent());
        }

        function toggleRowAppend() {
            const btn = $(this);
            const e = dict[btn.parent().parent().attr('id')];

            switch (e.retry) {
                case 1:
                    e.retry = 0;
                    e.selected = false;
                    break;
                default:
                    e.retry = 1;
                    e.selected = true;
                    break;
            }

            renderActions(e, btn.parent());
        }

        function toggleRowOverwrite() {
            const btn = $(this);
            const e = dict[btn.parent().parent().attr('id')];

            switch (e.retry) {
                case 2:
                    e.retry = 0;
                    e.selected = false;
                    break;
                default:
                    e.retry = 2;
                    e.selected = true;
                    break;
            }

            renderActions(e, btn.parent());
        }

        function toggleRowHeader() {
            const btn = $(this);
            const e = dict[btn.parent().parent().attr('id')];
            e.forceHeader = !e.forceHeader;
            renderActions(e, btn.parent());
        }

        function uploadRow() {
            const btn = $(this);
            const e = dict[btn.parent().parent().attr('id')];
            submitUploadTask(e);
        }

        function viewRow() {
            const btn = $(this);
            const e = dict[btn.parent().parent().attr('id')];
            ebus.trigger(qdb.MSG_QUERY_FIND_N_EXEC, e.name);
        }

        function showDetail(e) {
            const item = dict[$(this).parent().attr('id')];
            if (item.importState > -1) {
                $(document).trigger('import.detail', item);
            }
            nopropagation(e);
        }

        function render(e) {
            const html = $(`
                        <div id="${e.id}" class="ud-row" style="top: ${top}px;">
                            <div class="ud-cell ud-c0">
                                <i class="fa fa-square-o ud-checkbox js-row-toggle"></i>
                                <span class="label js-row-append">A</span>
                                <span class="label js-row-overwrite">O</span>
                                <span class="label js-row-toggle-header">H</span>
                                <i class="fa fa-upload js-row-upload"></i>
                            </div>
                            <div class="ud-cell ud-c1">${e.name}</div>
                            <div class="ud-cell ud-c2"><i class="fa fa-eye js-row-query"></i></div>
                            <div class="ud-cell ud-c3">${e.sizeFmt}</div>
                            <div class="ud-cell ud-c4 js-row-imported">?</div>
                            <div class="ud-cell ud-c5 js-row-rejected">?</div>
                            <div class="ud-cell ud-c6 js-row-header">?</div>
                            <div class="ud-cell ud-c7 ud-status">
                                <span class="label">pending</span>
                            </div>
                        </div>
            `);

            canvas.append(html);
            html.find('.js-row-toggle').click(toggleRow);
            html.find('.js-row-append').click(toggleRowAppend);
            html.find('.js-row-overwrite').click(toggleRowOverwrite);
            html.find('.js-row-toggle-header').click(toggleRowHeader);
            html.find('.js-row-upload').click(uploadRow);
            html.find('.js-row-query').click(viewRow);

            html.find('.ud-c1').click(showDetail);
            html.find('.ud-c2').click(showDetail);
            html.find('.ud-c3').click(showDetail);
            top += rowHeight;
        }

        function status(e, html, processNext) {
            const row = currentHtml;
            row.find(' > .ud-status').html(html);
            row.find(' > .ud-progress').remove();

            current.selected = false;
            if (processNext) {
                const next = uploadQueue.shift();
                if (next) {
                    retryOrCheckExistence(next);
                } else {
                    current = null;
                    currentHtml = null;
                    xhr = null;
                }
            }
            updateBtnImportCancel();
            $(document).trigger('import.detail.updated', e);
        }

        function importDone(data) {
            current.delta = new Date().getTime() - current.time;
            if (data.status === 'OK') {
                current.response = data;
                current.importState = 0; // ok
                renderRowAsCancel(null, current);

                currentHtml.find('.js-row-imported').html(fmtNumber(data.rowsImported));
                currentHtml.find('.js-row-rejected').html(fmtNumber(data.rowsRejected));
                currentHtml.find('.js-row-header').html(data.header ? 'Yes' : 'No');

                const type = data.rowsRejected > 0 ? 'label-warning' : 'label-success';
                status(current, '<span class="label ' + type + '">imported in ' + (current.delta / 1000) + 's</span>', true);
            } else {
                current.importState = 4; // error with journal, status has error message
                current.response = data.status;
                status(current, '<span class="label label-danger">failed</span>', true);
            }
        }

        function httpStatusToImportState(s) {
            switch (s) {
                case 0:
                    return 3; // server not responding
                case 500:
                    return 5; // internal error
                default:
                    return 101; // unknown
            }
        }

        function importFailed(r) {
            renderRowAsCancel(null, current);
            if (r.statusText !== 'abort') {
                current.response = r.responseText;
                current.importState = httpStatusToImportState(r.status);
                status(current, '<span class="label label-danger">failed</span>', true);
            } else {
                // current.importState = -1; // abort
                status(current, '<span class="label label-warning">aborted</span>', true);
            }
        }

        function setupImportRequest() {
            importRequest.url = '/imp?fmt=json';

            if (current.retry === ACTTION_OVERWRITE) {
                importRequest.url += '&overwrite=true';
            }

            if (current.forceHeader) {
                importRequest.url += '&forceHeader=true';
            }

            importRequest.xhr = setupUploadProgressCallback;
            importRequest.data = new FormData();

            // encode type overrides
            if (current.response && current.response.columns) {
                let schema = [];
                for (let i = 0; i < current.response.columns.length; i++) {
                    const c = current.response.columns[i];
                    if (c.altType && c.type !== c.altType.text && c.altType.text !== 'AUTO') {
                        schema.push({
                            name: c.name,
                            type: c.altType.value
                        });
                        // schema += c.name + '=' + c.altType.value + '&';
                    } else if (c.errors === 0 && c.type !== 'DATE' && (c.altType === undefined || c.altType.text !== 'AUTO')) {
                        schema.push({
                            name: c.name,
                            type: c.type
                        });
                        // schema += c.name + '=' + c.type + '&';
                    }
                }
                importRequest.data.append('schema', JSON.stringify(schema));
            }

            if (current.type === 'file') {
                importRequest.data.append('data', current.file);
            } else if (current.type === 'clipboard') {
                importRequest.url = importRequest.url + '&name=' + encodeURIComponent(current.name);
                importRequest.data.append('data', current.content);
            }
            current.time = new Date().getTime();
            return importRequest;
        }

        function sendImportRequest() {
            status(current, '<span class="label label-info">importing</span>', false);
            currentHtml.append(`<div class="ud-progress"></div>`);
            xhr = $.ajax(setupImportRequest()).done(importDone).fail(importFailed);
            // updateBtnImportCancel();
        }

        function existenceCheckFork(e) {
            switch (e.status) {
                case 'Exists':
                    current.importState = 1; // exists
                    status(current, '<span class="label label-danger">exists</span>', true);
                    break;
                case 'Does not exist':
                    current.importState = 0; // ok
                    sendImportRequest();
                    break;
                case 'Reserved name':
                    current.importState = 2; // exists foreign (reserved)
                    status(current, '<span class="label label-danger">reserved</span>', true);
                    break;
                default:
                    current.importState = 101; // unknown
                    status(current, '<span class="label label-danger">failed</span>', true);
                    break;
            }
        }

        function retryOrCheckExistence(e) {
            current = e;
            currentHtml = $('#' + e.id);
            if (e.retry) {
                current.importState = 0;
                sendImportRequest();
            } else {
                existenceCheckRequest.url = '/chk?f=json&j=' + encodeURIComponent(e.name);
                $.ajax(existenceCheckRequest).then(existenceCheckFork).fail(importFailed);
            }
        }

        function submitUploadTask(item) {
            if (current == null) {
                retryOrCheckExistence(item);
            } else if (current !== item) {
                uploadQueue.push(item);
            }

        }

        function enqueueImportItem(item) {
            dict[item.id] = item;
            render(item);
            submitUploadTask(item);
        }

        //noinspection JSUnusedLocalSymbols
        function addFiles(x, e) {
            for (let i = 0; i < e.files.length; i++) {
                const f = e.files[i];
                enqueueImportItem({
                    id: guid(),
                    name: f.name,
                    size: f.size,
                    file: f,
                    type: 'file',
                    sizeFmt: toSize(f.size),
                    selected: false,
                    imported: false,
                    forceHeader: false
                });
            }
        }

        //noinspection JSUnusedLocalSymbols
        function addClipboard(x, content) {
            enqueueImportItem({
                id: guid(),
                name: 'clipboard-' + new Date().getTime(),
                size: content.length,
                type: 'clipboard',
                content,
                sizeFmt: toSize(content.length),
                selected: false,
                imported: false,
                forceHeader: false
            });
        }

        function clearSelected() {
            for (let id in dict) {
                if (dict.hasOwnProperty(id)) {
                    const e = dict[id];
                    if (e.selected && e !== current) {
                        const uploadQueueIndex = uploadQueue.indexOf(e);
                        if (uploadQueueIndex > -1) {
                            delete uploadQueue[uploadQueueIndex];
                        }
                        $('#' + id).remove();
                        delete dict[id];
                        $(document).trigger('import.cleared', e);
                    }
                }
            }

            // rejig remaining rows
            top = 0;
            const rows = canvas.find('.ud-row');
            for (let i = 0; i < rows.length; i++) {
                $(rows[i]).css('top', top);
                top += rowHeight;
            }
            updateButtons();
        }

        function retrySelected() {
            for (let id in dict) {
                if (dict.hasOwnProperty(id)) {
                    const e = dict[id];
                    if (e.selected && e.retry) {
                        submitUploadTask(e);
                    }
                }
            }
        }

        function abortImport() {
            if (xhr !== null) {
                xhr.abort();
            }
        }

        function subscribe() {
            // subscribe to document event
            $(document).on('dropbox.files', addFiles);
            $(document).on('dropbox.clipboard', addClipboard);
            // $(document).on('import.clearSelected', clearSelected);
            // $(document).on('import.cancel', abortImport);
            // $(document).on('import.retry', retrySelected);

            $(document).on('import.line.overwrite', renderRowAsOverwrite);
            $(document).on('import.line.append', renderRowAsAppend);
            $(document).on('import.line.abort', renderRowAsCancel);

            $('#btnImportClearSelected').click(clearSelected);
            $('#btnImportCancel').click(abortImport);
            $('#btnRetry').click(retrySelected);

        }

        function init() {
            canvas = container.find('> .ud-canvas');
            subscribe();
        }

        init();

        return this;
    };

    // this class will manage drag&drop into dropbox element and
    // broadcast file readiness to document via custom event 'dropbox.files'
    $.fn.dropbox = function (bus) {

        let collection = $();
        const target = this;

        function startDrag() {
            target.addClass('drag-drop').removeClass('drag-idle');
        }

        function endDrag() {
            target.removeClass('drag-drop').addClass('drag-idle');
        }

        function handleDrop(evt) {
            nopropagation(evt);
            endDrag();
            collection = $();
            $(document).trigger('dropbox.files', evt.originalEvent.dataTransfer);
        }

        function handlePaste(evt) {
            let pastedText;
            if (window.clipboardData && window.clipboardData.getData) { // IE
                pastedText = window.clipboardData.getData('Text');
            } else if (evt.originalEvent.clipboardData && evt.originalEvent.clipboardData.getData) {
                pastedText = evt.originalEvent.clipboardData.getData('text/plain');
            }
            $(document).trigger('dropbox.clipboard', pastedText);
        }

        function handleDragEnter(event) {
            nopropagation(event);
            if (collection.size() === 0) {
                startDrag();
            }
            collection = collection.add(event.target);
        }

        function handleDragLeave(event) {
            /*
             * Firefox 3.6 fires the dragleave event on the previous element
             * before firing dragenter on the next one so we introduce a delay
             */
            setTimeout(function () {
                collection = collection.not(event.target);
                if (collection.size() === 0) {
                    endDrag();
                }
            }, 1);
        }

        //noinspection JSUnusedLocalSymbols
        function handleActivation(evt, name) {
            if (name === 'import') {
                $(document).on('drop', handleDrop);
                $(document).on('paste', handlePaste);
                $(document).on('dragenter', handleDragEnter);
                $(document).on('dragover', nopropagation);
                $(document).on('dragleave', handleDragLeave);
            } else {
                $(document).unbind('drop', handleDrop);
                $(document).unbind('paste', handlePaste);
                $(document).unbind('dragenter', handleDragEnter);
                $(document).unbind('dragover', nopropagation);
                $(document).unbind('dragleave', handleDragLeave);
            }
        }

        function init() {
            bus.on(qdb.MSG_ACTIVE_PANEL, handleActivation);

            const input = $('#js-browse-files-input')[0];

            $('#js-browse-files').click(function () {
                input.click();
            });

            input.onchange = function () {
                $(document).trigger('dropbox.files', input);
                // reset to be able to browse same file again
                input.value = '';
            };
        }

        init();

        return this;
    };
}(jQuery));
