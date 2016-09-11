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

    $.fn.importManager = function () {
        var dict = {};
        var container = this;
        var canvas;
        var top = 0;
        var uploadQueue = [];
        var current = null;
        var rowHeight = 35;
        var xhr = null;

        function updateProgress(event) {
            if (event.lengthComputable) {
                var pos = event.loaded || event.position;
                $('#' + current.id).find(' > .ud-progress').css('width', (pos * 100 / current.size) + '%');
            }
        }

        function updateButtons() {
            var selected = false;
            var retry = false;

            for (var id in dict) {
                if (dict.hasOwnProperty(id)) {
                    var e = dict[id];
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

        //noinspection JSUnusedLocalSymbols
        function renderRowAsOverwrite(x, e) {
            e.retry = 2; // overwrite
            $('#' + e.id + ' > .ud-c1').html(e.name + '<span class="label label-danger m-l-lg">overwrite</span>');
            updateButtons();
        }

        //noinspection JSUnusedLocalSymbols
        function renderRowAsAppend(x, e) {
            e.retry = 1; // append
            $('#' + e.id + ' > .ud-c1').html(e.name + '<span class="label label-primary m-l-lg">append</span>');
            updateButtons();
        }

        //noinspection JSUnusedLocalSymbols
        function renderRowAsCancel(x, e) {
            e.retry = 0; // cancel
            $('#' + e.id + ' > .ud-c1').html(e.name);
            updateButtons();
        }

        function setupUploadProgressCallback() {
            var xhrobj = $.ajaxSettings.xhr();
            if (xhrobj.upload) {
                xhrobj.upload.addEventListener('progress', updateProgress, false);
            }
            return xhrobj;
        }

        var importRequest = {
            xhr: setupUploadProgressCallback,
            url: '/imp?fmt=json',
            type: 'POST',
            contentType: false,
            processData: false,
            cache: false
        };

        var existenceCheckRequest = {
            type: 'GET',
            contentType: false,
            processData: false,
            cache: false
        };

        function updateBtnImportCancel() {
            $('#btnImportCancel').attr('disabled', current === null);
        }

        function toggleRow() {
            var id = $(this).parent().attr('id');
            var btn = $('#' + id).find('.fa');
            var e = dict[id];

            e.selected = !e.selected;

            if (e.selected) {
                btn.removeClass('fa-square-o').addClass('fa-check-square-o');
            } else {
                btn.removeClass('fa-check-square-o').addClass('fa-square-o');
            }

            updateButtons();
        }

        function showDetail(e) {
            var item = dict[$(this).parent().attr('id')];
            if (item.importState > -1) {
                $(document).trigger('import.detail', item);
            }
            nopropagation(e);
        }

        function render(e) {
            canvas.append('<div id="' + e.id + '" class="ud-row" style="top: ' + top + 'px;"><div class="ud-cell ud-c0"><i class="fa fa-square-o ud-checkbox"></i></div><div class="ud-cell ud-c1">' + e.name + '</div><div class="ud-cell ud-c2">' + e.sizeFmt + '</div><div class="ud-cell ud-c3"><span class="label">pending</span></div></div>');
            var row = $('#' + e.id);
            row.find('.ud-c0').click(toggleRow);
            row.find('.ud-c1').click(showDetail);
            row.find('.ud-c2').click(showDetail);
            row.find('.ud-c3').click(showDetail);
            top += rowHeight;
        }

        function status(e, html, processNext) {
            var row = $('#' + e.id);
            row.find(' > .ud-c3').html(html);
            row.find(' > .ud-progress').remove();

            if (processNext) {
                var next = uploadQueue.shift();
                if (next) {
                    retryOrCheckExistence(next);
                } else {
                    current = null;
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
                var type = data.rowsRejected > 0 ? 'label-warning' : 'label-success';
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
            if (current.retry === 2) {
                importRequest.url += '&o=true';
            }
            importRequest.xhr = setupUploadProgressCallback;
            importRequest.data = new FormData();

            // encode type overrides
            if (current.response && current.response.columns) {
                var schema = '';
                for (var i = 0; i < current.response.columns.length; i++) {
                    var c = current.response.columns[i];
                    if (c.altType && c.type !== c.altType.text && c.altType.text !== 'AUTO') {
                        schema += c.name + '=' + c.altType.value + '&';
                    } else if (c.errors === 0 && c.type !== 'DATE' && (c.altType === undefined || c.altType.text !== 'AUTO')) {
                        schema += c.name + '=' + c.type + '&';
                    }
                }
                importRequest.data.append('schema', schema);
            }

            if (current.type === 'file') {
                importRequest.data.append('data', current.file);
            } else if (current.type === 'clipboard') {
                importRequest.url = importRequest.url + '&n=' + encodeURIComponent(current.name);
                importRequest.data.append('data', current.content);
            }
            current.time = new Date().getTime();
            return importRequest;
        }

        function sendImportRequest() {
            status(current, '<span class="label label-info">importing</span>', false);
            $('#' + current.id).append('<div class="ud-progress"></div>');
            xhr = $.ajax(setupImportRequest()).done(importDone).fail(importFailed);
            updateBtnImportCancel();
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
            if (e.retry) {
                current.importState = 0;
                sendImportRequest();
            } else {
                existenceCheckRequest.url = '/chk?f=json&j=' + encodeURIComponent(e.name);
                $.ajax(existenceCheckRequest).then(existenceCheckFork).fail(importFailed);
            }
        }

        function enqueueImportItem(item) {
            dict[item.id] = item;
            render(item);
            if (current != null) {
                uploadQueue.push(item);
            } else {
                retryOrCheckExistence(item);
            }
        }

        //noinspection JSUnusedLocalSymbols
        function addFiles(x, e) {
            for (var i = 0; i < e.files.length; i++) {
                var f = e.files[i];
                enqueueImportItem({
                    id: guid(),
                    name: f.name,
                    size: f.size,
                    file: f,
                    type: 'file',
                    sizeFmt: toSize(f.size),
                    selected: false,
                    imported: false
                });
            }
        }

        //noinspection JSUnusedLocalSymbols
        function addClipboard(x, content) {
            enqueueImportItem({
                id: guid(),
                name: 'clipboard-' + (new Date()),
                size: content.length,
                type: 'clipboard',
                content: content,
                sizeFmt: toSize(content.length),
                selected: false,
                imported: false
            });
        }

        function clearSelected() {
            for (var id in dict) {
                if (dict.hasOwnProperty(id)) {
                    var e = dict[id];
                    if (e.selected && e !== current) {
                        var uploadQueueIndex = uploadQueue.indexOf(e);
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
            var rows = canvas.find('.ud-row');
            for (var i = 0; i < rows.length; i++) {
                $(rows[i]).css('top', top);
                top += rowHeight;
            }
            updateButtons();
        }

        function retrySelected() {
            for (var id in dict) {
                if (dict.hasOwnProperty(id)) {
                    var e = dict[id];
                    if (e.selected && e.retry) {
                        if (current === null) {
                            retryOrCheckExistence(e);
                        } else {
                            uploadQueue.push(e);
                        }
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
            $(document).on('import.clearSelected', clearSelected);
            $(document).on('import.cancel', abortImport);
            $(document).on('import.retry', retrySelected);

            $(document).on('import.line.overwrite', renderRowAsOverwrite);
            $(document).on('import.line.append', renderRowAsAppend);
            $(document).on('import.line.abort', renderRowAsCancel);
        }

        function init() {
            container.append('<div class="ud-header-row"><div class="ud-header ud-h0">&nbsp;</div><div class="ud-header ud-h1">File name</div><div class="ud-header ud-h2">Size</div><div class="ud-header ud-h3">Status</div></div>');
            container.append('<div class="ud-canvas"></div>');
            canvas = container.find('> .ud-canvas');
            subscribe();
        }

        init();

        return this;
    };

    // this class will manage drag&drop into dropbox element and
    // broadcast file readiness to document via custom event 'dropbox.files'
    $.fn.dropbox = function () {

        var collection = $();
        var target = this;

        function startDrag() {
            target.addClass('drag-drop').removeClass('drag-idle');
        }

        function endDrag() {
            target.removeClass('drag-drop').addClass('drag-idle');
        }

        function handleDrop(evt) {
            endDrag();
            collection = $();
            $(document).trigger('dropbox.files', evt.originalEvent.dataTransfer);
        }

        function handlePaste(evt) {
            var pastedText;
            if (window.clipboardData && window.clipboardData.getData) { // IE
                pastedText = window.clipboardData.getData('Text');
            } else if (evt.originalEvent.clipboardData && evt.originalEvent.clipboardData.getData) {
                pastedText = evt.originalEvent.clipboardData.getData('text/plain');
            }
            $(document).trigger('dropbox.clipboard', pastedText);
        }

        function handleDragEnter(event) {
            if (collection.size() === 0) {
                nopropagation(event);
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
                $(document).on('dragleave', handleDragLeave);
            } else {
                $(document).unbind('drop', handleDrop);
                $(document).unbind('paste', handlePaste);
                $(document).unbind('dragenter', handleDragEnter);
                $(document).unbind('dragleave', handleDragLeave);
            }
        }

        function init() {
            $(document).on('active.panel', handleActivation);

            var input = $('#js-browse-files-input');

            $('#js-browse-files').click(function () {
                input.click();
            });

            input[0].onchange = function () {
                $(document).trigger('dropbox.files', input[0]);
            };
        }

        init();

        return this;
    };
}(jQuery));

$(document).ready(function () {
    'use strict';

    $('#btnImportClearSelected').click(function () {
        $(document).trigger('import.clearSelected');
    });

    $('#btnImportCancel').click(function () {
        $(document).trigger('import.cancel');
    });

    $('#btnRetry').click(function () {
        $(document).trigger('import.retry');
    });

    $('#dragTarget').dropbox();
    $('#import-file-list').importManager();

    //
    // prevent dropping files into rest of document
    //
    $(document).on('dragenter', nopropagation);
    $(document).on('dragover', nopropagation);
    $(document).on('drop', nopropagation);

    $(document).ready(function () {
        $('input').iCheck({
            checkboxClass: 'icheckbox_square-red',
            radioClass: 'iradio_square-red'
        });
    });
});
