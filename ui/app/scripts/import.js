/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

function numberWithCommas(x) {
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
        var uploaded = null;
        var rowHeight = 35;

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
        }

        function showDetail(e) {
            var id = $(this).parent().attr('id');
            $('#upload-detail').html(dict[id].response.location);
            nopropagation(e);
        }

        function render(e) {
            canvas.append('<div id="' + e.id + '" class="ud-row" style="top: ' + top + 'px;"><div class="ud-cell ud-c0"><i class="fa fa-square-o"></i></div><div class="ud-cell ud-c1">' + e.name + '</div><div class="ud-cell ud-c2">' + e.sizeFmt + '</div><div class="ud-cell ud-c3"><span class="label">pending</span></div></div>');
            var row = $('#' + e.id);
            row.find('.ud-c0').click(toggleRow);
            row.find('.ud-c1').click(showDetail);
            row.find('.ud-c2').click(showDetail);
            row.find('.ud-c3').click(showDetail);
            top += rowHeight;
        }

        function updateProgress(event) {
            if (event.lengthComputable) {
                var pos = event.loaded || event.position;
                $('#' + uploaded.id).find(' > .ud-progress').css('width', (pos * 100 / uploaded.size) + '%');
            }
        }

        function status(e, html, processNext) {
            var row = $('#' + e.id);
            row.find(' > .ud-c3').html(html);
            row.find(' > .ud-progress').remove();

            if (processNext) {
                var next = uploadQueue.shift();
                if (next) {
                    upload(next);
                } else {
                    uploaded = null;
                }
            }
        }

        function setupUploadProgressCallback() {
            var xhrobj = $.ajaxSettings.xhr();
            if (xhrobj.upload) {
                xhrobj.upload.addEventListener('progress', updateProgress, false);
            }
            return xhrobj;
        }

        function success(data) {
            uploaded.response = data;
            status(uploaded, '<span class="label label-success">success</span>', true);
        }

        function error(xhr) {
            uploaded.response = xhr.responseText;
            status(uploaded, '<span class="label label-danger">failed</span>', true);
        }

        var request = {
            xhr: setupUploadProgressCallback,
            url: '/imp?fmt=json',
            type: 'POST',
            contentType: false,
            processData: false,
            cache: false,
            success: success,
            error: error
        };

        function upload(e) {
            uploaded = e;
            status(e, '<span class="label label-info">importing</span>', false);
            $('#' + e.id).append('<div class="ud-progress"></div>');
            request.data = new FormData();
            request.data.append('data', e.file);
            $.ajax(request);
        }

        container.append('<div class="ud-header-row"><div class="ud-header ud-h0">&nbsp;</div><div class="ud-header ud-h1">File name</div><div class="ud-header ud-h2">Size</div><div class="ud-header ud-h3">Status</div></div>');
        container.append('<div class="ud-canvas"></div>');
        canvas = container.find('> .ud-canvas');

        // subscribe to document event
        $(document).on('dropbox.files', function (x, dataTransfer) {
            for (var i = 0; i < dataTransfer.files.length; i++) {
                var f = dataTransfer.files[i];
                var e = {
                    id: guid(),
                    name: f.name,
                    size: f.size,
                    file: f,
                    sizeFmt: numberWithCommas(f.size),
                    selected: false
                };
                dict[e.id] = e;
                render(e);
                if (uploaded != null) {
                    uploadQueue.push(e);
                } else {
                    upload(e);
                }
            }
        });

        $(document).on('import.clearSelected', function () {
            for (var id in dict) {
                if (dict.hasOwnProperty(id) && dict[id].selected) {
                    $('#' + id).remove();
                    delete dict[id];
                }
            }

            // rejig remaining rows
            top = 0;
            var rows = canvas.find('.ud-row');
            for (var i = 0; i < rows.length; i++) {
                $(rows[i]).css('top', top);
                top += rowHeight;
            }
        });

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

        function init() {
            target.on('drop', function (evt) {
                endDrag();
                collection = $();
                $(document).trigger('dropbox.files', evt.originalEvent.dataTransfer);
            });

            // deal with event propagation to child elements
            // http://stackoverflow.com/questions/10867506/dragleave-of-parent-element-fires-when-dragging-over-children-elements

            target.each(function () {

                var self = $(this);

                self.on('dragenter', function (event) {
                    if (collection.size() === 0) {
                        nopropagation(event);
                        startDrag();
                    }
                    collection = collection.add(event.target);
                });

                self.on('dragleave', function (event) {
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
                });
            });

        }

        init();

        return this;
    };
}(jQuery));

$(document).ready(function () {
    'use strict';

    $('#btnClearSelected').click(function () {
        $(document).trigger('import.clearSelected');
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
});
