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

/*globals jQuery:false */

(function ($) {
    'use strict';

    var queryBatchSize = 1000;
    var MSG_QUERY_EXEC = 'query.in.exec';
    var MSG_QUERY_CANCEL = 'query.in.cancel';
    var MSG_QUERY_RUNNING = 'query.out.running';
    var MSG_QUERY_OK = 'query.out.ok';
    var MSG_QUERY_ERROR = 'query.out.error';
    var MSG_QUERY_DATASET = 'query.out.dataset';

    function toExportUrl(query) {
        return window.location.protocol + '//' + window.location.host + '/csv?query=' + encodeURIComponent(query);
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
            toExportUrl
        }
    });
}(jQuery));
