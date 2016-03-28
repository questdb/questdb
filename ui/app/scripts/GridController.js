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

/*globals jQuery:false */

(function ($) {
    'use strict';

    function GridController() {
        var query;
        var hActiveRequest = null;
        var hPendingRequest = null;

        var requestParams = {
            'query': '',
            'limit': '',
            'withCount': false
        };

        function init() {

        }

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

        function handleServerResponse() {
            console.log('success');
        }

        function handleServerError(jqXHR, textStatus) {
            console.log('not so lucky: ' + textStatus);
        }

        function qq() {
            abortActive();
            requestParams.query = query;
            requestParams.limit = '0,100';
            requestParams.withCount = true;
            hActiveRequest = $.get('/js', requestParams, handleServerResponse, handleServerError);
        }

        function sendQuery(q) {
            query = q;
            abortPending();
            setTimeout(qq, 50);
        }

        init();

        return {
            'sendQuery': sendQuery
        };
    }

    $.extend(true, window, {
        nfsdb: {
            GridController: GridController
        }
    });

}(jQuery));
