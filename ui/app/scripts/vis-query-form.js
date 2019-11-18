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

(function ($) {
        'use strict';
        $.fn.queryForm = function () {
            const div = $(this);
            const fQueryName = div.find('#_vis_frm_query_name')[0];
            const gQueryName = div.find('.qdb-vis-query-name');
            const gQueryNameHelp = gQueryName.find('.help-block');
            const fTitle = div.find('.js-vis-title');
            const fQueryText = qdb.createEditor(div.find('#_vis_frm_query_text')[0]);
            const queryRequestParams = {
                query: '',
                limit: '0,0',
                count: false
            };

            let last;

            function newQuery(index) {
                return {
                    id: '_li_query_' + index,
                    name: 'query' + index
                };
            }

            function copyToForm(query) {

                last = query;

                fQueryName.value = query.name;
                if (query.text) {
                    fQueryText.setValue(query.text, -1);
                } else {
                    fQueryText.setValue('', -1);
                }

                if (query.name === '') {
                    fTitle.html('&lt;no name&gt;');
                } else {
                    fTitle.html(query.name);
                }
                fQueryName.focus();
            }

            function storeColumns(response, status, jqXHR) {
                jqXHR.query.columns = response.columns;
                jqXHR.query.error = null;
            }

            function clearColumnsAndReportQueryError(jqXHR) {
                jqXHR.query.columns = null;
                jqXHR.query.error = jqXHR.responseJSON;
            }

            function fetchQueryColumns(query) {
                queryRequestParams.query = query.textNormalized;
                const request = $.get('/exec', queryRequestParams);
                request.query = query;
                request.done(storeColumns).fail(clearColumnsAndReportQueryError);
            }

            function normalizeQuery(text) {
                if (!text || text.length === 0) {
                    return null;
                }

                const q = text.trim();
                const n = q.length;
                if (n === 0) {
                    return null;
                }

                if (q.charAt(n - 1) === ';') {
                    return q.substr(0, n - 1);
                } else {
                    return q;
                }
            }

            function copyToMem(query) {
                let error = false;

                if (fQueryName.value === '') {
                    gQueryName.addClass('has-error');
                    gQueryNameHelp.html('Please fill this field');
                    error = true;
                }

                if (error) {
                    return false;
                }

                gQueryNameHelp.html('');
                gQueryName.removeClass('has-error');

                let changed = false;

                if (query.name !== fQueryName.value) {
                    query.name = fQueryName.value;
                    changed = true;
                }

                const q = fQueryText.getValue();
                if (query.text !== q) {
                    changed = true;
                    query.text = q;
                    const nq = normalizeQuery(q);
                    if (query.textNormalized !== nq) {
                        query.textNormalized = nq;
                        if (query.textNormalized) {
                            fetchQueryColumns(query);
                        }
                    }
                }

                if (changed) {
                    query.timestamp = new Date().getTime();
                }

                if (query.callback) {
                    query.callback();
                }
                return true;
            }

            function copyToLast() {
                if (last) {
                    copyToMem(last);
                }
            }

            function clear() {
                fQueryName.value = '';
                fQueryText.value = '';
                last = null;
            }

            fQueryName.onfocusout = copyToLast;
            fQueryText.on('blur', copyToLast);

            return div.listManager(newQuery, copyToForm, copyToMem, clear);
        };
    }(jQuery)
);
