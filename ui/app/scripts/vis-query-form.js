/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * The MIT License (MIT)
 *
 * Copyright (C) 2016-2017 Appsicle
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
                query.name = fQueryName.value;

                const q = fQueryText.getValue();
                if (query.text !== q) {
                    query.text = q;
                    const nq = normalizeQuery(q);
                    if (query.textNormalized !== nq) {
                        query.textNormalized = nq;
                        if (query.textNormalized) {
                            fetchQueryColumns(query);
                        }
                    }
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
            }

            fQueryName.onfocusout = copyToLast;
            fQueryText.on('blur', copyToLast);

            return div.listManager(newQuery, copyToForm, copyToMem, clear);
        };
    }(jQuery)
);
