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
/*globals ace:false */

(function ($) {
    'use strict';

    const queryBatchSize = 1000;
    const MSG_QUERY_EXPORT = 'query.in.export';
    const MSG_QUERY_EXEC = 'query.in.exec';
    const MSG_QUERY_CANCEL = 'query.in.cancel';
    const MSG_QUERY_RUNNING = 'query.out.running';
    const MSG_QUERY_OK = 'query.out.ok';
    const MSG_QUERY_ERROR = 'query.out.error';
    const MSG_QUERY_DATASET = 'query.out.dataset';
    const MSG_QUERY_FIND_N_EXEC = 'query.build.execute';
    const MSG_ACTIVE_PANEL = 'active.panel';

    const MSG_EDITOR_FOCUS = 'editor.focus';
    const MSG_EDITOR_EXECUTE = 'editor.execute';
    const MSG_EDITOR_EXECUTE_ALT = 'editor.execute.alt';

    const MSG_CHART_DRAW = 'chart.draw';

    function toExportUrl(query) {
        return window.location.protocol + '//' + window.location.host + '/exp?query=' + encodeURIComponent(query);
    }

    function setHeight(element, height) {
        element.css('height', height + 'px');
        element.css('min-height', height + 'px');
    }

    function createEditor(div) {
        const edit = ace.edit(div);
        edit.getSession().setMode('ace/mode/questdb');
        edit.setTheme('ace/theme/merbivore_soft');
        edit.setShowPrintMargin(false);
        edit.setDisplayIndentGuides(false);
        edit.setHighlightActiveLine(false);
        edit.$blockScrolling = Infinity;

        $(window).on('resize', function () {
            edit.resize();
        });

        return edit;
    }

    $.extend(true, window, {
        qdb: {
            queryBatchSize,
            MSG_QUERY_EXPORT,
            MSG_QUERY_EXEC,
            MSG_QUERY_CANCEL,
            MSG_QUERY_RUNNING,
            MSG_QUERY_OK,
            MSG_QUERY_ERROR,
            MSG_QUERY_DATASET,
            MSG_ACTIVE_PANEL,
            MSG_QUERY_FIND_N_EXEC,
            MSG_EDITOR_FOCUS,
            MSG_EDITOR_EXECUTE,
            MSG_EDITOR_EXECUTE_ALT,
            MSG_CHART_DRAW,
            toExportUrl,
            setHeight,
            createEditor
        }
    });
}(jQuery));
