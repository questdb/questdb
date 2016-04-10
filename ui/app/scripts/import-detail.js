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


(function ($) {
    'use strict';

    $.fn.importEditor = function () {
        var container = $(this);
        var statsSwitcher = $('.stats-switcher');
        var divEditor = $(this).find('.js-import-editor');
        var msgPanel = $(this).find('.js-import-error');
        var divMessage = $(this).find('.js-message');
        var divTabName = $(this).find('.js-import-tab-name');
        var divRejectedPct = $(this).find('.import-rejected');
        var divImportedPct = $(this).find('.import-imported');
        var divRejectedCount = $(this).find('.js-rejected-row-count');
        var divImportedCount = $(this).find('.js-imported-row-count');
        var divCanvas = $(this).find('.ud-canvas');
        var divBtnGroup = $(this).find('.js-import-error-btn-group');
        var lineHeight = 35;
        var select;
        var types = [
            'BOOLEAN',
            'BYTE',
            'DOUBLE',
            'FLOAT',
            'INT',
            'LONG',
            'SHORT',
            'STRING',
            'SYMBOL',
            'DATE'
        ];

        var current = null;

        function selectClick() {
            var div = $(this);
            select.appendTo(div.parent());
            select.css('left', div.css('left'));
            select.css('width', div.css('width'));

            // find column index
            var colIndex = parseInt($(this).parent().find('.js-g-row').text()) - 1;

            // get column
            var col = current.response.columns[colIndex];

            // set option
            if (col.altType) {
                select.val(col.altType);
            } else {
                select.val(col.type);
            }

            select.changeTargetDiv = div;
            select.changeTargetCol = col;

            select.show();
            select.focus();
        }

        function selectHide() {
            select.hide();
        }

        function getTypeHtml(col) {
            if (col.altType && col.altType !== col.type) {
                return col.type + '<i class="fa fa-angle-double-right g-type-separator"></i>' + col.altType;
            } else {
                return col.type;
            }
        }

        function calcModifiedFlag() {
            var modified = false;
            for (var i = 0; i < current.response.columns.length; i++) {
                var col = current.response.columns[i];
                if (col.altType && col.type !== col.altType) {
                    modified = true;
                    break;
                }
            }

            $(document).trigger(modified ? 'import.line.overwrite' : 'import.line.cancel', current.id);
        }

        function selectChange() {
            select.changeTargetCol.altType = $(this).find('option:selected').text();
            select.changeTargetDiv.html(getTypeHtml(select.changeTargetCol));
            calcModifiedFlag();
            selectHide();
        }

        function attachSelect() {
            $('.g-type').click(selectClick);
            $('.g-other').click(selectHide);
            select.change(selectChange);
        }

        function render(e) {
            if (e.response && e.importState === 0) {
                divTabName.html(e.response.location);

                // update "chart"
                var importedRows = e.response.rowsImported;
                var rejectedRows = e.response.rowsRejected;
                var totalRows = importedRows + rejectedRows;
                divRejectedPct.css('width', Math.round(rejectedRows * 100 / totalRows) + '%');
                divImportedPct.css('width', Math.round(importedRows * 100 / totalRows) + '%');

                // update counts
                divRejectedCount.html(rejectedRows);
                divImportedCount.html(importedRows);

                divCanvas.empty();

                // records
                if (e.response.columns) {
                    var top = 0;
                    for (var k = 0; k < e.response.columns.length; k++) {
                        var col = e.response.columns[k];
                        divCanvas.append('<div class="ud-row" style="top: ' + top + 'px">' +
                            '<div class="ud-cell gc-1 g-other js-g-row">' + (k + 1) + '</div>' +
                            '<div class="ud-cell gc-2 g-other">' + (col.errors > 0 ? '<i class="fa fa-exclamation-triangle g-warning"></i>' : '') + col.name + '</div>' +
                            '<div class="ud-cell gc-3 g-type">' + getTypeHtml(col) + '</div>' +
                            '<div class="ud-cell gc-4 g-other">' + col.errors + '</div>' +
                            '</div>');

                        top += lineHeight;
                    }
                }

                attachSelect();

                // display component
                divEditor.show();
                msgPanel.hide();
            } else {
                switch (e.importState) {
                    case 1:
                        divMessage.html('Journal <strong>' + e.name + '</strong> already exists on server');
                        divBtnGroup.show();
                        break;
                    case 2:
                        divMessage.html('Journal name <strong>' + e.name + '</strong> is reserved');
                        divBtnGroup.hide();
                        break;
                    case 3:
                        divMessage.html('Server is not responding...');
                        divBtnGroup.hide();
                        break;
                    case 4:
                        divMessage.html('Server rejected file due to unsupported file format.');
                        divBtnGroup.hide();
                        break;
                    case 5:
                        divMessage.html('Server encountered internal problem. Check server logs for more details.');
                        divBtnGroup.hide();
                        break;
                    default:
                        divMessage.html('Unknown error: ' + e.responseStatus);
                        divBtnGroup.hide();
                        break;
                }
                divEditor.hide();
                msgPanel.show();
            }
            container.show();
        }

        function setupSelect() {
            select = $('<select class="g-dynamic-select form-control m-b"/>');
            for (var i = 0; i < types.length; i++) {
                var val = types[i];
                $('<option />', {value: val, text: val}).appendTo(select);
            }
        }

        $(document).on('import.detail', function (x, e) {
            current = e;
            render(e);
        });

        $(document).on('import.cleared', function (x, e) {
            if (e === current) {
                current = null;
                divEditor.hide();
            }
        });

        setupSelect();

        $('.import-stats-chart').click(function () {
            if (statsSwitcher.hasClass('stats-visible')) {
                statsSwitcher.removeClass('stats-visible');
            } else {
                statsSwitcher.addClass('stats-visible');
            }
        });

        $('input:radio[name="importAction"]').on('ifClicked', function () {

            var msg;
            switch (this.value) {
                case 'append':
                    msg = 'import.line.append';
                    break;
                case 'overwrite':
                    msg = 'import.line.overwrite';
                    break;
                default:
                    msg = 'import.line.cancel';
                    break;
            }
            $(document).trigger(msg, current.id);
        });
    };
}(jQuery));

$(document).ready(function () {
    'use strict';
    $('#import-detail').importEditor();
});
