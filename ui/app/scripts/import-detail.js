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

(function ($) {
    'use strict';

    $.fn.importEditor = function (ebus) {
        const container = $(this);
        const statsSwitcher = $('.stats-switcher');
        const divEditor = $(this).find('.js-import-editor');
        const msgPanel = $(this).find('.js-import-error');
        const divMessage = $(this).find('.js-message');
        const divTabName = $(this).find('.js-import-tab-name');
        const divRejectedPct = $(this).find('.import-rejected');
        const divImportedPct = $(this).find('.import-imported');
        const divRejectedCount = $(this).find('.js-rejected-row-count');
        const divImportedCount = $(this).find('.js-imported-row-count');
        const divCanvas = $(this).find('.ud-canvas');
        const footerHeight = $('.footer')[0].offsetHeight;
        const divBtnGroup = $(this).find('.js-import-error-btn-group');
        const btnRadio = $('input:radio[name="importAction"]');
        const lineHeight = 35;
        let select;
        let location;
        const types = [
            {text: 'AUTO', value: null},
            {text: 'BOOLEAN', value: 'BOOLEAN'},
            {text: 'BYTE', value: 'BYTE'},
            {text: 'DOUBLE', value: 'DOUBLE'},
            {text: 'FLOAT', value: 'FLOAT'},
            {text: 'INT', value: 'INT'},
            {text: 'LONG', value: 'LONG'},
            {text: 'SHORT', value: 'SHORT'},
            {text: 'STRING', value: 'STRING'},
            {text: 'SYMBOL', value: 'SYMBOL'},
            {text: 'DATE (ISO)', value: 'DATE_ISO'},
            {text: 'DATE (YYYY-MM-DD hh:mm:ss)', value: 'DATE_1'},
            {text: 'DATE (MM/DD/YYYY)', value: 'DATE_2'},
            {text: 'DATE (DD/MM/YYYY)', value: 'DATE_3'}
        ];

        let current = null;
        const editorBus = ebus;

        function resizeCanvas() {
            const top = divCanvas[0].getBoundingClientRect().top;
            let h = Math.round((window.innerHeight - top));
            h = h - footerHeight - 45;
            divCanvas[0].style.height = h + 'px';
        }

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
                select.val(col.altType.value);
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
            if (col.altType && col.altType.text !== col.type) {
                return col.type + '<i class="fa fa-angle-double-right g-type-separator"></i>' + col.altType.text;
            } else {
                return col.type;
            }
        }

        function calcModifiedFlag() {
            var modified = false;
            for (var i = 0; i < current.response.columns.length; i++) {
                var col = current.response.columns[i];
                if (col.altType && col.type !== col.altType.text) {
                    modified = true;
                    break;
                }
            }

            $(document).trigger(modified ? 'import.line.overwrite' : 'import.line.cancel', current);
        }

        function selectChange() {
            var sel = $(this).find('option:selected');
            select.changeTargetCol.altType = {text: sel.text(), value: sel.val()};
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
            if (e.importState === 0 && !e.response) {
                // aborted at start
                return;
            }

            if (e.response && e.importState === 0) {
                divTabName.html(location = e.response.location);

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
                resizeCanvas();
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
                        divMessage.html(e.response);
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
                // reset button group option
                btnRadio.iCheck('uncheck');
            }
            container.show();
        }

        function setupSelect() {
            select = $('<select class="g-dynamic-select form-control m-b"/>');
            for (var i = 0; i < types.length; i++) {
                var val = types[i];
                $('<option />', {value: val.value, text: val.text}).appendTo(select);
            }
        }

        $(document).on('import.detail', function (x, e) {
            current = e;
            render(e);
        });

        $(document).on('import.detail.updated', function (x, e) {
            if (current === e && e.response) {
                render(e);
            }
        });

        $(document).on('import.cleared', function (x, e) {
            if (e === current) {
                current = null;
                divEditor.hide();
                msgPanel.hide();
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

        btnRadio.on('ifClicked', function () {
            var msg;
            switch (this.value) {
                case 'append':
                    msg = 'import.line.append';
                    break;
                case 'overwrite':
                    msg = 'import.line.overwrite';
                    break;
                default:
                    msg = 'import.line.abort';
                    break;
            }
            $(document).trigger(msg, current);
        });

        divTabName.mouseover(function () {
            divTabName.addClass('animated tada').one('webkitAnimationEnd mozAnimationEnd MSAnimationEnd oanimationend animationend', function () {
                $(this).removeClass('animated').removeClass('tada');
            });
        });

        divTabName.click(function () {
            editorBus.trigger('query.build.execute', location);
        });

        $('input').iCheck({
            checkboxClass: 'icheckbox_square-red',
            radioClass: 'iradio_square-red'
        });

        $(window).resize(resizeCanvas);
    };
}(jQuery));
