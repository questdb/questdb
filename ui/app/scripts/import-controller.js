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

/*globals qdb:false */
/*globals jQuery:false */


(function ($) {
    'use strict';

    const divImportPanel = $('.js-import-panel');
    const importTopPanel = $('#import-top');
    const importDetail = $('#import-detail');
    const importMenu = $('#import-menu')[0];

    const footer = $('.footer')[0];
    const canvasPanel = importTopPanel.find('.ud-canvas');
    const w = $(window);
    let visible = false;

    let upperHalfHeight = 450;

    function resize() {
        if (visible) {
            const h = w[0].innerHeight;
            const footerHeight = footer.offsetHeight;
            qdb.setHeight(importTopPanel, upperHalfHeight);
            qdb.setHeight(importDetail, h - footerHeight - upperHalfHeight - importMenu.offsetHeight - 10);

            let r1 = importTopPanel[0].getBoundingClientRect();
            let r2 = canvasPanel[0].getBoundingClientRect();
            // qdb.setHeight(importTopPanel, upperHalfHeight);
            qdb.setHeight(canvasPanel, upperHalfHeight - (r2.top - r1.top) - 10);
        }
    }

    function toggleVisibility(x, name) {
        if (name === 'import') {
            visible = true;
            divImportPanel.show();
            w.trigger('resize');
        } else {
            visible = false;
            divImportPanel.hide();
        }
    }

    function splitterResize(x, p) {
        upperHalfHeight += p;
        w.trigger('resize');
    }

    function setup(bus) {
        w.bind('resize', resize);

        $('#dragTarget').dropbox(bus);
        $('#import-file-list').importManager(bus);
        $('#import-detail').importEditor(bus);
        $('#import-splitter').splitter(bus, 'import', 470, 300);

        bus.on('splitter.import.resize', splitterResize);
        bus.on(qdb.MSG_ACTIVE_PANEL, toggleVisibility);
    }

    $.extend(true, window, {
        qdb: {
            setupImportController: setup
        }
    });
}(jQuery));
