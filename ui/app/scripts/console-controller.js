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
/*globals Clipboard:false */

(function ($) {
    'use strict';

    const divSqlPanel = $('.js-sql-panel');
    const divExportUrl = $('.js-export-url');
    const editor = $('#editor');
    const sqlEditor = $('#sqlEditor');
    const consoleTop = $('#console-top');
    const wrapper = $('#page-wrapper');
    const msgPanel = editor.find('.js-query-message-panel');
    const navbar = $('nav.navbar-default');
    const win = $(window);
    const grid = $('#grid');
    const quickVis = $('#quick-vis');
    const toggleChartBtn = $('#js-toggle-chart');
    const toggleGridBtn = $('#js-toggle-grid');

    let topHeight = 350;
    const bottomHeight = 350;
    let visible = false;

    function resize() {
        if (visible) {
            const navbarHeight = navbar.height();
            const wrapperHeight = wrapper.height();
            const msgPanelHeight = msgPanel.height();
            let h;

            if (navbarHeight > wrapperHeight) {
                h = navbarHeight;
            }

            if (navbarHeight < wrapperHeight) {
                h = win.height();
            }

            if (h) {
                if (h < topHeight + bottomHeight) {
                    h = topHeight + bottomHeight;
                }
                // qdb.setHeight(wrapper, h - 1);
            }

            qdb.setHeight(consoleTop, topHeight);
            qdb.setHeight(editor, topHeight);
            qdb.setHeight(sqlEditor, topHeight - msgPanelHeight - 60);
        }
    }

    function loadSplitterPosition() {
        if (typeof (Storage) !== 'undefined') {
            const n = localStorage.getItem('splitter.position');
            if (n) {
                topHeight = parseInt(n);
                if (!topHeight) {
                    topHeight = 350;
                }
            }
        }
    }

    function saveSplitterPosition() {
        if (typeof (Storage) !== 'undefined') {
            localStorage.setItem('splitter.position', topHeight);
        }
    }

    function toggleVisibility(x, name) {
        if (name === 'console') {
            visible = true;
            divSqlPanel.show();
        } else {
            visible = false;
            divSqlPanel.hide();
        }
    }

    function toggleChart() {
        toggleChartBtn.addClass('active');
        toggleGridBtn.removeClass('active');
        grid.hide();
        quickVis.show();
        quickVis.resize();
    }

    function toggleGrid() {
        toggleChartBtn.removeClass('active');
        toggleGridBtn.addClass('active');
        grid.show();
        quickVis.hide();
        grid.resize();
    }

    function setup(bus) {
        win.bind('resize', resize);
        bus.on(qdb.MSG_QUERY_DATASET, function (e, m) {
            divExportUrl.val(qdb.toExportUrl(m.query));
        });

        divExportUrl.click(function () {
            this.select();
        });

        /* eslint-disable no-new */
        new Clipboard('.js-export-copy-url');
        $('.js-query-refresh').click(function () {
            bus.trigger('grid.refresh');
        });

        // named splitter
        bus.on('splitter.console.resize', function (x, e) {
            topHeight += e;
            win.trigger('resize');
            bus.trigger('preferences.save');
        });

        bus.on('preferences.save', saveSplitterPosition);
        bus.on('preferences.load', loadSplitterPosition);
        bus.on(qdb.MSG_ACTIVE_PANEL, toggleVisibility);

        bus.query();
        bus.domController();

        sqlEditor.editor(bus);
        grid.grid(bus);
        quickVis.quickVis(bus);

        $('#console-splitter').splitter(bus, 'console', 200, 0);

        // wire query publish
        toggleChartBtn.click(toggleChart);
        toggleGridBtn.click(toggleGrid);
        bus.on(qdb.MSG_QUERY_DATASET, toggleGrid);
    }

    $.extend(true, window, {
        qdb: {
            setupConsoleController: setup
        }
    });
}(jQuery));
