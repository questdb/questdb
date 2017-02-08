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

/*globals $:false */
/*globals qdb:false */
/*globals jQuery:false */


(function ($) {
    'use strict';

    let messageBus;
    const menuItems = $('#side-menu').find('a');

    function switchTo(name, index) {
        messageBus.trigger(qdb.MSG_ACTIVE_PANEL, name);
        const n = menuItems.length;
        for (let i = 0; i < n; i++) {
            if (i === index) {
                menuItems[i].setAttribute('class', 'selected');
            } else {
                menuItems[i].setAttribute('class', '');
            }
        }
    }

    function switchToConsole() {
        switchTo('console', 0);
    }

    function switchToVis() {
        switchTo('visualisation', 1);
    }

    function switchToImport() {
        switchTo('import', 2);
    }

    function setup(b) {
        messageBus = b;
        $('#side-menu').metisMenu();
        $('a#nav-console').click(switchToConsole);
        $('a#nav-import').click(switchToImport);
        $('a#nav-visualisation').click(switchToVis);
        b.on(qdb.MSG_QUERY_FIND_N_EXEC, switchToConsole);
    }

    $.extend(true, window, {
        qdb: {
            setup,
            switchToConsole
        }
    });

}(jQuery));

let bus;

$(document).ready(function () {
    'use strict';
    bus = $({});

    qdb.setup(bus);
    qdb.setupConsoleController(bus);
    qdb.setupImportController(bus);
    qdb.setupVisualisationController(bus);
    qdb.switchToConsole();
    bus.trigger('preferences.load');
});

$(window).load(function () {
    'use strict';
    $(window).trigger('resize');
    bus.trigger('editor.focus');
});
