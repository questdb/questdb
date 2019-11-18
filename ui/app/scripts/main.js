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
