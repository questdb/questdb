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

(function ($) {
    'use strict';
    $.fn.splitter = function (msgBus, pName, pMinTop, pMinBottom) {
        const bus = $(msgBus);
        const div = $(this);
        const busMsgName = 'splitter.' + pName + '.resize';
        let ghost;
        let start;
        let end;
        let styleMain;
        const minTop = pMinTop;
        const minBottom = pMinBottom;

        function drag(e) {
            e.preventDefault();
            if (e.pageY > minTop && e.pageY < ((window.innerHeight + $(window).scrollTop()) - minBottom)) {
                end = e.pageY;
                ghost[0].style = styleMain + 'top: ' + e.pageY + 'px;';
            }
        }

        function endDrag() {
            $(document).off('mousemove', drag);
            $(document).off('mouseup', endDrag);
            ghost[0].style = 'display: none';
            div.removeClass('qs-dragging');
            bus.trigger(busMsgName, (end - start));
        }

        function beginDrag() {
            const rect = div[0].getBoundingClientRect();
            start = rect.top + $(window).scrollTop();
            styleMain = 'position: absolute; left: ' + rect.left + 'px; width: ' + rect.width + 'px; height: ' + rect.height + 'px;';
            if (!ghost) {
                ghost = $('<div class="qs-ghost"></div>');
                ghost.appendTo('body');
            }
            ghost[0].style = styleMain + 'top: ' + start + 'px;';
            div.addClass('qs-dragging');
            $(document).mousemove(drag);
            $(document).mouseup(endDrag);
        }

        $(this).mousedown(beginDrag);
    };
}(jQuery));
