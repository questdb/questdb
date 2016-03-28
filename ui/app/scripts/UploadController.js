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

$(document).ready(function () {
    'use strict';

    var tab = $('#uploadTable');
    var target = $('#dragTarget');
    var collection = $();

    function nopropagation(e) {
        e.stopPropagation();
        if (e.preventDefault) {
            e.preventDefault();
        }
    }

    function addFilesToTable(files) {
        for (var i = 0; i < files.length; i++) {
            var f = files[i];
            tab.find('> tbody:last-child').append('<tr><td>' + f.name + '</td><td>' + f.size + '</td><td>0</td><td>uploading</td></tr>');
        }
    }

    function dragDrop(e) {
        collection = $();
        target.removeClass('drag-drop');
        target.addClass('drag-idle');
        e.preventDefault();
        addFilesToTable(e.originalEvent.dataTransfer.files);
    }

    target.on('drop', dragDrop);

    // deal with event propagation to child elements
    // http://stackoverflow.com/questions/10867506/dragleave-of-parent-element-fires-when-dragging-over-children-elements

    $.fn.dndhover = function () {

        return this.each(function () {

            var self = $(this);

            self.on('dragenter', function (event) {
                if (collection.size() === 0) {
                    self.trigger('dndHoverStart');
                }
                collection = collection.add(event.target);
            });

            self.on('dragleave', function (event) {
                /*
                 * Firefox 3.6 fires the dragleave event on the previous element
                 * before firing dragenter on the next one so we introduce a delay
                 */
                setTimeout(function () {
                    collection = collection.not(event.target);
                    if (collection.size() === 0) {
                        self.trigger('dndHoverEnd');
                    }
                }, 1);
            });
        });
    };

    target.dndhover().on({
        'dndHoverStart': function (event) {

            target.addClass('drag-drop');
            target.removeClass('drag-idle');

            event.stopPropagation();
            event.preventDefault();
            return false;
        },
        'dndHoverEnd': function (event) {

            target.removeClass('drag-drop');
            target.addClass('drag-idle');

            event.stopPropagation();
            event.preventDefault();
            return false;
        }
    });

    //
    // prevent dropping files into rest of document
    //
    $(document).on('dragenter', nopropagation);
    $(document).on('dragover', nopropagation);
    $(document).on('drop', nopropagation);
});
