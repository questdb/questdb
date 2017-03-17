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
        $.fn.listManager = function (pCreateCallback, pCopyToFormCallback, pCopyToMemCallback, pClearCallback) {
            const div = $(this);
            const ulList = div.find('.qdb-list ul');
            const createCallback = pCreateCallback;
            const copyToFormCallback = pCopyToFormCallback;
            const copyToMemCallback = pCopyToMemCallback;
            const clearCallback = pClearCallback;
            const map = [];
            const btnAdd = div.find('.qdb-list-add-btn');
            const btnDelete = div.find('.qdb-list-delete-btn');
            const divForm = div.find('.qdb-vis-form');
            const divPlaceholder = div.find('.qdb-vis-placeholder');

            let activeItem;

            function updateVisibility() {
                const count = ulList.find('li').length;
                if (count === 0) {
                    divForm.hide();
                    divPlaceholder.show();
                } else {
                    divPlaceholder.hide();
                    divForm.show();
                }
            }

            function activeLi() {
                return ulList.find('#' + activeItem);
            }

            function switchTo(evt) {

                if (activeItem) {
                    const html = activeLi()[0];
                    if (html) {
                        if (!copyToMemCallback(map[activeItem])) {
                            return;
                        }
                        html.className = '';
                    }
                }

                activeItem = evt.target.getAttribute('id');
                evt.target.className = 'active';

                const series = map[activeItem];

                if (series) {
                    copyToFormCallback(series);
                } else {
                    clearCallback();
                }
            }

            function updateCallback() {
                const html = ulList.find('#' + this.id);
                html.html(this.name);
            }

            function addItem() {
                const next = ulList.find('li').length + 1;
                const item = createCallback(next);
                item.callback = updateCallback;
                const id = item.id;
                const name = item.name;
                const html = $(`<li id="${id}">${name}</li>`);
                map[id] = item;
                html.click(switchTo);
                html.appendTo(ulList);
                updateVisibility();
                html.click();
            }

            function deleteItem() {
                if (activeItem) {
                    const html = activeLi();
                    if (html) {
                        clearCallback();
                        let next = html.next();
                        if (next.length === 0) {
                            next = html.prev();
                        }
                        html.remove();
                        next.click();
                    }
                }
                updateVisibility();
            }

            btnAdd.click(addItem);
            btnDelete.click(deleteItem);

            updateVisibility();

            return map;
        };
    }(jQuery)
);
