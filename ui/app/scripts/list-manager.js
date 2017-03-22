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

            let usrUpdateCallaback;
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

            function callUsrUpdateCallback(item) {
                if (usrUpdateCallaback) {
                    usrUpdateCallaback(item);
                }
            }

            function updateCallback() {
                const html = ulList.find('#' + this.id);
                html.html(this.name);
                callUsrUpdateCallback(this);
            }

            function addItem0(item) {
                item.callback = updateCallback;
                const id = item.id;
                const name = item.name;
                const html = $(`<li id="${id}">${name}</li>`);
                map[id] = item;
                html.click(switchTo);
                html.appendTo(ulList);
                return html;
            }

            function addItem() {
                let next = ulList.find('li').length + 1;
                let item;

                do {
                    item = createCallback(next++);
                } while (ulList.find('#' + item.id).length > 0);

                item.timestamp = new Date().getTime();
                const html = addItem0(item);
                updateVisibility();
                html.click();

                callUsrUpdateCallback(item);
            }

            function deleteItem() {
                if (activeItem) {
                    const html = activeLi();
                    delete map[activeItem];
                    callUsrUpdateCallback();
                    if (html) {
                        clearCallback();
                        let next = html.next();
                        if (next.length === 0) {
                            next = html.prev();
                        }
                        html.remove();
                        if (next.length === 0) {
                            activeItem = null;
                        } else {
                            next.click();
                        }
                    }
                }

                updateVisibility();
            }

            function getMap() {
                return map;
            }

            function onUpdate(callback) {
                usrUpdateCallaback = callback;
            }

            function setMap(items) {
                map.splice(0, map.length);
                let first;

                for (let i = 0, n = items.length; i < n; i++) {
                    const html = addItem0(items[i]);
                    if (!first) {
                        first = html;
                    }
                }

                if (first) {
                    first.click();
                }

                updateVisibility();
            }

            btnAdd.click(addItem);
            btnDelete.click(deleteItem);

            updateVisibility();

            return {
                getMap,
                setMap,
                onUpdate
            };
        };
    }(jQuery)
);
