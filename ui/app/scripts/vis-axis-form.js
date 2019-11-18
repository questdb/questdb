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
        $.fn.axisForm = function () {
            const div = $(this);
            const fName = div.find('#_vis_frm_axis_name')[0];
            const fType = div.find('#_vis_frm_axis_type')[0];
            const fValueType = div.find('#_vis_frm_axis_value_type')[0];
            const fColumn = div.find('#_vis_frm_axis_column')[0];
            const fValues = div.find('#_vis_frm_axis_values')[0];
            const fScale = div.find('#_vis_frm_axis_scale')[0];

            let last;

            function newQuery(index) {
                return {
                    id: '_li_axis_' + index,
                    name: 'axis' + index,
                    scale: false
                };
            }

            function copyToForm(axis) {
                last = axis;

                console.log('copyToForm');
                console.log(axis);

                fName.value = axis.name;
                if (axis.type) {
                    fType.value = axis.type;
                } else {
                    fType.value = 'X-axis';
                }

                if (axis.valueType) {
                    fValueType.value = axis.valueType;
                } else {
                    fValueType.value = 'Category column';
                }

                if (axis.column) {
                    fColumn.value = axis.column;
                } else {
                    fColumn.value = '';
                }

                if (axis.values) {
                    fValues.value = axis.values;
                } else {
                    fValues.value = '';
                }

                if (axis.scale) {
                    fScale.checked = true;
                } else {
                    fScale.checked = false;
                }
            }

            function copyToMem(axis) {
                console.log('axis copy to mem');
                let changed = false;
                if (axis.name !== fName.value) {
                    axis.name = fName.value;
                    changed = true;
                }

                if (axis.type !== fType.value) {
                    axis.type = fType.value;
                    changed = true;
                }

                if (axis.valueType !== fValueType.value) {
                    axis.valueType = fValueType.value;
                    changed = true;
                }

                if (axis.column !== fColumn.value) {
                    axis.column = fColumn.value;
                    changed = true;
                }

                if (axis.values !== fValues.value) {
                    axis.values = fValues.value;
                    changed = true;
                }

                if (axis.scale !== fScale.checked) {
                    axis.scale = fScale.checked;
                }

                if (changed) {
                    axis.timestamp = new Date().getTime();
                }

                if (axis.callback) {
                    axis.callback();
                }
                return true;
            }

            function copyToLast() {
                if (last) {
                    copyToMem(last);
                }
            }

            function clear() {
                fName.value = '';
                fType.value = 'X-axis';
                fValueType.value = 'Category column';
                fColumn.value = '';
                fValues.value = '';
                fScale.checked = false;
            }

            fName.onfocusout = copyToLast;
            fType.onfocusout = copyToLast;
            fValueType.onfocusout = copyToLast;
            fColumn.onfocusout = copyToLast;
            fValues.onfocusout = copyToLast;
            fScale.onfocusout = copyToLast;

            return div.listManager(newQuery, copyToForm, copyToMem, clear);
        };
    }(jQuery)
);
