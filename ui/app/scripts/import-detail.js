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

    var data = [
        'INT',
        'STRING',
        'DOUBLE'
    ];


    var select = $('<select class="g-dynamic-select form-control m-b"/>');

    for (var i = 0; i < data.length; i++) {
        var val = data[i];
        $('<option />', {value: val, text: val}).appendTo(select);
    }

    $('.g-type').click(function () {
        var div = $(this);
        select.appendTo(div.parent());
        select.css('display', 'block');
        select.css('left', div.css('left'));
        select.css('width', div.css('width'));
        select.val(div.text());
        select.changeTarget = div;
        select.focus();
    });

    $('.g-other').click(function () {
        select.css('display', 'none');
    });

    select.change(function () {
        select.changeTarget.html($(this).find('option:selected').text());
        select.css('display', 'none');
    });

    var statsSwitcher = $('.stats-switcher');


    $('.import-stats-chart').click(function () {
        if (statsSwitcher.hasClass('stats-visible')) {
            statsSwitcher.removeClass('stats-visible');
        } else {
            statsSwitcher.addClass('stats-visible');
        }
    });
});
