/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * The MIT License (MIT)
 *
 * Copyright (C) 2016 Appsicle
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
/*
 *
 *   INSPINIA - Responsive Admin Theme
 *   version 2.4
 *
 */

// check if browser support HTML5 local storage
function localStorageSupport() {
    'use strict';
    return (('localStorage' in window) && window.localStorage !== null);
}

// Full height of sidebar
function fixHeight() {
    'use strict';

    var heightWithoutNavbar = $('body > #wrapper').height() - 61;
    $('.sidebard-panel').css('min-height', heightWithoutNavbar + 'px');

    var navbarHeigh = $('nav.navbar-default').height();
    var pw = $('#page-wrapper');
    var wrapperHeigh = pw.height();

    if (navbarHeigh > wrapperHeigh) {
        pw.css('min-height', navbarHeigh + 'px');
    }

    if (navbarHeigh < wrapperHeigh) {
        pw.css('min-height', $(window).height() + 'px');
    }

    if ($('body').hasClass('fixed-nav')) {
        if (navbarHeigh > wrapperHeigh) {
            pw.css('min-height', navbarHeigh - 60 + 'px');
        } else {
            pw.css('min-height', $(window).height() - 60 + 'px');
        }
    }
}

// Configure layout items
$(document).ready(function () {
    'use strict';

    // Add body-small class if window less than 768px
    if ($(this).width() < 769) {
        $('body').addClass('body-small');
    } else {
        $('body').removeClass('body-small');
    }

    $('#side-menu').metisMenu();

    function smoothlyMenu() {
        var b = $('body');
        if (!b.hasClass('mini-navbar') || b.hasClass('body-small')) {
            // Hide menu in order to smoothly turn on when maximize menu
            $('#side-menu').hide();
            // For smoothly turn on menu
            setTimeout(
                function () {
                    $('#side-menu').fadeIn(400);
                }, 200);
        } else if (b.hasClass('fixed-sidebar')) {
            $('#side-menu').hide();
            setTimeout(
                function () {
                    $('#side-menu').fadeIn(400);
                }, 100);
        } else {
            // Remove all inline style from jquery fadeIn function to reset menu state
            $('#side-menu').removeAttr('style');
        }
    }

    // Minimalize menu
    $('.navbar-minimalize').click(function () {
        $('body').toggleClass('mini-navbar');
        smoothlyMenu();
    });

    // Move modal to body
    // Fix Bootstrap backdrop issu with animation.css
    $('.modal').appendTo('body');

    fixHeight();

    $(window).bind('load resize scroll', function () {
        if (!$('body').hasClass('body-small')) {
            fixHeight();
        }
    });
});

// Minimalize menu when screen is less than 768px
$(window).bind('resize', function () {
    'use strict';
    if ($(this).width() < 769) {
        $('body').addClass('body-small');
    } else {
        $('body').removeClass('body-small');
    }
});

// Local Storage functions
// Set proper body class and plugins based on user configuration
$(document).ready(function () {
    'use strict';
    if (localStorageSupport) {

        var collapse = localStorage.getItem('collapse_menu');
        var fixednavbar = localStorage.getItem('fixednavbar');
        var boxedlayout = localStorage.getItem('boxedlayout');
        var fixedfooter = localStorage.getItem('fixedfooter');

        var body = $('body');

        if (collapse === 'on') {
            if (body.hasClass('fixed-sidebar')) {
                if (!body.hasClass('body-small')) {
                    body.addClass('mini-navbar');
                }
            } else {
                if (!body.hasClass('body-small')) {
                    body.addClass('mini-navbar');
                }

            }
        }

        if (fixednavbar === 'on') {
            $('.navbar-static-top').removeClass('navbar-static-top').addClass('navbar-fixed-top');
            body.addClass('fixed-nav');
        }

        if (boxedlayout === 'on') {
            body.addClass('boxed-layout');
        }

        if (fixedfooter === 'on') {
            $('.footer').addClass('fixed');
        }
    }
});

// Configure SQL Editor items
$(document).ready(function () {
    'use strict';

    var divSqlPanel = $('.js-sql-panel');
    var divImportPanel = $('.js-import-panel');

    $('a#sql-editor').click(function () {
        divSqlPanel.show();
        divImportPanel.hide();
        $('#sqlEditor').css('height', '240px');
    });

    $('a#file-upload').click(function () {
        divSqlPanel.hide();
        divImportPanel.show();
    });

    // var e = ace.edit('sqlEditor');
    // e.getSession().setMode('ace/mode/sql');
    // e.setTheme('ace/theme/merbivore_soft');
    // e.setShowPrintMargin(false);
    // e.setDisplayIndentGuides(false);
    // e.setHighlightActiveLine(false);
    //
    // // read editor contents from local storage
    // if (typeof (Storage) !== 'undefined' && localStorage.getItem('lastQuery')) {
    //     e.setValue(localStorage.getItem('lastQuery'));
    // }
    //
    // e.focus();

    var container = $('#grid');
    container.css('height', '430px');
});
