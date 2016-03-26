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
/*globals ace:false */
/*globals Slick:false */

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

    // Collapse ibox function
    $('.collapse-link').click(function () {
        var ibox = $(this).closest('div.ibox');
        var button = $(this).find('i');
        var content = ibox.find('div.ibox-content');
        content.slideToggle(200);
        button.toggleClass('fa-chevron-up').toggleClass('fa-chevron-down');
        ibox.toggleClass('').toggleClass('border-bottom');
        setTimeout(function () {
            ibox.resize();
            ibox.find('[id^=map-]').resize();
        }, 50);
    });

    // Close ibox function
    $('.close-link').click(function () {
        var content = $(this).closest('div.ibox');
        content.remove();
    });

    // Fullscreen ibox function
    $('.fullscreen-link').click(function () {
        var ibox = $(this).closest('div.ibox');
        var button = $(this).find('i');
        $('body').toggleClass('fullscreen-ibox-mode');
        button.toggleClass('fa-expand').toggleClass('fa-compress');
        ibox.toggleClass('fullscreen');
        setTimeout(function () {
            $(window).trigger('resize');
        }, 100);
    });

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

    // Close menu in canvas mode
    $('.close-canvas-menu').click(function () {
        $('body').toggleClass('mini-navbar');
        smoothlyMenu();
    });

    // Run menu of canvas
    $('body.canvas-menu .sidebar-collapse').slimScroll({
        height: '100%',
        railOpacity: 0.9
    });

    // Open close right sidebar
    $('.right-sidebar-toggle').click(function () {
        $('#right-sidebar').toggleClass('sidebar-open');
    });

    // Initialize slimscroll for right sidebar
    $('.sidebar-container').slimScroll({
        height: '100%',
        railOpacity: 0.4,
        wheelStep: 10
    });

    // Open close small chat
    $('.open-small-chat').click(function () {
        $(this).children().toggleClass('fa-comments').toggleClass('fa-remove');
        $('.small-chat-box').toggleClass('active');
    });

    // Initialize slimscroll for small chat
    $('.small-chat-box .content').slimScroll({
        height: '234px',
        railOpacity: 0.4
    });

    $('.check-link').click(function () {
        var button = $(this).find('i');
        var label = $(this).next('span');
        button.toggleClass('fa-check-square').toggleClass('fa-square-o');
        label.toggleClass('todo-completed');
        return false;
    });

    // Append config box / Only for demo purpose
    // Uncomment on server mode to enable XHR calls
    //$.get("skin-config.html", function (data) {
    //    if (!$('body').hasClass('no-skin-config'))
    //        $('body').append(data);
    //});

    // Minimalize menu
    $('.navbar-minimalize').click(function () {
        $('body').toggleClass('mini-navbar');
        smoothlyMenu();
    });

    // Tooltips demo
    $('.tooltip-demo').tooltip({
        selector: '[data-toggle=tooltip]',
        container: 'body'
    });

    // Move modal to body
    // Fix Bootstrap backdrop issu with animation.css
    $('.modal').appendTo('body');

    fixHeight();

    // Fixed Sidebar
    $(window).bind('load', function () {
        if ($('body').hasClass('fixed-sidebar')) {
            $('.sidebar-collapse').slimScroll({
                height: '100%',
                railOpacity: 0.9
            });
        }
    });

    // Move right sidebar top after scroll
    $(window).scroll(function () {
        if ($(window).scrollTop() > 0 && !$('body').hasClass('fixed-nav')) {
            $('#right-sidebar').addClass('sidebar-top');
        } else {
            $('#right-sidebar').removeClass('sidebar-top');
        }
    });

    $(window).bind('load resize scroll', function () {
        if (!$('body').hasClass('body-small')) {
            fixHeight();
        }
    });

    $('[data-toggle=popover]').popover();
    $('[data-toggle=tooltip]').tooltip();

    // Add slimscroll to element
    $('.full-height-scroll').slimscroll({
        height: '100%'
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
        var fixedsidebar = localStorage.getItem('fixedsidebar');
        var fixednavbar = localStorage.getItem('fixednavbar');
        var boxedlayout = localStorage.getItem('boxedlayout');
        var fixedfooter = localStorage.getItem('fixedfooter');

        var body = $('body');

        if (fixedsidebar === 'on') {
            body.addClass('fixed-sidebar');
            $('.sidebar-collapse').slimScroll({
                height: '100%',
                railOpacity: 0.9
            });
        }

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

    var e = ace.edit('sqlEditor');
    e.getSession().setMode('ace/mode/sql');
    e.setTheme('ace/theme/merbivore_soft');
    e.setShowPrintMargin(false);
    e.setDisplayIndentGuides(false);
    e.setHighlightActiveLine(false);

    // read editor contents from local storage
    if (typeof (Storage) !== 'undefined' && localStorage.getItem('lastQuery')) {
        e.setValue(localStorage.getItem('lastQuery'));
    }

    e.focus();

    var container = $('#grid');
    container.css('height', '430px');

    var grid = new Slick.Grid(container, [], [], {
        enableCellNavigation: true,
        enableColumnReorder: false,
        enableTextSelectionOnCells: true
    });

    grid.resizeCanvas();
});
