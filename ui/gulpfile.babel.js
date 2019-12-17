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

import gulpLoadPlugins from "gulp-load-plugins";
import fileinclude from "gulp-file-include";

const dist = '../core/src/main/resources/site/public';
const {src, dest, series} = require('gulp');
const del = require('del');
const path = require('path');
const $ = gulpLoadPlugins();


function clean(cb) {
    del(['.tmp', dist], {force: true});
    cb();
}

function tidy(cb) {
    del(['.tmp'], {force: true});
    cb();
}

function lint() {
    return src('app/scripts/**/*.js')
        .pipe($.eslint({}))
        .pipe($.eslint.format())
        ;
}

function styles() {
    return src('app/styles/*.scss')
        .pipe($.plumber())
        .pipe($.sourcemaps.init())
        .pipe($.sass.sync({
            outputStyle: 'expanded',
            precision: 10,
            includePaths: ['.']
        }).on('error', $.sass.logError))
        .pipe($.autoprefixer({browsers: ['> 1%', 'last 2 versions', 'Firefox ESR']}))
        .pipe($.sourcemaps.write())
        .pipe(dest('.tmp/styles'))
        ;
}

function scripts() {
    return src(['app/scripts/**/*.js', 'app/thirdparty/**/*.js'])
        .pipe($.plumber())
        .pipe($.sourcemaps.init())
        .pipe($.babel())
        .pipe($.sourcemaps.write('.'))
        .pipe(dest('.tmp/scripts'))
        ;

}

function html() {
    return src('app/*.html')
        .pipe(fileinclude({
            prefix: '@@',
            basepath: '@file'
        }))
        .pipe($.useref({searchPath: ['.tmp', 'app', '.']}))
        .pipe($.if('/\.js$/', $.uglify({compress: {drop_console: true}})))
        .pipe($.if('/\.css$/b', $.cssnano({safe: true, autoprefixer: false, discardComments: {removeAll: true}})))
        .pipe($.if('/\.html$/', $.htmlmin({
            collapseWhitespace: true,
            minifyCSS: true,
            minifyJS: {compress: {drop_console: true}},
            processConditionalComments: true,
            removeComments: true,
            removeEmptyAttributes: true,
            removeScriptTypeAttributes: true,
            removeStyleLinkTypeAttributes: true
        })))
        .pipe(dest(dist))
        ;
}

function fonts() {
    return src(
        require('main-bower-files')
        ('**/*.{eot,svg,ttf,woff,woff2}', function (err) {
        })
            .concat('app/fonts/**/*'))
        .pipe(dest(path.join(dist, 'fonts')))
        ;
}

function css_patterns() {
    return src('app/styles/patterns/**/*')
        .pipe(dest(path.join(dist, 'styles/patterns')))
        ;
}

function images() {
    return src('app/images/**/*')
        .pipe($.cache($.imagemin({
            progressive: true,
            interlaced: true,
            // don't remove IDs from SVGs, they are often used
            // as hooks for embedding and styling
            svgoPlugins: [{cleanupIDs: false}]
        })))
        .pipe(dest(path.join(dist, 'images')))
        ;
}

function extras() {
    return src([
        'app/*.*',
        '!app/*.html'
    ], {
        dot: true
    })
        .pipe(dest(dist))
        ;
}

exports.default = series(
    clean,
    lint,
    styles,
    scripts,
    html,
    fonts,
    css_patterns,
    images,
    extras,
    tidy
);