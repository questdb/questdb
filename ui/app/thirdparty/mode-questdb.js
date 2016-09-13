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

ace.define(
    "ace/mode/sql_highlight_rules",
    ["require", "exports", "module", "ace/lib/oop", "ace/mode/text_highlight_rules"],
    function (e, t, n) {
        "use strict";
        var r = e("../lib/oop"), i = e("./text_highlight_rules").TextHighlightRules, s = function () {
            var e = "select|insert|update|delete|from|where|and|or|by|order|limit|as|case|when|else|end|type|left|right|join|on|outer|desc|asc|union|create|table|primary|key|if|foreign|not|references|default|null|inner|cross|natural|database|drop|grant|over|sample|partition|latest|NaN";
            var t = "true|false";
            var n = "avg|count|first|last|max|min|sum|ucase|lcase|mid|len|round|rank|now|format|coalesce|ifnull|isnull|nvl";
            var r = "int|date|string|symbol|float|double|binary|timestamp";
            i = this.createKeywordMapper({
                "support.function": n,
                keyword: e,
                "constant.language": t,
                "storage.type": r
            }, "identifier", !0);
            this.$rules = {
                start: [{token: "comment", regex: "--.*$"}, {
                    token: "comment",
                    start: "/\\*",
                    end: "\\*/"
                }, {token: "string", regex: '".*?"'}, {token: "string", regex: "'.*?'"}, {
                    token: "constant.numeric",
                    regex: "[+-]?\\d+(?:(?:\\.\\d*)?(?:[eE][+-]?\\d+)?)?\\b"
                }, {token: i, regex: "[a-zA-Z_$][a-zA-Z0-9_$]*\\b"}, {
                    token: "keyword.operator",
                    regex: "\\+|\\-|\\/|\\/\\/|%|<@>|@>|<@|&|\\^|~|<|>|<=|=>|==|!=|<>|="
                }, {token: "paren.lparen", regex: "[\\(]"}, {token: "paren.rparen", regex: "[\\)]"}, {
                    token: "text",
                    regex: "\\s+"
                }]
            };
            this.normalizeRules()
        };
        r.inherits(s, i);
        t.SqlHighlightRules = s
    });


ace.define("ace/mode/questdb", ["require", "exports", "module", "ace/lib/oop", "ace/mode/text", "ace/mode/sql_highlight_rules", "ace/range"], function (e, t, n) {
    "use strict";
    var r = e("../lib/oop"), i = e("./text").Mode, s = e("./sql_highlight_rules").SqlHighlightRules, o = e("../range").Range, u = function () {
        this.HighlightRules = s
    };
    r.inherits(u, i), function () {
        this.lineCommentStart = "--";
        this.$id = "ace/mode/questdb"
    }.call(u.prototype);
    t.Mode = u
});