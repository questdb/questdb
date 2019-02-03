/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

ace.define(
    "ace/mode/sql_highlight_rules",
    ["require", "exports", "module", "ace/lib/oop", "ace/mode/text_highlight_rules"],
    function (e, t) {
        "use strict";
        var r = e("../lib/oop"), i = e("./text_highlight_rules").TextHighlightRules, s = function () {
            var e = "select|insert|update|delete|from|where|and|or|by|order|limit|as|case|when|else|end|type|left|right|join|on|outer|desc|asc|union|create|table|primary|key|if|foreign|not|references|default|null|inner|cross|natural|database|drop|grant|over|sample|partition|latest|NaN|with|rename";
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
            this.normalizeRules();
        };
        r.inherits(s, i);
        t.SqlHighlightRules = s;
    });


ace.define("ace/mode/questdb", ["require", "exports", "module", "ace/lib/oop", "ace/mode/text", "ace/mode/sql_highlight_rules", "ace/range"], function (e, t) {
    "use strict";
    var r = e("../lib/oop"), i = e("./text").Mode, s = e("./sql_highlight_rules").SqlHighlightRules, o = e("../range").Range, u = function () {
        this.HighlightRules = s;
    };
    r.inherits(u, i), function () {
        this.lineCommentStart = "--";
        this.$id = "ace/mode/questdb";
    }.call(u.prototype);
    t.Mode = u;
});