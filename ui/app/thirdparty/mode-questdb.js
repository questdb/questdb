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

ace.define(
    "ace/mode/sql_highlight_rules",
    ["require", "exports", "module", "ace/lib/oop", "ace/mode/text_highlight_rules"],
    function (e, t) {
        "use strict";
        var r = e("../lib/oop"), i = e("./text_highlight_rules").TextHighlightRules, s = function () {
            var e = "select|insert|update|delete|from|where|and|or|by|order|limit|as|case|when|else|end|type|left|right|join|on|outer|desc|asc|union|create|table|primary|key|if|foreign|not|references|default|null|inner|cross|natural|database|drop|grant|over|sample|partition|latest|NaN|with|rename|truncate|asof|copy|alter|into|values|index|add|column";
            var t = "true|false";
            var n = "avg|count|first|last|max|min|sum|ucase|lcase|mid|len|round|rank|now|format|coalesce|ifnull|isnull|nvl";
            var r = "int|date|string|symbol|float|double|binary|timestamp|long|long256";
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