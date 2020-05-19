import "ace-builds"
import "ace-builds/webpack-resolver"
import "echarts/lib/chart/bar"
import "echarts/lib/chart/line"
import "echarts/lib/component/tooltip"
import "echarts/lib/component/title"
import "docsearch.js/dist/cdn/docsearch.min.css"
import $ from "jquery"
import "metismenu"

import { setupConsoleController } from "./console-controller"
import { setupImportController } from "./import-controller"
import { setupVisualisationController } from "./vis-controller"

import "../../styles/main.scss"
import "./docsearch"
import * as qdb from "./globals"
import "./grid"
import "./import"
import "./import-detail"
import "./list-manager"
import "./query"
import "./quick-vis"
import "./splitter"
import "./vis-query-form"
import "./vis-series-form"
import "./vis-axis-form"

// Disable "back" button.
history.pushState(null, null, "index.html")
window.addEventListener("popstate", function () {
  history.pushState(null, null, "index.html")
})

let messageBus

function switchTo(name, index) {
  const menuItems = $("#side-menu").find("a")
  messageBus.trigger(qdb.MSG_ACTIVE_PANEL, name)
  const n = menuItems.length
  for (let i = 0; i < n; i++) {
    if (i === index) {
      menuItems[i].setAttribute("class", "selected")
    } else {
      menuItems[i].setAttribute("class", "")
    }
  }
}

function switchToConsole() {
  switchTo("console", 0)
}

function switchToVis() {
  switchTo("visualisation", 1)
}

function switchToImport() {
  switchTo("import", 1)
}

ace.define(
  "ace/mode/sql_highlight_rules",
  [
    "require",
    "exports",
    "module",
    "ace/lib/oop",
    "ace/mode/text_highlight_rules",
  ],
  function (e, t) {
    "use strict"
    var r = e("../lib/oop"),
      i = e("./text_highlight_rules").TextHighlightRules,
      s = function () {
        var e =
          "select|insert|update|delete|from|where|and|or|by|order|limit|as|case|when|else|end|type|left|right|join|on|outer|desc|asc|union|create|table|primary|key|if|foreign|not|references|default|null|inner|cross|natural|database|drop|grant|over|sample|partition|latest|NaN|with|rename|truncate|asof|copy|alter|into|values|index|add|column|then|distinct"
        var t = "true|false"
        var n =
          "avg|count|first|last|max|min|sum|ucase|lcase|mid|len|round|rank|now|format|coalesce|ifnull|isnull|nvl"
        var r =
          "int|date|string|symbol|float|double|binary|timestamp|long|long256"
        i = this.createKeywordMapper(
          {
            "support.function": n,
            keyword: e,
            "constant.language": t,
            "storage.type": r,
          },
          "identifier",
          !0,
        )
        this.$rules = {
          start: [
            { token: "comment", regex: "--.*$" },
            {
              token: "comment",
              start: "/\\*",
              end: "\\*/",
            },
            { token: "string", regex: '".*?"' },
            { token: "string", regex: "'.*?'" },
            {
              token: "constant.numeric",
              regex: "[+-]?\\d+(?:(?:\\.\\d*)?(?:[eE][+-]?\\d+)?)?\\b",
            },
            { token: i, regex: "[a-zA-Z_$][a-zA-Z0-9_$]*\\b" },
            {
              token: "keyword.operator",
              regex:
                "\\+|\\-|\\/|\\/\\/|%|<@>|@>|<@|&|\\^|~|<|>|<=|=>|==|!=|<>|=",
            },
            { token: "paren.lparen", regex: "[\\(]" },
            { token: "paren.rparen", regex: "[\\)]" },
            {
              token: "text",
              regex: "\\s+",
            },
          ],
        }
        this.normalizeRules()
      }
    r.inherits(s, i)
    t.SqlHighlightRules = s
  },
)

ace.define(
  "ace/mode/questdb",
  [
    "require",
    "exports",
    "module",
    "ace/lib/oop",
    "ace/mode/text",
    "ace/mode/sql_highlight_rules",
    "ace/range",
  ],
  function (e, t) {
    "use strict"
    var r = e("../lib/oop"),
      i = e("./text").Mode,
      s = e("./sql_highlight_rules").SqlHighlightRules,
      o = e("../range").Range,
      u = function () {
        this.HighlightRules = s
      }
    r.inherits(u, i),
      function () {
        this.lineCommentStart = "--"
        this.$id = "ace/mode/questdb"
      }.call(u.prototype)
    t.Mode = u
  },
)

$(document).ready(function () {
  messageBus = $({})

  window.bus = messageBus

  $("#side-menu").metisMenu()
  $("a#nav-console").click(switchToConsole)
  $("a#nav-import").click(switchToImport)
  $("a#nav-visualisation").click(switchToVis)

  messageBus.on(qdb.MSG_QUERY_FIND_N_EXEC, switchToConsole)

  setupConsoleController(messageBus)
  setupImportController(messageBus)
  setupVisualisationController(messageBus)
  switchToConsole()

  messageBus.trigger("preferences.load")

  const win = $(window)
  win.trigger("resize")
})
