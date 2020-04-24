import Clipboard from "clipboard"
import $ from "jquery"

import * as qdb from "./globals"

const divSqlPanel = $(".js-sql-panel")
const divExportUrl = $(".js-export-url")
const editor = $("#editor")
const sqlEditor = $("#sqlEditor")
const consoleTop = $("#console-top")
const wrapper = $("#page-wrapper")
const msgPanel = editor.find(".js-query-message-panel")
const navbar = $("nav.navbar-default")
const win = $(window)
const grid = $("#grid")
const quickVis = $("#quick-vis")
const toggleChartBtn = $("#js-toggle-chart")
const toggleGridBtn = $("#js-toggle-grid")

let topHeight = 350
const bottomHeight = 350
let visible = false

function resize() {
  if (visible) {
    const navbarHeight = navbar.height()
    const wrapperHeight = wrapper.height()
    const msgPanelHeight = msgPanel.height()
    let h

    if (navbarHeight > wrapperHeight) {
      h = navbarHeight
    }

    if (navbarHeight < wrapperHeight) {
      h = win.height()
    }

    if (h) {
      if (h < topHeight + bottomHeight) {
        h = topHeight + bottomHeight
      }
      // qdb.setHeight(wrapper, h - 1);
    }

    qdb.setHeight(consoleTop, topHeight)
    qdb.setHeight(sqlEditor, topHeight - msgPanelHeight - 60)
  }
}

function loadSplitterPosition() {
  if (typeof Storage !== "undefined") {
    const n = localStorage.getItem("splitter.position")
    if (n) {
      topHeight = parseInt(n)
      if (!topHeight) {
        topHeight = 350
      }
    }
  }
}

function saveSplitterPosition() {
  if (typeof Storage !== "undefined") {
    localStorage.setItem("splitter.position", topHeight)
  }
}

function toggleVisibility(x, name) {
  if (name === "console") {
    visible = true
    divSqlPanel.show()
  } else {
    visible = false
    divSqlPanel.hide()
  }
}

function toggleChart() {
  toggleChartBtn.addClass("active")
  toggleGridBtn.removeClass("active")
  grid.hide()
  quickVis.show()
  quickVis.resize()
}

function toggleGrid() {
  toggleChartBtn.removeClass("active")
  toggleGridBtn.addClass("active")
  grid.show()
  quickVis.hide()
  grid.resize()
}

export function setupConsoleController(bus) {
  win.bind("resize", resize)
  bus.on(qdb.MSG_QUERY_DATASET, function (e, m) {
    divExportUrl.val(qdb.toExportUrl(m.query))
  })

  divExportUrl.click(function () {
    this.select()
  })

  /* eslint-disable no-new */
  new Clipboard(".js-export-copy-url")
  $(".js-query-refresh").click(function () {
    bus.trigger("grid.refresh")
  })

  // named splitter
  bus.on("splitter.console.resize", function (x, e) {
    topHeight += e
    win.trigger("resize")
    bus.trigger("preferences.save")
  })

  bus.on("preferences.save", saveSplitterPosition)
  bus.on("preferences.load", loadSplitterPosition)
  bus.on(qdb.MSG_ACTIVE_PANEL, toggleVisibility)

  bus.query()
  bus.domController()

  sqlEditor.editor(bus)
  grid.grid(bus)
  quickVis.quickVis(bus)

  $("#console-splitter").splitter(bus, "console", 200, 0)

  // wire query publish
  toggleChartBtn.click(toggleChart)
  toggleGridBtn.click(toggleGrid)
  bus.on(qdb.MSG_QUERY_DATASET, toggleGrid)
}
