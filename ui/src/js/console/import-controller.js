import $ from "jquery"

import * as qdb from "./globals"

const divImportPanel = $(".js-import-panel")
const importTopPanel = $("#import-top")
const importDetail = $("#import-detail")
const importMenu = $("#import-menu")[0]

const footer = $("#footer")[0]
const canvasPanel = importTopPanel.find(".ud-canvas")
const w = $(window)
let visible = false

let upperHalfHeight = 450

function resize() {
  if (visible) {
    const h = w[0].innerHeight
    const footerHeight = footer.offsetHeight
    qdb.setHeight(importTopPanel, upperHalfHeight)
    qdb.setHeight(
      importDetail,
      h - footerHeight - upperHalfHeight - importMenu.offsetHeight - 10,
    )

    let r1 = importTopPanel[0].getBoundingClientRect()
    let r2 = canvasPanel[0].getBoundingClientRect()
    // qdb.setHeight(importTopPanel, upperHalfHeight);
    qdb.setHeight(canvasPanel, upperHalfHeight - (r2.top - r1.top) - 10)
  }
}

function toggleVisibility(x, name) {
  if (name === "import") {
    visible = true
    divImportPanel.show()
    w.trigger("resize")
  } else {
    visible = false
    divImportPanel.hide()
  }
}

function splitterResize(x, p) {
  upperHalfHeight += p
  w.trigger("resize")
}

export function setupImportController(bus) {
  w.bind("resize", resize)

  $("#dragTarget").dropbox(bus)
  $("#import-file-list").importManager(bus)
  $("#import-detail").importEditor(bus)
  $("#import-splitter").splitter(bus, "import", 470, 300)

  bus.on("splitter.import.resize", splitterResize)
  bus.on(qdb.MSG_ACTIVE_PANEL, toggleVisibility)
}
