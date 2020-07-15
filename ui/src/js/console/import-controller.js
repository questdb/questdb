import $ from "jquery"

import * as qdb from "./globals"

let upperHalfHeight = 515

function resize() {
  const footer = $(".footer")
  const importTopPanel = $("#import-top")
  const canvasPanel = importTopPanel.find(".ud-canvas")
  const importDetail = $("#import-detail")
  const importMenu = $("#import-menu")[0]
  const h = $(window)[0].innerHeight
  const footerHeight = footer.offsetHeight

  qdb.setHeight(importTopPanel, upperHalfHeight)
  qdb.setHeight(
    importDetail,
    h - footerHeight - upperHalfHeight - importMenu.offsetHeight - 50,
  )

  let r1 = importTopPanel[0].getBoundingClientRect()
  let r2 = canvasPanel[0].getBoundingClientRect()
}

function splitterResize(x, p) {
  upperHalfHeight += p
  $(window).trigger("resize")
}

export function setupImportController(bus) {
  $(window).bind("resize", resize)

  $("#dragTarget").dropbox(bus)
  $("#import-file-list").importManager(bus)
  $("#import-detail").importEditor(bus)
  $("#import-splitter").splitter(bus, "import", 420, 250)

  bus.on("splitter.import.resize", splitterResize)
}
