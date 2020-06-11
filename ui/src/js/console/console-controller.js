import $ from "jquery"

import * as qdb from "./globals"

export function setupConsoleController(bus) {
  const grid = $("#grid")
  const quickVis = $("#quick-vis")

  grid.grid(bus)
  quickVis.quickVis(bus)
}
