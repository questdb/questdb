import "echarts/lib/chart/bar"
import "echarts/lib/chart/line"
import "echarts/lib/component/tooltip"
import "echarts/lib/component/title"
import "docsearch.js/dist/cdn/docsearch.min.css"
import $ from "jquery"

import { setupConsoleController } from "./console-controller"
import { setupImportController } from "./import-controller"

import "../../styles/main.scss"
import "./grid"
import "./import"
import "./import-detail"
import "./quick-vis"
import "./splitter"

let messageBus = $({})
window.bus = messageBus

$(document).ready(function () {
  messageBus.trigger("preferences.load")

  const win = $(window)
  win.trigger("resize")
})

messageBus.on("react.ready", () => {
  setupConsoleController(messageBus)
  setupImportController(messageBus)
})
