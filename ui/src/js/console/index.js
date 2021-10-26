/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
