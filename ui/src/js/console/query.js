import $ from "jquery"

import * as qdb from "./globals"

$.fn.query = function () {
  const bus = $(this)
  let qry
  let hActiveRequest = null
  let hPendingRequest = null
  let time
  const batchSize = qdb.queryBatchSize

  const requestParams = {
    query: "",
    limit: "",
  }

  function cancelActiveQuery() {
    if (hActiveRequest !== null) {
      hActiveRequest.abort()
      hActiveRequest = null
    }
  }

  function abortPending() {
    if (hPendingRequest !== null) {
      clearTimeout(hPendingRequest)
      hPendingRequest = null
    }
  }

  function handleServerResponse(r) {
    bus.trigger(qdb.MSG_QUERY_OK, {
      count: r.count,
      timings: r.timings && {
        ...r.timings,
        fetch: (Date.now() - time) * 1e6,
      },
    })

    if (r.dataset) {
      bus.trigger(qdb.MSG_QUERY_DATASET, r)
    }
    hActiveRequest = null
  }

  function handleServerError(r) {
    bus.trigger(qdb.MSG_QUERY_ERROR, {
      query: qry,
      r: r.responseJSON,
      status: r.status,
      statusText: r.statusText,
      delta: new Date().getTime() - time,
    })
    hActiveRequest = null
  }

  function sendQueryDelayed() {
    cancelActiveQuery()
    requestParams.query = qry.q
    requestParams.limit = "0," + batchSize
    requestParams.count = true
    requestParams.src = "con"
    requestParams.timings = true
    time = Date.now()
    hActiveRequest = $.get("/exec", requestParams)
    bus.trigger(qdb.MSG_QUERY_RUNNING)
    hActiveRequest.done(handleServerResponse).fail(handleServerError)
  }

  //noinspection JSUnusedLocalSymbols
  function sendQuery(x, q) {
    qry = q
    abortPending()
    hPendingRequest = setTimeout(sendQueryDelayed, 50)
  }

  function publishCurrentQuery() {
    bus.trigger("query.text", qry)
  }

  bus.on(qdb.MSG_QUERY_EXEC, sendQuery)
  bus.on(qdb.MSG_QUERY_CANCEL, cancelActiveQuery)
  bus.on("query.publish", publishCurrentQuery)
}

$.fn.domController = function () {
  const div = $(".js-query-spinner")
  const divMsg = $(".js-query-message-panel")
  let timer
  let runBtn
  let running = false
  const bus = $(this)

  function delayedStart() {
    div.addClass("query-progress-animated", 100)
    divMsg.removeClass("query-message-ok")
    divMsg.html("&nbsp;Running...")
  }

  function start() {
    running = true
    runBtn.html('<i class="fa fa-stop"></i>Cancel')
    runBtn.removeClass("js-query-run").addClass("js-query-cancel")
    timer = setTimeout(delayedStart, 500)
  }

  function stop() {
    runBtn.html('<i class="fa fa-play"></i>Run')
    runBtn.removeClass("js-query-cancel").addClass("js-query-run")
    clearTimeout(timer)
    div.removeClass("query-progress-animated")
    running = false
  }

  function toTextPosition(q, pos) {
    let r = 0,
      c = 0,
      n = Math.min(pos, q.q.length)
    for (let i = 0; i < n; i++) {
      if (q.q.charAt(i) === "\n") {
        r++
        c = 0
      } else {
        c++
      }
    }

    return {
      r: r + 1 + q.r,
      c: (r === 0 ? c + q.c : c) + 1,
    }
  }

  //noinspection JSUnusedLocalSymbols
  function error(x, m) {
    stop()
    divMsg.removeClass("query-message-ok").addClass("query-message-error")
    if (m.statusText === "abort") {
      divMsg.html("Cancelled by user")
    } else if (m.r) {
      const pos = toTextPosition(m.query, m.r.position)
      divMsg.html(`
        <div>
          Failed, it looks like there is an error with the query:
          <br />
          <span class="query-error-at"><strong>${pos.r}:${pos.c}</strong>&nbsp;&nbsp;${m.r.error}</span>
        </div>
      `)
      bus.trigger("editor.show.error", pos)
    } else if (m.status === 0) {
      divMsg.html("Server down?")
    } else {
      divMsg.html("Server error: " + m.status)
    }
  }

  function roundTiming(time) {
    return Math.round((time + Number.EPSILON) * 100) / 100
  }

  function formatTiming(nanos) {
    if (nanos === 0) {
      return "0"
    }

    if (nanos > 1e9) {
      return `${roundTiming(nanos / 1e9)}s`
    }

    if (nanos > 1e6) {
      return `${roundTiming(nanos / 1e6)}ms`
    }

    if (nanos > 1e3) {
      return `${roundTiming(nanos / 1e3)}μs`
    }

    return `${nanos}ns`
  }

  //noinspection JSUnusedLocalSymbols
  function ok(x, m) {
    stop()
    divMsg.removeClass("query-message-error").addClass("query-message-ok")

    const rows = m.count
      ? `<div class="query-result-value">
            <div>
              Row count
            </div>
            <div>
                ${m.count.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")}
            </div>
         </div>`
      : ""

    if (m.timings) {
      divMsg.html(`
    <div class="query-result-value">
      <div>
        Fetching time
      </div>
      <div>
        ${formatTiming(m.timings.fetch)}
      </div>
    </div>
    <div class="query-result-value">
      <div>
        Execution time
      </div>
      <div>
        ${formatTiming(m.timings.execute)}
      </div>
    </div>
    <div class="query-result-value">
      <div>
        Counting time
      </div>
      <div>
        ${formatTiming(m.timings.count)}
      </div>
    </div>
    <div class="query-result-value">
      <div>
        Compiling time
      </div>
      <div>
        ${formatTiming(m.timings.compiler)}
      </div>
    </div>
    ${rows}
    `)
    } else if (rows) {
      divMsg.html(rows)
    } else {
      divMsg.html("&nbsp;Success!")
    }
  }

  function toggleRunBtn() {
    if (running) {
      bus.trigger(qdb.MSG_QUERY_CANCEL)
    } else {
      bus.trigger(qdb.MSG_EDITOR_EXECUTE)
    }
  }

  function exportClick(e) {
    e.preventDefault()
    bus.trigger("grid.publish.query")
  }

  //noinspection JSUnusedLocalSymbols
  function exportQuery(x, e) {
    if (e) {
      window.location.href = "/exp?query=" + encodeURIComponent(e.q)
    }
  }

  function bind() {
    runBtn = $(".js-query-run")
    runBtn.click(toggleRunBtn)
    bus.on(qdb.MSG_QUERY_ERROR, error)
    bus.on(qdb.MSG_QUERY_OK, ok)
    bus.on(qdb.MSG_QUERY_RUNNING, start)
    bus.on(qdb.MSG_QUERY_EXPORT, exportQuery)

    $(".js-editor-toggle-invisible").click(function () {
      bus.trigger("editor.toggle.invisibles")
    })
    $(".js-query-export").click(exportClick)
  }

  bind()
}

$.fn.editor = function (msgBus) {
  let edit
  const storeKeys = {
    text: "query.text",
    line: "editor.line",
    col: "editor.col",
  }

  const Range = ace.require("ace/range").Range
  let marker
  const searchOpts = {
    wrap: true,
    caseSensitive: true,
    wholeWord: false,
    regExp: false,
    preventScroll: false,
  }
  const bus = msgBus
  const element = this

  function clearMarker() {
    if (marker) {
      edit.session.removeMarker(marker)
      marker = null
    }
  }

  function setup() {
    edit = qdb.createEditor(element[0])
    edit.session.on("change", clearMarker)
  }

  function loadPreferences() {
    if (typeof Storage !== "undefined") {
      const q = localStorage.getItem(storeKeys.text)
      if (q) {
        edit.setValue(q)
      }

      const row = localStorage.getItem(storeKeys.line)
      const col = localStorage.getItem(storeKeys.col)

      if (row && col) {
        setTimeout(() => {
          edit.gotoLine(row, col)
        }, 50)
      }
    }
  }

  function savePreferences() {
    if (typeof Storage !== "undefined") {
      localStorage.setItem(storeKeys.text, edit.getValue())
      localStorage.setItem(storeKeys.line, edit.getCursorPosition().row + 1)
      localStorage.setItem(storeKeys.col, edit.getCursorPosition().column)
    }
  }

  function computeQueryTextFromCursor() {
    const text = edit.getValue()
    const pos = edit.getCursorPosition()
    let r = 0
    let c = 0

    let startRow = 0
    let startCol = 0
    let startPos = -1
    let sql = null
    let inQuote = false
    let sqlTextStack = []

    for (let i = 0; i < text.length; i++) {
      const char = text.charAt(i)

      switch (char) {
        case ";":
          if (inQuote) {
            c++
            continue
          }

          if (r < pos.row || (r === pos.row && c < pos.column)) {
            sqlTextStack.push({
              row: startRow,
              col: startCol,
              pos: startPos,
              lim: i,
            })
            startRow = r
            startCol = c
            startPos = i + 1
            c++
          } else {
            if (startPos === -1) {
              sql = text.substring(0, i)
            } else {
              sql = text.substring(startPos, i)
            }
          }
          break
        case " ":
          // ignore leading space
          if (startPos === i) {
            startRow = r
            startCol = c
            startPos = i + 1
          }
          c++
          break
        case "\n":
          r++
          c = 0
          if (startPos === i) {
            startRow = r
            startCol = c
            startPos = i + 1
            c++
          }
          break
        case "'":
          inQuote = !inQuote
          c++
          break
        default:
          c++
          break
      }

      if (sql !== null) {
        break
      }
    }

    if (sql === null) {
      if (startPos === -1) {
        sql = text
      } else {
        sql = text.substring(startPos)
      }
    }

    if (sql.length === 0) {
      let prev = sqlTextStack.pop()
      if (prev) {
        return {
          q: text.substring(prev.pos, prev.lim),
          r: prev.row,
          c: prev.col,
        }
      }
      return null
    }

    return { q: sql, r: startRow, c: startCol }
  }

  function computeQueryTextFromSelection() {
    let q = edit.getSelectedText()
    let n = q.length
    let c
    while (n > 0 && ((c = q.charAt(n)) === " " || c === "\n" || c === ";")) {
      n--
    }

    if (n > 0) {
      q = q.substr(0, n + 1)
      const range = edit.getSelectionRange()
      return { q, r: range.start.row, c: range.start.column }
    }

    return null
  }

  function submitQuery() {
    bus.trigger("preferences.save")
    clearMarker()
    let q
    if (edit.getSelectedText() === "") {
      q = computeQueryTextFromCursor()
    } else {
      q = computeQueryTextFromSelection()
    }

    if (q) {
      bus.trigger(qdb.MSG_QUERY_EXEC, q)
    }
  }

  //noinspection JSUnusedLocalSymbols
  function showError(x, pos) {
    const token = edit.session.getTokenAt(pos.r - 1, pos.c)
    marker = edit.session.addMarker(
      new Range(
        pos.r - 1,
        pos.c - 1,
        pos.r - 1,
        pos.c + token.value.length - 1,
      ),
      "js-syntax-error",
      "text",
      true,
    )

    edit.gotoLine(pos.r, pos.c - 1)
    edit.focus()
  }

  function toggleInvisibles() {
    edit.renderer.setShowInvisibles(!edit.renderer.getShowInvisibles())
  }

  function focusGrid() {
    bus.trigger("grid.focus")
  }

  //noinspection JSUnusedLocalSymbols
  function findOrInsertQuery(e, q) {
    // "select" existing query or append text of new one
    // "find" will select text if anything is found, so we just
    // execute whats there
    if (!edit.find("'" + q + "'", searchOpts)) {
      const row = edit.session.getLength()
      const text = "\n'" + q + "';"
      edit.session.insert(
        {
          row,
          column: 0,
        },
        text,
      )
      edit.selection.moveCursorToPosition({
        row: row + 1,
        column: 0,
      })
      edit.selection.selectLine()
    }
    submitQuery()
  }

  function insertColumn(e, q) {
    edit.insert(", " + q)
    edit.focus()
  }

  function bind() {
    bus.on(qdb.MSG_EDITOR_EXECUTE, submitQuery)
    bus.on(qdb.MSG_EDITOR_EXECUTE_ALT, submitQuery)
    bus.on("editor.show.error", showError)
    bus.on("editor.toggle.invisibles", toggleInvisibles)
    bus.on(qdb.MSG_QUERY_FIND_N_EXEC, findOrInsertQuery)
    bus.on("editor.insert.column", insertColumn)
    bus.on(qdb.MSG_EDITOR_FOCUS, function () {
      edit.scrollToLine(
        edit.getCursorPosition().row + 1,
        true,
        true,
        function () {},
      )
      edit.focus()
    })

    edit.commands.addCommand({
      name: qdb.MSG_EDITOR_EXECUTE,
      bindKey: "F9",
      exec: submitQuery,
    })

    edit.commands.addCommand({
      name: qdb.MSG_EDITOR_EXECUTE_ALT,
      bindKey: {
        mac: "Command-Enter",
        win: "Ctrl-Enter",
      },
      exec: submitQuery,
    })

    edit.commands.addCommand({
      name: "editor.focus.grid",
      bindKey: "F2",
      exec: focusGrid,
    })

    bus.on("preferences.load", loadPreferences)
    bus.on("preferences.save", savePreferences)
  }

  setup()
  bind()
}
