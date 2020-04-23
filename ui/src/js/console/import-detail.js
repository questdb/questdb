import $ from "jquery"

import * as qdb from "./globals"

$.fn.importEditor = function (ebus) {
  const container = $(this)
  const statsSwitcher = $(".stats-switcher")
  const divEditor = $("#js-import-editor")
  const msgPanel = $("#js-import-error")
  const placeholder = $("#js-import-placeholder")
  const divMessage = $(this).find(".js-message")
  const divTabName = $(this).find(".js-import-tab-name")
  const divRejectedPct = $(this).find(".import-rejected")
  const divImportedPct = $(this).find(".import-imported")
  const divRejectedCount = $(this).find(".js-rejected-row-count")
  const divImportedCount = $(this).find(".js-imported-row-count")
  const divCanvas = $(this).find(".ud-canvas")
  const footerHeight = $(".footer")[0].offsetHeight
  const lineHeight = 35
  let select
  let location
  const types = [
    { text: "AUTO", value: null },
    { text: "BOOLEAN", value: "BOOLEAN" },
    { text: "BYTE", value: "BYTE" },
    { text: "DOUBLE", value: "DOUBLE" },
    { text: "DATE", value: "DATE" },
    { text: "FLOAT", value: "FLOAT" },
    { text: "INT", value: "INT" },
    { text: "LONG", value: "LONG" },
    { text: "SHORT", value: "SHORT" },
    { text: "CHAR", value: "CHAR" },
    { text: "STRING", value: "STRING" },
    { text: "SYMBOL", value: "SYMBOL" },
  ]

  let current = null
  const editorBus = ebus

  function resizeCanvas() {
    const top = divCanvas[0].getBoundingClientRect().top
    let h = Math.round(window.innerHeight - top)
    h = h - footerHeight - 45
    divCanvas[0].style.height = h + "px"
  }

  function selectClick() {
    const div = $(this)
    select.appendTo(div.parent())
    select.css("left", div.css("left"))
    select.css("width", div.css("width"))

    // find column index
    const colIndex = parseInt($(this).parent().find(".js-g-row").text()) - 1

    // get column
    const col = current.response.columns[colIndex]

    // set option
    if (col.altType) {
      select.val(col.altType.value)
    } else {
      select.val(col.type)
    }

    select.changeTargetDiv = div
    select.changeTargetCol = col

    select.show()
    select.focus()
  }

  function selectHide() {
    select.hide()
  }

  function getTypeHtml(col) {
    if (col.altType && col.altType.text !== col.type) {
      return (
        col.type +
        '<i class="fa fa-angle-double-right g-type-separator"></i>' +
        col.altType.text
      )
    } else {
      return col.type
    }
  }

  function calcModifiedFlag() {
    let modified = false
    for (let i = 0; i < current.response.columns.length; i++) {
      const col = current.response.columns[i]
      if (col.altType && col.type !== col.altType.text) {
        modified = true
        break
      }
    }

    $(document).trigger(
      modified ? "import.line.overwrite" : "import.line.cancel",
      current,
    )
  }

  function selectChange() {
    const sel = $(this).find("option:selected")
    select.changeTargetCol.altType = { text: sel.text(), value: sel.val() }
    select.changeTargetDiv.html(getTypeHtml(select.changeTargetCol))
    calcModifiedFlag()
    selectHide()
  }

  function attachSelect() {
    $(".g-type").click(selectClick)
    $(".g-other").click(selectHide)
    select.change(selectChange)
  }

  function render(e) {
    if (e.importState === 0 && !e.response) {
      // aborted at start
      return
    }

    if (e.response && e.importState === 0) {
      divTabName.html((location = e.response.location))

      // update "chart"
      const importedRows = e.response.rowsImported
      const rejectedRows = e.response.rowsRejected
      const totalRows = importedRows + rejectedRows
      divRejectedPct.css(
        "width",
        Math.round((rejectedRows * 100) / totalRows) + "%",
      )
      divImportedPct.css(
        "width",
        Math.round((importedRows * 100) / totalRows) + "%",
      )

      // update counts
      divRejectedCount.html(rejectedRows)
      divImportedCount.html(importedRows)

      divCanvas.empty()

      // records
      if (e.response.columns) {
        let top = 0
        for (let k = 0; k < e.response.columns.length; k++) {
          const col = e.response.columns[k]
          divCanvas.append(
            '<div class="ud-row" style="top: ' +
              top +
              'px">' +
              '<div class="ud-cell gc-1 g-other js-g-row">' +
              (k + 1) +
              "</div>" +
              '<div class="ud-cell gc-2 g-other">' +
              (col.errors > 0
                ? '<i class="fa fa-exclamation-triangle g-warning"></i>'
                : "") +
              col.name +
              "</div>" +
              '<div class="ud-cell gc-3 g-type">' +
              getTypeHtml(col) +
              "</div>" +
              '<div class="ud-cell gc-4 g-other">' +
              (col.pattern !== undefined ? col.pattern : "") +
              "</div>" +
              '<div class="ud-cell gc-5 g-other">' +
              (col.locale !== undefined ? col.locale : "") +
              "</div>" +
              '<div class="ud-cell gc-6 g-other">' +
              col.errors +
              "</div>" +
              "</div>",
          )

          top += lineHeight
        }
      }

      attachSelect()

      // display component
      divEditor.show()
      msgPanel.hide()
      resizeCanvas()
    } else {
      switch (e.importState) {
        case 1:
          divMessage.html(
            "Table <strong>" + e.name + "</strong> already exists",
          )
          break
        case 2:
          divMessage.html("Name <strong>" + e.name + "</strong> is reserved")
          break
        case 3:
          divMessage.html("Server is not responding...")
          break
        case 4:
          divMessage.html(e.response)
          break
        case 5:
          divMessage.html(
            "Server encountered internal problem. Check server logs for more details.",
          )
          break
        default:
          divMessage.html("Unknown error: " + e.responseStatus)
          break
      }
      divEditor.hide()
      msgPanel.show()
      placeholder.hide()
      // reset button group option
    }
    container.show()
  }

  function setupSelect() {
    select = $('<select class="g-dynamic-select form-control m-b"/>')
    for (let i = 0; i < types.length; i++) {
      const val = types[i]
      $("<option />", { value: val.value, text: val.text }).appendTo(select)
    }
  }

  $(document).on("import.detail", function (x, e) {
    current = e
    render(e)
  })

  $(document).on("import.detail.updated", function (x, e) {
    if (current === e && e.response) {
      render(e)
    }
  })

  $(document).on("import.cleared", function (x, e) {
    if (e === current) {
      current = null
      divEditor.hide()
      msgPanel.hide()
      placeholder.show()
    }
  })

  setupSelect()

  $(".import-stats-chart").click(function () {
    if (statsSwitcher.hasClass("stats-visible")) {
      statsSwitcher.removeClass("stats-visible")
    } else {
      statsSwitcher.addClass("stats-visible")
    }
  })

  divTabName.mouseover(function () {
    divTabName
      .addClass("animated tada")
      .one(
        "webkitAnimationEnd mozAnimationEnd MSAnimationEnd oanimationend animationend",
        function () {
          $(this).removeClass("animated").removeClass("tada")
        },
      )
  })

  divTabName.click(function () {
    editorBus.trigger(qdb.MSG_QUERY_FIND_N_EXEC, location)
  })

  $(window).resize(resizeCanvas)
  editorBus.on(qdb.MSG_ACTIVE_PANEL, resizeCanvas)
}
