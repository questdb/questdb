import echarts from "echarts/lib/echarts"
import $ from "jquery"
import SlimSelect from "slim-select"

import * as qdb from "./globals"
import eChartsMacarons from "./utils/macarons"

//
// this code is taken from:
// https://stackoverflow.com/questions/7837456/how-to-compare-arrays-in-javascript
//

// attach the .equals method to Array's prototype to call it on any array
function arrayEquals(left, right) {
  // if the other array is a falsy value, return
  if (!left || !right) {
    return false
  }

  // compare lengths - can save a lot of time
  if (left.length !== right.length) {
    return false
  }

  for (var i = 0, l = left.length; i < l; i++) {
    // Check if we have nested arrays
    if (left[i] instanceof Array && right[i] instanceof Array) {
      // recurse into the nested arrays
      if (!left[i].equals(right[i])) {
        return false
      }
    } else if (left[i] !== right[i]) {
      // Warning - two different object instances will never be equal: {x:20} != {x:20}
      return false
    }
  }
  return true
}

// end of copy paste

$.fn.quickVis = function (msgBus) {
  var defaults = {
    minColumnWidth: 60,
    rowHeight: 28,
    divCacheSize: 128,
    viewportHeight: 400,
    yMaxThreshold: 10000000,
    maxRowsToAnalyze: 100,
    bottomMargin: 75,
    minVpHeight: 120,
    minDivHeight: 160,
  }
  var bus = msgBus
  var div = $(this)
  const btnDraw = $("#_qvis_frm_draw")
  var viewport
  var echart
  var query
  var queryExecutionTimestamp
  var xAxis
  var yAxis
  var chartType
  const columnSet = new Set()
  const blankChartOptions = {
    title: {},
    tooltip: {},
    legend: {
      data: ["Series"],
    },
    xAxis: {
      data: [],
    },
    yAxis: {},
    series: [
      {
        name: "Y-axis",
        type: "bar",
        data: [],
      },
    ],
  }

  var cachedResponse
  var cachedQuery
  var hActiveRequest

  const chartTypePicker = new SlimSelect({
    select: "#_qvis_frm_chart_type",
  })

  const xAxisPicker = new SlimSelect({
    select: "#_qvis_frm_axis_x",
  })

  const yAxisPicker = new SlimSelect({
    select: "#_qvis_frm_axis_y",
  })

  function resize() {
    echart.resize()
  }

  function addToSet(array, set) {
    for (var i = 0; i < array.length; i++) {
      set.add(array[i])
    }
  }

  function setDrawBtnToCancel() {
    btnDraw.html('<i class="fa fa-stop"></i><span>Cancel</span>')
    btnDraw.removeClass("js-chart-draw").addClass("js-chart-cancel")
  }

  function setDrawBtnToDraw() {
    btnDraw.html('<i class="fa fa-play"></i><span>Draw</span>')
    btnDraw.removeClass("js-chart-cancel").addClass("js-chart-draw")
  }

  // draw server response
  function draw(r) {
    try {
      let i
      // create column name to index map
      const columns = r.columns
      const dataset = r.dataset
      if (columns && dataset) {
        const map = new Map()
        for (i = 0; i < columns.length; i++) {
          map.set(columns[i].name, i)
        }

        // prepare x-axis, there can only be one
        let optionXAxis
        if (xAxis != null) {
          let xAxisDataIndex = map.get(xAxis)
          // x-axis data
          const data = []
          for (i = 0; i < dataset.length; i++) {
            data[i] = dataset[i][xAxisDataIndex]
          }

          optionXAxis = {
            type: "category",
            name: xAxis,
            data: data,
          }
        } else {
          optionXAxis = {}
        }

        let series = []
        // prepare series data
        if (yAxis.length > 0) {
          for (i = 0; i < yAxis.length; i++) {
            const columnIndex = map.get(yAxis[i])
            if (columnIndex) {
              let seriesData = []
              for (let j = 0; j < dataset.length; j++) {
                seriesData[j] = dataset[j][columnIndex]
              }

              if (chartType === "area") {
                series[i] = {
                  type: "line",
                  name: yAxis[i],
                  data: seriesData,
                  areaStyle: {},
                  smooth: true,
                  symbol: "none",
                }
              } else {
                series[i] = {
                  name: yAxis[i],
                  type: chartType,
                  data: seriesData,
                  large: true,
                }
              }
            }
          }
        }
        const option = {
          legend: {},
          xAxis: optionXAxis,
          yAxis: {
            type: "value",
          },
          series: series,
        }
        echart.setOption(option, true)
      }
    } finally {
      resize()
      setDrawBtnToDraw()
    }
  }

  function handleServerResponse(r) {
    hActiveRequest = null
    bus.trigger(qdb.MSG_QUERY_OK, {
      delta: new Date().getTime() - queryExecutionTimestamp,
      count: r.count,
    })
    cachedResponse = r
    cachedQuery = query
    draw(r)
  }

  function handleServerError(r) {
    hActiveRequest = null
    setDrawBtnToDraw()
    bus.trigger(qdb.MSG_QUERY_ERROR, {
      query: cachedQuery,
      r: r.responseJSON,
      status: r.status,
      statusText: r.statusText,
      delta: new Date().getTime() - queryExecutionTimestamp,
    })
  }

  function executeQueryAndDraw() {
    setDrawBtnToCancel()
    chartType = chartTypePicker.selected()

    // check if the only change is chart type
    const selectedXAxis = xAxisPicker.selected()
    const selectedYAxis = yAxisPicker.selected()

    if (
      arrayEquals(selectedXAxis, xAxis) &&
      arrayEquals(selectedYAxis, yAxis) &&
      query === cachedQuery
    ) {
      draw(cachedResponse)
    } else {
      // create set of unique column names that are necessary to build a chart
      columnSet.clear()

      // copy axis columns to set to remove duplicate column names
      // also make a copy of selected fields in case user updates controls while query is being executed
      xAxis = xAxisPicker.selected()
      if (xAxis) {
        columnSet.add(xAxis)
      }

      yAxis = yAxisPicker.selected()
      addToSet(yAxisPicker.selected(), columnSet)

      // expand the set into HTTP query parameter value
      // we only need columns used in chart rather than all columns in the result set
      var urlColumns = ""
      columnSet.forEach(function (value) {
        if (urlColumns !== "") {
          urlColumns += ","
        }
        urlColumns += value
      })

      const requestParams = {}
      requestParams.query = query
      requestParams.count = false
      requestParams.cols = urlColumns
      requestParams.src = "vis"
      // time the query because control that displays query success expected time delta
      queryExecutionTimestamp = new Date().getTime()
      hActiveRequest = $.get("/exec", requestParams)
      bus.trigger(qdb.MSG_QUERY_RUNNING)
      hActiveRequest.done(handleServerResponse).fail(handleServerError)
    }
  }

  function clearChart() {
    echart.setOption(blankChartOptions, true)
  }

  function updatePickers(e, data) {
    var x = []
    const columns = data.columns
    for (var i = 0; i < columns.length; i++) {
      x[i] = { text: columns[i].name }
    }
    xAxisPicker.setData(x)
    yAxisPicker.setData(x)

    yAxisPicker.set(x.slice(1).map((item) => item.text))

    // stash query text so that we can use this later to server for chart column values
    query = data.query
    clearChart()
  }

  function cancelDraw() {
    if (hActiveRequest) {
      hActiveRequest.abort()
      hActiveRequest = null
    }
  }

  function btnDrawClick() {
    if (hActiveRequest) {
      bus.trigger(qdb.MSG_QUERY_CANCEL)
    } else {
      bus.trigger(qdb.MSG_CHART_DRAW)
    }
    return false
  }

  function bind() {
    viewport = div.find(".quick-vis-canvas")[0]
    $(window).resize(resize)
    bus.on(qdb.MSG_ACTIVE_PANEL, resize)
    echart = echarts.init(viewport, eChartsMacarons)
    bus.on(qdb.MSG_QUERY_DATASET, updatePickers)
    bus.on(qdb.MSG_QUERY_CANCEL, cancelDraw)
    bus.on(qdb.MSG_CHART_DRAW, executeQueryAndDraw)
    btnDraw.click(btnDrawClick)
    clearChart()
  }

  bind()
  resize()
}
