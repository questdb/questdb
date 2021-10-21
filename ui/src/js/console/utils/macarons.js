/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

const contrastColor = "#eee"
const axisCommon = function () {
  return {
    axisLine: {
      lineStyle: {
        color: contrastColor,
      },
    },
    axisTick: {
      lineStyle: {
        color: contrastColor,
      },
    },
    axisLabel: {
      textStyle: {
        color: contrastColor,
      },
    },
    splitLine: {
      lineStyle: {
        type: "dashed",
        color: "#aaa",
      },
    },
    splitArea: {
      areaStyle: {
        color: contrastColor,
      },
    },
  }
}

const colorPalette = [
  "#dd6b66",
  "#759aa0",
  "#e69d87",
  "#8dc1a9",
  "#ea7e53",
  "#eedd78",
  "#73a373",
  "#73b9bc",
  "#7289ab",
  "#91ca8c",
  "#f49f42",
]

const theme = {
  color: colorPalette,
  backgroundColor: "#282a36",
  tooltip: {
    axisPointer: {
      lineStyle: {
        color: contrastColor,
      },
      crossStyle: {
        color: contrastColor,
      },
    },
  },
  legend: {
    textStyle: {
      color: contrastColor,
    },
  },
  textStyle: {
    color: contrastColor,
  },
  title: {
    textStyle: {
      color: contrastColor,
    },
  },
  toolbox: {
    iconStyle: {
      normal: {
        borderColor: contrastColor,
      },
    },
  },
  dataZoom: {
    textStyle: {
      color: contrastColor,
    },
  },
  timeline: {
    lineStyle: {
      color: contrastColor,
    },
    itemStyle: {
      normal: {
        color: colorPalette[1],
      },
    },
    label: {
      normal: {
        textStyle: {
          color: contrastColor,
        },
      },
    },
    controlStyle: {
      normal: {
        color: contrastColor,
        borderColor: contrastColor,
      },
    },
  },
  timeAxis: axisCommon(),
  logAxis: axisCommon(),
  valueAxis: axisCommon(),
  categoryAxis: axisCommon(),

  line: {
    symbol: "circle",
  },
  graph: {
    color: colorPalette,
  },
  gauge: {
    title: {
      textStyle: {
        color: contrastColor,
      },
    },
  },
  candlestick: {
    itemStyle: {
      normal: {
        color: "#FD1050",
        color0: "#0CF49B",
        borderColor: "#FD1050",
        borderColor0: "#0CF49B",
      },
    },
  },
  textStyle: {
    fontFamily:
      '"Open Sans", -apple-system, BlinkMacSystemFont, Helvetica, Roboto, sans-serif',
  },
}

export default theme
