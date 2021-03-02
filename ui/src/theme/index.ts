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

import type { DefaultTheme as DefaultThemeShape } from "styled-components"

import type { ColorShape, FontSizeShape } from "types"

const color: ColorShape = {
  black: "#191a21",
  blackAlpha40: "rgba(25, 26, 33, 0.4)",
  gray1: "#585858",
  gray2: "#bbbbbb",
  draculaBackgroundDarker: "#21222c",
  draculaBackground: "#282a36",
  draculaForeground: "#f8f8f2",
  draculaSelection: "#44475a",
  draculaComment: "#6272a4",
  draculaRed: "#ff5555",
  draculaOrange: "#ffb86c",
  draculaYellow: "#f1fa8c",
  draculaGreen: "#50fa7b",
  draculaPurple: "#bd93f9",
  draculaCyan: "#8be9fd",
  draculaPink: "#ff79c6",
  transparent: "transparent",
  white: "#fafafa",
}

const fontSize: FontSizeShape = {
  ms: "1rem",
  xs: "1.2rem",
  sm: "1.3rem",
  md: "1.4rem",
  lg: "1.5rem",
  xl: "1.7rem",
  hg: "3rem",
}

export const theme: DefaultThemeShape = {
  baseFontSize: "10px",
  color,
  font:
    '"Open Sans", -apple-system, BlinkMacSystemFont, Helvetica, Roboto, sans-serif',

  fontEmoji:
    '"apple color emoji", "segoe ui emoji", "android emoji", "emojisymbols", "emojione mozilla", "twemoji mozilla", "segoe ui symbol", "noto color emoji"',
  fontMonospace:
    'SFMono-Regular, Menlo, Monaco, Consolas,"Liberation Mono", "Courier New", monospace',
  fontSize,
}

export type ThemeShape = typeof theme
