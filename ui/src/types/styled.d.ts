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

import "styled-components"

export type ColorShape = {
  black: string
  blackAlpha40: string
  gray1: string
  gray2: string
  draculaBackgroundDarker: string
  draculaBackground: string
  draculaForeground: string
  draculaSelection: string
  draculaComment: string
  draculaRed: string
  draculaOrange: string
  draculaYellow: string
  draculaGreen: string
  draculaPurple: string
  draculaCyan: string
  draculaPink: string
  transparent: string
  white: string
}

export type FontSizeShape = {
  ms: string
  xs: string
  sm: string
  md: string
  lg: string
  xl: string
  hg: string
}

export type Color = keyof ColorShape

export type FontSize = keyof FontSizeShape

declare module "styled-components" {
  export interface DefaultTheme {
    baseFontSize: string
    color: ColorShape
    font: string
    fontEmoji: string
    fontMonospace: string
    fontSize: FontSizeShape
  }
}
