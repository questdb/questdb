import "styled-components"

export type ColorShape = {
  black: string
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
  white: string
}

export type FontSizeShape = {
  ms: string
  xs: string
  sm: string
  md: string
  lg: string
  xl: string
}

export type Color = keyof ColorShape

export type FontSize = keyof FontSizeShape

declare module "styled-components" {
  export interface DefaultTheme {
    baseFontSize: string
    color: ColorShape
    font: string
    fontMonospace: string
    fontSize: FontSizeShape
  }
}
