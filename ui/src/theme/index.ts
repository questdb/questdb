import type { DefaultTheme as DefaultThemeShape } from "styled-components"

import type { ColorShape, FontSizeShape } from "types"

const color: ColorShape = {
  black: "#191a21",
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
  lg: "1.5rem",
  md: "1.4rem",
  sm: "1.3rem",
  xl: "1.7rem",
  xs: "1.2rem",
  ms: "1rem",
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
