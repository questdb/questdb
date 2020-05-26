import type { Color, ThemeShape } from "types"

const color = (color: Color) => (props?: { theme: ThemeShape }) =>
  props ? props.theme.color[color] : undefined

export { color }
