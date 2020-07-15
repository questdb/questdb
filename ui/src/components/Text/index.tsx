import { ReactNode } from "react"
import styled, { css } from "styled-components"

import type { Color, FontSize } from "types"
import { color } from "utils"

type FontStyle = "normal" | "italic"
type Transform = "capitalize" | "lowercase" | "uppercase"

export type TextProps = Readonly<{
  _style?: FontStyle
  align?: "left" | "right" | "center"
  className?: string
  code?: boolean
  color?: Color
  children?: ReactNode
  ellipsis?: boolean
  size?: FontSize
  transform?: Transform
  weight?: number
}>

const defaultProps: Readonly<{
  color: Color
}> = {
  color: "black",
}

const ellipsisStyles = css`
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`

export const textStyles = css<TextProps>`
  color: ${(props) => (props.color ? color(props.color) : "inherit")};
  font-family: ${({ code, theme }) => code && theme.fontMonospace};
  font-size: ${({ size, theme }) => (size ? theme.fontSize[size] : "inherit")};
  font-style: ${({ _style }) => _style || "inherit"};
  font-weight: ${({ weight }) => weight};
  text-transform: ${({ transform }) => transform};
  ${({ align }) => (align ? `text-align: ${align}` : "")};
  ${({ ellipsis }) => ellipsis && ellipsisStyles};
`

export const Text = styled.span<TextProps>`
  ${textStyles};
`

Text.defaultProps = defaultProps
