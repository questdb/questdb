import React from "react"
import styled from "styled-components"

import type { Color } from "types"
import { color } from "utils"

import { Text, textStyles, TextProps } from "../Text"

const defaultProps = Text.defaultProps

type Props = Readonly<{
  hoverColor: Color
  href: string
  rel?: string
  target?: string
}> &
  TextProps

const Wrapper = styled.a<Props>`
  ${textStyles};
  text-decoration: none;
  line-height: 1;

  &:hover:not([disabled]),
  &:focus:not([disabled]) {
    color: ${(props) =>
      props.hoverColor ? color(props.hoverColor) : "inherit"};
  }
`

export const Link = (props: Props) => <Wrapper {...props} />

Link.defaultProps = defaultProps
