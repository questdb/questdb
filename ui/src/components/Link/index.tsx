import React from "react"
import styled from "styled-components"

import { color } from "utils"

import { Text, textCss, TextProps } from "../Text"

const defaultProps = Text.defaultProps

type Props = Readonly<{
  href: string
  rel?: string
  target?: string
}> &
  TextProps

const Wrapper = styled.a<Props>`
  ${textCss};
  text-decoration: none;

  &:hover:not([disabled]),
  &:focus:not([disabled]) {
    color: ${color("draculaCyan")};
  }
`

export const Link = (props: Props) => <Wrapper {...props} />

Link.defaultProps = defaultProps
