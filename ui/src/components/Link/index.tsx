import React from "react"
import styled from "styled-components"

import { color } from "utils"

import { Text, textStyles, TextProps } from "../Text"

const defaultProps = Text.defaultProps

type Props = Readonly<{
  href: string
  rel?: string
  target?: string
}> &
  TextProps

const Wrapper = styled.a<Props>`
  ${textStyles};
  text-decoration: none;

  &:hover:not([disabled]),
  &:focus:not([disabled]) {
    color: ${color("draculaCyan")};
  }
`

export const Link = (props: Props) => <Wrapper {...props} />

Link.defaultProps = defaultProps
