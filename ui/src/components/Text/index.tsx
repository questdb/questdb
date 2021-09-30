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

import React, { ReactNode } from "react"
import styled, { css } from "styled-components"

import type { Color, FontSize } from "types"
import { color } from "utils"

type FontStyle = "normal" | "italic"
type Transform = "capitalize" | "lowercase" | "uppercase"
type Type = "span" | "label"

export type TextProps = Readonly<{
  _style?: FontStyle
  align?: "left" | "right" | "center"
  className?: string
  code?: boolean
  color?: Color
  children?: ReactNode
  ellipsis?: boolean
  htmlFor?: string
  onClick?: () => void
  size?: FontSize
  transform?: Transform
  type?: Type
  weight?: number
}>

const defaultProps: Readonly<{
  color: Color
  type: Type
}> = {
  color: "black",
  type: "span",
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
  font-style: ${({ _style }) => _style ?? "inherit"};
  font-weight: ${({ weight }) => weight};
  text-transform: ${({ transform }) => transform};
  ${({ align }) => (align ? `text-align: ${align}` : "")};
  ${({ ellipsis }) => ellipsis && ellipsisStyles};
`

const TextStyled = styled.label<TextProps>`
  ${textStyles};
`

export const Text = ({ type, ...rest }: TextProps) => {
  return <TextStyled as={type} {...rest} type={type} />
}

Text.defaultProps = defaultProps
