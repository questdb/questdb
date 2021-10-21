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
