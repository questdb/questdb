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

import React, { ComponentProps, forwardRef, Ref } from "react"
import styled from "styled-components"

import { ButtonProps, getButtonSize } from "components"
import { color } from "utils"

type Type = "text" | "number"

const defaultProps: { size: ButtonProps["size"]; type: Type } = {
  size: "md",
  type: "text",
}

type Props = Readonly<{
  id?: string
  placeholder?: string
  size: ButtonProps["size"]
  title?: string
  type: Type
}>

const InputStyled = styled.input<Props>`
  height: ${getButtonSize};
  border: none;
  padding: 0 1rem;
  line-height: 1.5;
  outline: none;
  background: ${color("draculaSelection")};
  border-radius: 4px;
  color: ${color("draculaForeground")};

  &:focus {
    box-shadow: inset 0 0 0 1px ${color("draculaForeground")};
  }

  &::placeholder {
    color: ${color("gray2")};
  }
`

const InputWithRef = (
  props: ComponentProps<typeof InputStyled>,
  ref: Ref<HTMLInputElement>,
) => <InputStyled {...defaultProps} {...props} ref={ref} />

export const Input = forwardRef(InputWithRef)

Input.defaultProps = defaultProps
