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

import React, { forwardRef, Ref } from "react"
import styled, { css } from "styled-components"

import type { Color, FontSize } from "types"
import { color } from "utils"

import { ButtonProps, getButtonSize } from "../Button"
import { bezierTransition } from "../Transition"

type Direction = "top" | "right" | "bottom" | "left"

const defaultProps: {
  direction: Direction
  fontSize: FontSize
  selected: boolean
  size: ButtonProps["size"]
  type: ButtonProps["type"]
} = {
  direction: "bottom",
  fontSize: "md",
  selected: false,
  size: "md",
  type: "button",
}

type DefaultProps = typeof defaultProps

type Props = Readonly<{
  direction: Direction
  selected: boolean
}> &
  ButtonProps

type RenderRefProps = Omit<Props, keyof DefaultProps> & Partial<DefaultProps>

type ThemeShape = {
  background: Color
  color: Color
}

const baseStyles = css<Props>`
  display: flex;
  height: ${getButtonSize};
  padding: 0 1rem;
  align-items: center;
  justify-content: center;
  background: transparent;
  border: none;
  outline: 0;
  opacity: ${({ selected }) => (selected ? "1" : "0.5")};
  font-size: ${({ fontSize, theme }) => theme.fontSize[fontSize]};
  font-weight: 400;
  line-height: 1.15;
  ${({ direction }) =>
    `border-${direction || defaultProps.direction}: 3px solid transparent;`};
  ${bezierTransition};
  ${({ disabled }) => disabled && "pointer-events: none;"};

  svg + span {
    margin-left: 1rem;
  }
`

const getTheme = (normal: ThemeShape, hover: ThemeShape) =>
  css<Props>`
    background: ${color(normal.background)};
    color: ${color(normal.color)};
    ${({ direction, selected, theme }) =>
      selected &&
      `border-${direction || defaultProps.direction}-color: ${
        theme.color.draculaPink
      };`};

    &:focus {
      box-shadow: inset 0 0 0 1px ${color("draculaForeground")};
    }

    &:hover:not([disabled]) {
      background: ${color(hover.background)};
      color: ${color(hover.color)};
      opacity: 1;
    }

    &:active:not([disabled]) {
      background: ${color(hover.background)};
      filter: brightness(90%);
    }
  `

const PrimaryToggleButtonStyled = styled.button<Props>`
  ${baseStyles};
  ${getTheme(
    {
      background: "draculaBackgroundDarker",
      color: "draculaForeground",
    },
    {
      background: "draculaComment",
      color: "draculaForeground",
    },
  )};
`

const PrimaryToggleButtonWithRef = (
  props: RenderRefProps,
  ref: Ref<HTMLButtonElement>,
) => <PrimaryToggleButtonStyled {...defaultProps} {...props} ref={ref} />

export const PrimaryToggleButton = forwardRef(PrimaryToggleButtonWithRef)

PrimaryToggleButton.defaultProps = defaultProps
