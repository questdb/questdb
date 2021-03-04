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

import React, { forwardRef, MouseEvent, ReactNode, Ref } from "react"
import styled, { css } from "styled-components"

import type { Color, FontSize } from "types"
import { color } from "utils"

import { bezierTransition } from "../Transition"

type Size = "sm" | "md" | "lg"
type Type = "button" | "submit"

const defaultProps: { fontSize: FontSize; size: Size; type: Type } = {
  fontSize: "md",
  size: "md",
  type: "button",
}

type DefaultProps = typeof defaultProps

type RenderRefProps = Omit<ButtonProps, keyof DefaultProps> &
  Partial<DefaultProps>

export type ButtonProps = Readonly<{
  children?: ReactNode
  className?: string
  disabled?: boolean
  fontSize: FontSize
  onClick?: (event: MouseEvent) => void
  size: Size
  type: Type
}>

type ThemeShape = {
  background: Color
  border: Color
  color: Color
}

export const getButtonSize = ({ size }: { size: ButtonProps["size"] }) => {
  switch (size) {
    case "sm":
      return "2rem"
    case "md":
      return "3rem"
    case "lg":
      return "5rem"
    default:
      return "3rem"
  }
}

const baseStyles = css<ButtonProps>`
  display: flex;
  height: ${getButtonSize};
  padding: 0 1rem;
  align-items: center;
  justify-content: center;
  background: transparent;
  border-radius: 4px;
  border: 1px solid transparent;
  outline: 0;
  font-size: ${({ fontSize, theme }) => theme.fontSize[fontSize]};
  font-weight: 400;
  line-height: 1.15;
  ${bezierTransition};

  svg + span {
    margin-left: 0.5rem;
  }
`

const getTheme = (
  normal: ThemeShape,
  hover: ThemeShape,
  disabled: ThemeShape,
) =>
  css<ButtonProps>`
    background: ${color(normal.background)};
    color: ${color(normal.color)};
    border-color: ${color(normal.border)};

    &:focus {
      box-shadow: inset 0 0 0 1px ${color("draculaForeground")};
    }

    &:hover:not([disabled]) {
      background: ${color(hover.background)};
      color: ${color(hover.color)};
      border-color: ${color(hover.border)};
    }

    &:active:not([disabled]) {
      background: ${color(hover.background)};
      filter: brightness(90%);
    }

    &:disabled {
      cursor: not-allowed;
      background: ${color(disabled.background)};
      color: ${color(disabled.color)};
      border-color: ${color(disabled.border)};
    }
  `

const PrimaryButtonStyled = styled.button<ButtonProps>`
  ${baseStyles};
  ${getTheme(
    {
      background: "draculaSelection",
      border: "draculaSelection",
      color: "draculaForeground",
    },
    {
      background: "draculaComment",
      border: "draculaSelection",
      color: "draculaForeground",
    },
    {
      background: "draculaSelection",
      border: "gray1",
      color: "gray1",
    },
  )};
`

const PrimaryButtonWithRef = (
  props: RenderRefProps,
  ref: Ref<HTMLButtonElement>,
) => <PrimaryButtonStyled {...defaultProps} {...props} ref={ref} />

export const PrimaryButton = forwardRef(PrimaryButtonWithRef)

PrimaryButton.defaultProps = defaultProps

const SecondaryButtonStyled = styled.button<ButtonProps>`
  ${baseStyles};
  ${getTheme(
    {
      background: "draculaBackground",
      border: "draculaBackground",
      color: "draculaForeground",
    },
    {
      background: "draculaComment",
      border: "draculaComment",
      color: "draculaForeground",
    },
    {
      background: "gray1",
      border: "gray1",
      color: "draculaBackground",
    },
  )};
`

const SecondaryButtonWithRef = (
  props: RenderRefProps,
  ref: Ref<HTMLButtonElement>,
) => <SecondaryButtonStyled {...defaultProps} {...props} ref={ref} />

export const SecondaryButton = forwardRef(SecondaryButtonWithRef)

SecondaryButton.defaultProps = defaultProps

const SuccessButtonStyled = styled.button<ButtonProps>`
  ${baseStyles};
  ${getTheme(
    {
      background: "draculaSelection",
      border: "draculaSelection",
      color: "draculaGreen",
    },
    {
      background: "draculaComment",
      border: "draculaSelection",
      color: "draculaGreen",
    },
    {
      background: "draculaSelection",
      border: "gray1",
      color: "gray1",
    },
  )};
`

const SuccessButtonWithRef = (
  props: RenderRefProps,
  ref: Ref<HTMLButtonElement>,
) => <SuccessButtonStyled {...defaultProps} {...props} ref={ref} />

export const SuccessButton = forwardRef(SuccessButtonWithRef)

SuccessButton.defaultProps = defaultProps

const ErrorButtonStyled = styled.button<ButtonProps>`
  ${baseStyles};
  ${getTheme(
    {
      background: "draculaSelection",
      border: "draculaSelection",
      color: "draculaRed",
    },
    {
      background: "draculaComment",
      border: "draculaSelection",
      color: "draculaRed",
    },
    {
      background: "draculaSelection",
      border: "gray1",
      color: "gray1",
    },
  )};
`

const ErrorButtonWithRef = (
  props: RenderRefProps,
  ref: Ref<HTMLButtonElement>,
) => <ErrorButtonStyled {...defaultProps} {...props} ref={ref} />

export const ErrorButton = forwardRef(ErrorButtonWithRef)

ErrorButton.defaultProps = defaultProps

const TransparentButtonStyled = styled.button<ButtonProps>`
  ${baseStyles};
  ${getTheme(
    {
      background: "transparent",
      border: "transparent",
      color: "draculaForeground",
    },
    {
      background: "draculaComment",
      border: "transparent",
      color: "draculaForeground",
    },
    {
      background: "draculaSelection",
      border: "gray1",
      color: "gray1",
    },
  )};
`

const TransparentButtonWithRef = (
  props: RenderRefProps,
  ref: Ref<HTMLButtonElement>,
) => <TransparentButtonStyled {...defaultProps} {...props} ref={ref} />

export const TransparentButton = forwardRef(TransparentButtonWithRef)

TransparentButton.defaultProps = defaultProps
