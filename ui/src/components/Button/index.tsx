import { darken } from "polished"
import React, { forwardRef, MouseEvent, ReactNode, Ref } from "react"
import styled, { css } from "styled-components"

import type { Color } from "types"
import { color } from "utils"

import { buttonTransition } from "../Transition"

type Size = "sm" | "md"
type Type = "button" | "submit"

const defaultProps: Readonly<{
  children?: ReactNode
  disabled?: boolean
  onClick?: (event: MouseEvent) => void
  size: Size
  type: Type
}> = {
  size: "md",
  type: "button",
}

export type ButtonProps = Partial<typeof defaultProps>

type ThemeShape = {
  background: Color
  border: Color
  color: Color
}

export const getButtonSize = ({ size }: ButtonProps) =>
  size === "sm" ? "2rem" : "3rem"

const baseCss = css<ButtonProps>`
  display: flex;
  height: ${getButtonSize};
  padding: 0 1rem;
  align-items: center;
  background: transparent;
  border-radius: 4px;
  border: 1px solid transparent;
  font-weight: 400;
  outline: 0;
  line-height: 1.15;
  ${buttonTransition};

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
      background: ${({ theme }) => darken(0.1, theme.color[hover.background])};
    }

    &:disabled {
      cursor: not-allowed;
      background: ${color(disabled.background)};
      color: ${color(disabled.color)};
      border-color: ${color(disabled.border)};
    }
  `

const Primary = styled.button<ButtonProps>`
  ${baseCss};
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
  props: ButtonProps,
  ref: Ref<HTMLButtonElement>,
) => <Primary {...props} ref={ref} />

export const PrimaryButton = forwardRef(PrimaryButtonWithRef)

PrimaryButton.defaultProps = defaultProps

const Secondary = styled.button<ButtonProps>`
  ${baseCss};
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
      background: "draculaSelection",
      border: "gray1",
      color: "gray1",
    },
  )};
`

const SecondaryButtonWithRef = (
  props: ButtonProps,
  ref: Ref<HTMLButtonElement>,
) => <Secondary {...props} ref={ref} />

export const SecondaryButton = forwardRef(SecondaryButtonWithRef)

SecondaryButton.defaultProps = defaultProps

const Success = styled.button<ButtonProps>`
  ${baseCss};
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
  props: ButtonProps,
  ref: Ref<HTMLButtonElement>,
) => <Success {...props} ref={ref} />

export const SuccessButton = forwardRef(SuccessButtonWithRef)

SuccessButton.defaultProps = defaultProps

const Error = styled.button<ButtonProps>`
  ${baseCss};
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
  props: ButtonProps,
  ref: Ref<HTMLButtonElement>,
) => <Error {...props} ref={ref} />

export const ErrorButton = forwardRef(ErrorButtonWithRef)

ErrorButton.defaultProps = defaultProps
