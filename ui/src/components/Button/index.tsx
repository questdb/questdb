import { MouseEvent, ReactNode } from "react"
import styled, { css } from "styled-components"

import type { Color } from "types"
import { color } from "utils"

import { bezierTransition } from "../Transition"

type Size = "sm" | "md"
type Type = "button" | "submit"

const size: Size = "md"
const type: Type = "button"

const defaultProps = {
  size,
  type,
}

export type ButtonProps = Readonly<{
  children?: ReactNode
  className?: string
  disabled?: boolean
  onClick?: (event: MouseEvent) => void
  size?: Size
  type?: Type
}>

type ThemeShape = {
  background: Color
  border: Color
  color: Color
}

export const getButtonSize = ({ size }: ButtonProps) =>
  size === "sm" ? "2rem" : "3rem"

const baseStyles = css<ButtonProps>`
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
  ${bezierTransition};
  ${({ disabled }) => disabled && "pointer-events: none;"};

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

export const PrimaryButton = styled.button<ButtonProps>`
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

PrimaryButton.defaultProps = defaultProps

export const SecondaryButton = styled.button<ButtonProps>`
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
      background: "draculaSelection",
      border: "gray1",
      color: "gray1",
    },
  )};
`

SecondaryButton.defaultProps = defaultProps

export const SuccessButton = styled.button<ButtonProps>`
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

SuccessButton.defaultProps = defaultProps

export const ErrorButton = styled.button<ButtonProps>`
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

ErrorButton.defaultProps = defaultProps

export const TransparentButton = styled.button<ButtonProps>`
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

TransparentButton.defaultProps = defaultProps
