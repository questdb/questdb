import { darken } from "polished"
import React, { MouseEvent, ReactNode } from "react"
import styled, { css } from "styled-components"

import { theme } from "theme"
import { Color } from "types"
import { color } from "utils"

const defaultProps = {
  size: "md",
  type: "button",
}

type Props = {
  children: ReactNode
  disabled?: boolean
  onClick?: (event: MouseEvent) => void
  size: "sm" | "md"
  type: "button" | "submit"
}

const getSize = ({ size }: Pick<Props, "size">) => {
  if (size === "sm") {
    return "2rem"
  }

  return "3rem"
}

const baseCss = css`
  display: flex;
  height: ${getSize};
  padding: 0 1rem;
  align-items: center;
  background: transparent;
  border-radius: 0.25rem;
  border: 1px solid transparent;
  font-weight: 400;
  outline: 0;
  line-height: 1.15;
  transition: all 70ms cubic-bezier(0, 0, 0.38, 0.9);

  svg {
    margin-right: 0.5rem;
  }
`

type ButtonThemeShape = {
  background: Color
  border: Color
  color: Color
}

const getTheme = (
  normal: ButtonThemeShape,
  hover: ButtonThemeShape,
  disabled: ButtonThemeShape,
) => {
  return css`
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
      background: ${darken(0.1, theme.color[hover.background])};
    }

    &:disabled {
      cursor: not-allowed;
      background: ${color(disabled.background)};
      color: ${color(disabled.color)};
      border-color: ${color(disabled.border)};
    }
  `
}
const Primary = styled.button<Props>`
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

export const PrimaryButton = ({ children, ...rest }: Props) => {
  return <Primary {...rest}>{children}</Primary>
}

PrimaryButton.defaultProps = defaultProps

const Secondary = styled.button<Props>`
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

export const SecondaryButton = ({ children, ...rest }: Props) => {
  return <Secondary {...rest}>{children}</Secondary>
}

SecondaryButton.defaultProps = defaultProps
