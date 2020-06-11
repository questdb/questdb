import { darken } from "polished"
import styled, { css } from "styled-components"

import type { Color } from "types"
import { color } from "utils"

import { ButtonProps, getButtonSize, PrimaryButton } from "../Button"
import { bezierTransition } from "../Transition"

type Direction = "top" | "right" | "bottom" | "left"

const direction: Direction = "bottom"

const defaultProps = {
  ...PrimaryButton.defaultProps,
  direction,
  selected: false,
}

type Props = Readonly<{
  direction?: Direction
  selected?: boolean
}> &
  ButtonProps

type ThemeShape = {
  background: Color
  color: Color
}

const baseCss = css<Props>`
  display: flex;
  height: ${getButtonSize};
  padding: 0 1rem;
  align-items: center;
  justify-content: center;
  background: transparent;
  font-weight: 400;
  outline: 0;
  line-height: 1.15;
  opacity: ${({ selected }) => (selected ? "1" : "0.5")};
  border: none;
  ${({ direction }) =>
    `border-${direction || defaultProps.direction}: 2px solid transparent;`};
  ${bezierTransition};

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
      background: ${({ theme }) => darken(0.1, theme.color[hover.background])};
    }
  `

export const PrimaryToggleButton = styled.button<Props>`
  ${baseCss};
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

PrimaryToggleButton.defaultProps = defaultProps
