import React, { ReactNode, Ref } from "react"
import styled, { css } from "styled-components"

import { Text } from "components"
import { color } from "utils"

type Props = Readonly<{
  arrow?: {
    setArrowElement: Ref<HTMLDivElement>
    styles?: Record<string, string>
  }
  children: ReactNode
}>

const tooltipCss = css`
  position: absolute;
  width: 8px;
  height: 8px;
`

export const TooltipArrow = styled.div`
  ${tooltipCss};

  &::before {
    ${tooltipCss};
    top: 0;
    left: 0;
    content: "";
    transform: rotate(45deg);
    background: ${color("draculaForeground")};
    border: 1px solid ${color("draculaBackgroundDarker")};
    border-radius: 0.25rem;
  }
`

export const Wrapper = styled.div`
  position: relative;
  max-width: 260px;
  padding: 0.25rem 1rem;
  background: ${color("draculaForeground")};
  border: 1px solid ${color("draculaBackgroundDarker")};
  border-radius: 0.25rem;

  &[data-popper-placement^="right"] ${TooltipArrow} {
    left: -5px;

    &::before {
      border-right: none;
      border-top: none;
    }
  }

  &[data-popper-placement^="left"] ${TooltipArrow} {
    right: -5px;

    &::before {
      border-left: none;
      border-bottom: none;
    }
  }

  &[data-popper-placement^="top"] ${TooltipArrow} {
    bottom: -5px;

    &::before {
      border-left: none;
      border-top: none;
    }
  }

  &[data-popper-placement^="bottom"] ${TooltipArrow} {
    top: -5px;

    &::before {
      border-right: none;
      border-bottom: none;
    }
  }
`

export const Tooltip = ({ arrow, children, ...rest }: Props) => (
  <Wrapper {...rest}>
    <Text color="black">{children}</Text>
    {arrow && <TooltipArrow ref={arrow.setArrowElement} style={arrow.styles} />}
  </Wrapper>
)
