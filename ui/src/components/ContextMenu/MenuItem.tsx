import React, { PropsWithChildren } from "react"
import styled from "styled-components"
import { MenuItem as ReactMenuItem, MenuItemProps } from "react-contextmenu"
import { color } from "utils"

const StyledMenuItem = styled(ReactMenuItem)`
  color: ${color("draculaForeground")};
  cursor: pointer;
  padding: 0.5rem 1rem;

  &:hover {
    background: ${color("draculaSelection")};
  }
`

const StyledDivider = styled(ReactMenuItem)`
  background: ${color("draculaForeground")};
  height: 1px;
  width: 100%;
  margin: 0.5rem 0;
  padding: 0 1rem;
`

type Props = {} & MenuItemProps

const MenuItem = ({ children, divider, ...rest }: PropsWithChildren<Props>) => {
  const Component = divider ? StyledDivider : StyledMenuItem

  return <Component {...rest}>{children}</Component>
}

export default MenuItem
