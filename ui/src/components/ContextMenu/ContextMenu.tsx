import React, { PropsWithChildren } from "react"
import styled from "styled-components"
import {
  ContextMenu as ReactContextMenu,
  ContextMenuProps,
} from "react-contextmenu"
import { color } from "utils"

const StyledContextMenu = styled(ReactContextMenu)`
  z-index: 100;
  background: ${color("draculaBackgroundDarker")};
  border: 1px solid ${color("draculaSelection")};
  border-radius: 4px;
`

type Props = {} & ContextMenuProps

const MenuItem = ({ children, ...rest }: PropsWithChildren<Props>) => (
  <StyledContextMenu {...rest}>{children}</StyledContextMenu>
)

export default MenuItem
