import { ReactNode } from "react"
import styled from "styled-components"

import { color } from "utils"

type Props = Readonly<{
  children: ReactNode
  className?: string
}>

export const PaneMenu = styled.div`
  position: relative;
  display: flex;
  height: 41px;
  padding: 0 1rem;
  align-items: center;
  background: ${color("draculaBackgroundDarker")};
  box-shadow: 0 6px 6px -6px ${color("black")};
  border-bottom: 1px solid ${color("black")};
  border-top: 1px solid transparent;
  z-index: 5;
`
