import { ReactNode } from "react"
import styled from "styled-components"

import { color } from "utils"

type Props = Readonly<{
  children: ReactNode
  className?: string
}>

export const PaneContent = styled.div<Props>`
  display: flex;
  flex-direction: column;
  flex: 1;
  background: ${color("draculaBackground")};
  overflow: auto;
`
