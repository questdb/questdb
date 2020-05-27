import React, { ReactNode } from "react"
import styled from "styled-components"

import { color } from "utils"

type Props = Readonly<{
  children: ReactNode
  className?: string
}>

const Wrapper = styled.div`
  display: flex;
  flex-direction: column;
  flex: 1;
  background: ${color("draculaBackground")};
  overflow: auto;
`

export const Pane = ({ children, className }: Props) => (
  <Wrapper className={className}>{children}</Wrapper>
)
