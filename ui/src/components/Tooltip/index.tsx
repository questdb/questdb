import React, { ReactNode } from "react"
import styled from "styled-components"

import { Text } from "components"
import { color } from "utils"

type Props = Readonly<{
  children: ReactNode
}>

const Wrapper = styled.div`
  position: relative;
  max-width: 260px;
  padding: 1rem;
  background: ${color("black")};
  border: 1px solid rgba(255, 255, 255, 0.15);
`

const Tooltip = ({ children }: Props) => {
  return (
    <Wrapper>
      <Text color="white">{children}</Text>
    </Wrapper>
  )
}

export { Tooltip }
