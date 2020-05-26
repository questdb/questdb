import React from "react"
import styled from "styled-components"
import { Database } from "@styled-icons/entypo"

import { Text } from "components"
import { color } from "utils"

const Wrapper = styled.div`
  position: relative;
  display: flex;
  padding: 1rem;
  background: ${color("draculaBackgroundDarker")};
  box-shadow: 0 6px 6px -6px ${color("black")};
  border-bottom: 1px solid ${color("black")};
`

const DatabaseIcon = styled(Database)`
  margin-right: 1rem;
`

const Header = () => {
  return (
    <Wrapper>
      <Text color="draculaForeground">
        <DatabaseIcon size="18px" />
        Tables
      </Text>
    </Wrapper>
  )
}

export default Header
