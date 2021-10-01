import React from "react"
import styled from "styled-components"
import Toggler from "./Toggler"
import { color } from "utils"

const Wrapper = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  align-items: flex-start;
  width: 100%;
`

const SettingGroup = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: flex-start;
  align-items: center;
  width: 100%;
  padding: 2rem;
  background: ${color("draculaBackground")};

  &:not(:last-child) {
    border-bottom: 1px dotted ${color("draculaSelection")};
  }
`
const SettingControl = styled.div`
  width: 250px;
`

const Notifications = () => (
  <Wrapper>
    <SettingGroup>
      <SettingControl>
        <Toggler />
      </SettingControl>
    </SettingGroup>
  </Wrapper>
)

export default Notifications
