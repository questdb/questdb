import React from "react"
import styled from "styled-components"
import Notifications from "./Notifications"
import { Text } from "components"
import { Settings2 } from "@styled-icons/evaicons-solid/Settings2"
import { color } from "utils"

const Wrapper = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  align-items: flex-start;
`
const Header = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: flex-start;
  align-items: center;
  width: 100%;
  padding: 1rem;

  & > :last-child:not(:first-child) {
    margin-left: 0.5rem;
  }
`
const Icon = styled(Settings2)`
  color: ${color("draculaForeground")};
`
const SettingsWrapper = styled.div`
  width: 50%;
`

const Settings = () => (
  <Wrapper>
    <Header>
      <Icon size="20px" />
      <Text color="draculaForeground">Settings</Text>
    </Header>
    <SettingsWrapper>
      <Notifications />
    </SettingsWrapper>
  </Wrapper>
)

export default Settings
