import React, { useCallback, useState } from "react"
import styled from "styled-components"
import Notifications from "./Notifications"
import {
  PaneContent,
  PaneMenu,
  PaneWrapper,
  Text,
  PrimaryToggleButton,
} from "components"
import { Settings2 } from "@styled-icons/evaicons-solid/Settings2"
import { Notification } from "@styled-icons/entypo/Notification"
import { color } from "utils"

const Wrapper = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  align-items: flex-start;
`
const HeaderWrapper = styled(PaneWrapper)`
  width: 100%;
`
const HeaderMenu = styled(PaneMenu)`
  & > :not(:first-child) {
    margin-left: 1rem;
  }
`
const Info = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: flex-start;
  align-items: center;
  width: 100%;
  padding: 2rem;
`
const Icon = styled(Settings2)`
  color: ${color("draculaForeground")};
`
const SettingsMenu = styled(PaneWrapper)`
  width: 100%;
  padding: 0 10px;
`
const ToggleButton = styled(PrimaryToggleButton)`
  height: 4rem;
  padding: 0 1rem;
`
const Content = styled(PaneContent)`
  color: ${color("draculaForeground")};

  *::selection {
    background: ${color("draculaRed")};
    color: ${color("draculaForeground")};
  }
`

const Settings = () => {
  const [selected, setSelected] = useState<"notification">("notification")
  const handleNotificationClick = useCallback(() => {
    setSelected("notification")
  }, [])

  return (
    <Wrapper>
      <HeaderWrapper>
        <HeaderMenu>
          <Icon size="20px" />
          <Text color="draculaForeground">Settings</Text>
        </HeaderMenu>
      </HeaderWrapper>
      <Info>
        <Text color="draculaForeground">
          On this page, you can customize your Quest DB console
        </Text>
      </Info>
      <PaneWrapper>
        <SettingsMenu>
          <ToggleButton
            onClick={handleNotificationClick}
            selected={selected === "notification"}
          >
            <Notification size="18px" />
            <span>Notification</span>
          </ToggleButton>
        </SettingsMenu>
      </PaneWrapper>
      <Content>{selected === "notification" && <Notifications />}</Content>
    </Wrapper>
  )
}

export default Settings
