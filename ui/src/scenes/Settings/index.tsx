/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

import React, { useCallback, useState } from "react"
import styled from "styled-components"
import Notifications from "./Notifications"
import { PaneContent, PaneMenu, Text, PrimaryToggleButton } from "components"
import { Settings2 } from "@styled-icons/evaicons-solid/Settings2"
import { Popup } from "@styled-icons/entypo/Popup"
import { color } from "utils"

const PaneWrapper = styled.div`
  display: flex;
  flex-direction: column;
  flex: 0;
`

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
            <Popup size="18px" />
            <span>Notification Log</span>
          </ToggleButton>
        </SettingsMenu>
      </PaneWrapper>
      <Content>{selected === "notification" && <Notifications />}</Content>
    </Wrapper>
  )
}

export default Settings
