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

import React, { ChangeEvent, useCallback, useContext } from "react"
import styled from "styled-components"
import { Input, Text } from "components"
import { LocalStorageContext } from "providers/LocalStorageProvider"
import { StoreKey } from "utils/localStorage/types"

const Wrapper = styled.div`
  & > :last-child:not(:first-child) {
    margin-left: 1rem;
  }
`

const Duration = () => {
  const { notificationDelay, updateSettings } = useContext(LocalStorageContext)

  const handleChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      updateSettings(StoreKey.NOTIFICATION_DELAY, e.target.value)
    },
    [updateSettings],
  )

  return (
    <Wrapper>
      <Input
        max={60}
        min={1}
        onChange={handleChange}
        step="1"
        style={{ width: "60px" }}
        type="number"
        value={notificationDelay}
      />
      <Text color="draculaForeground">seconds</Text>
    </Wrapper>
  )
}

export default Duration
