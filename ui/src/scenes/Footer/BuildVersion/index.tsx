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

import { QuestContext } from "providers"
import React, { useCallback, useContext, useEffect, useState } from "react"
import styled from "styled-components"
import * as QuestDB from "utils/questdb"
import { ClipboardCopy } from "@styled-icons/heroicons-outline/ClipboardCopy"
import { SecondaryButton } from "components"
import { formatVersion } from "./services"

const Wrapper = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: flex-start;
  align-items: center;

  & > :not(:last-child) {
    margin-right: 1rem;
  }
`
const CopyButton = styled(SecondaryButton)`
  & > :not(:last-child) {
    margin-right: 1rem;
  }
`

const BuildVersion = () => {
  const { quest } = useContext(QuestContext)
  const [buildVersion, setBuildVersion] = useState("")

  useEffect(() => {
    void quest.queryRaw("select build", { limit: "0,1000" }).then((result) => {
      if (result.type === QuestDB.Type.DQL) {
        if (result.count === 1) {
          setBuildVersion(formatVersion(result.dataset[0][0]))
        }
      }
    })
  })

  const handleCopy = useCallback(() => {
    void navigator.clipboard.writeText(buildVersion)
  }, [buildVersion])

  return (
    <Wrapper>
      <CopyButton onClick={handleCopy} title="Copy Build Version">
        <span>{buildVersion}</span>
        <ClipboardCopy size="18px" />
      </CopyButton>
    </Wrapper>
  )
}

export default BuildVersion
