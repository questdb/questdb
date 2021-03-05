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

import React, { useCallback, useEffect, useState } from "react"
import { useSelector } from "react-redux"
import styled from "styled-components"
import { CodeSSlash } from "@styled-icons/remix-line/CodeSSlash"
import { Upload2 } from "@styled-icons/remix-line/Upload2"

import { PopperHover, PrimaryToggleButton, Tooltip } from "components"
import { selectors } from "store"
import { color } from "utils"

const Wrapper = styled.div`
  display: flex;
  height: calc(100% - 4rem);
  flex: 0 0 4.5rem;
  flex-direction: column;
  border-right: 1px solid rgba(0, 0, 0, 0.1);
  background: ${color("draculaBackgroundDarker")};
`

const Logo = styled.div`
  position: relative;
  display: flex;
  flex: 0 0 4rem;
  background: ${color("black")};
  z-index: 1;

  a {
    display: flex;
    flex: 1;
    align-items: center;
    justify-content: center;
  }
`

type NavigationProps = Readonly<{
  selected: boolean
}>

const Navigation = styled(PrimaryToggleButton)<NavigationProps>`
  display: flex;
  flex: 0 0 5rem;
  align-items: center;
  justify-content: center;
`

const DisabledNavigation = styled.div`
  display: flex;
  position: relative;
  height: 100%;
  width: 100%;
  flex: 0 0 5rem;
  align-items: center;
  justify-content: center;

  &:disabled {
    pointer-events: none;
  }
`

type Tab = "console" | "import"

const Sidebar = () => {
  const [selected, setSelected] = useState<Tab>("console")
  const handleConsoleClick = useCallback(() => {
    setSelected("console")
  }, [])
  const handleImportClick = useCallback(() => {
    setSelected("import")
  }, [])
  const { readOnly } = useSelector(selectors.console.getConfig)

  useEffect(() => {
    const consolePanel = document.querySelector<HTMLElement>(".js-sql-panel")
    const importPanel = document.querySelector<HTMLElement>(".js-import-panel")

    if (!consolePanel || !importPanel) {
      return
    }

    if (selected === "import") {
      consolePanel.style.display = "none"
      importPanel.style.display = "flex"
    } else {
      consolePanel.style.display = "flex"
      importPanel.style.display = "none"
    }
  }, [selected])

  return (
    <Wrapper>
      <Logo>
        <a href="https://questdb.io" rel="noreferrer" target="_blank">
          <img alt="QuestDB Logo" height="26" src="/assets/favicon.svg" />
        </a>
      </Logo>

      <PopperHover
        delay={350}
        placement="right"
        trigger={
          <Navigation
            direction="left"
            onClick={handleConsoleClick}
            selected={selected === "console"}
          >
            <CodeSSlash size="18px" />
          </Navigation>
        }
      >
        <Tooltip>Console</Tooltip>
      </PopperHover>

      <PopperHover
        delay={readOnly ? 0 : 350}
        placement="right"
        trigger={
          readOnly ? (
            <DisabledNavigation>
              <Navigation
                direction="left"
                disabled
                onClick={handleImportClick}
                selected={selected === "import"}
              >
                <Upload2 size="18px" />
              </Navigation>
            </DisabledNavigation>
          ) : (
            <Navigation
              direction="left"
              onClick={handleImportClick}
              selected={selected === "import"}
            >
              <Upload2 size="18px" />
            </Navigation>
          )
        }
      >
        <Tooltip>
          {readOnly ? (
            <>
              <b>Import</b> is currently disabled.
              <br />
              To use this feature, turn <b>read-only</b> mode to <i>false</i> in
              the configuration file
            </>
          ) : (
            <>Import</>
          )}
        </Tooltip>
      </PopperHover>
    </Wrapper>
  )
}

export default Sidebar
