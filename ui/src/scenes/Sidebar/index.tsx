import React, { useCallback, useEffect, useState } from "react"
import { useSelector } from "react-redux"
import styled from "styled-components"
import { Code } from "@styled-icons/entypo/Code"
import { Upload } from "@styled-icons/entypo/Upload"

import { PopperHover, PrimaryToggleButton, Tooltip } from "components"
import { selectors } from "store"
import { color } from "utils"

const Wrapper = styled.div`
  display: flex;
  flex: 0 0 45px;
  flex-direction: column;
`

const Logo = styled.div`
  position: relative;
  display: flex;
  flex: 0 0 41px;
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
  const { readOnly } = useSelector(selectors.console.getConfiguration)

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
          <img alt="QuestDB Logo" height="30px" src="/assets/images/logo.png" />
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
            <Code size="18px" />
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
                <Upload size="16px" />
              </Navigation>
            </DisabledNavigation>
          ) : (
            <Navigation
              direction="left"
              onClick={handleImportClick}
              selected={selected === "import"}
            >
              <Upload size="16px" />
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
