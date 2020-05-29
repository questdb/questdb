import React, { useCallback, useEffect, useState } from "react"
import styled from "styled-components"
import { Code, Upload } from "@styled-icons/entypo"

import { PopperHover, PrimaryToggleButton, Tooltip } from "components"
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

type Tab = "console" | "import"

const Sidebar = () => {
  const [selected, setSelected] = useState<Tab>("console")
  const handleConsoleClick = useCallback(() => {
    setSelected("console")
  }, [])
  const handleImportClick = useCallback(() => {
    setSelected("import")
  }, [])

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
        delay={350}
        placement="right"
        trigger={
          <Navigation
            direction="left"
            onClick={handleImportClick}
            selected={selected === "import"}
          >
            <Upload size="16px" />
          </Navigation>
        }
      >
        <Tooltip>Import</Tooltip>
      </PopperHover>
    </Wrapper>
  )
}

export default Sidebar
