import React, { useCallback, useEffect, useState } from "react"
import { useSelector } from "react-redux"
import styled from "styled-components"
import { Download2 } from "@styled-icons/remix-line/Download2"
import { Grid } from "@styled-icons/remix-line/Grid"
import { PieChart } from "@styled-icons/remix-line/PieChart"
import { Refresh } from "@styled-icons/remix-line/Refresh"

import {
  PaneContent,
  PaneWrapper,
  PaneMenu,
  PopperHover,
  PrimaryToggleButton,
  SecondaryButton,
  Text,
  Tooltip,
  useScreenSize,
} from "components"
import { selectors } from "store"
import { BusEvent, color } from "utils"
import * as QuestDB from "utils/questdb"

const Menu = styled(PaneMenu)`
  justify-content: space-between;
`

const Wrapper = styled(PaneWrapper)`
  overflow: hidden;
`

const Content = styled(PaneContent)`
  color: ${color("draculaForeground")};

  *::selection {
    background: ${color("draculaRed")};
    color: ${color("draculaForeground")};
  }
`

const ButtonWrapper = styled.div`
  display: flex;
  align-items: center;

  ${/* sc-selector */ SecondaryButton}:not(:last-child) {
    margin-right: 1rem;
  }

  ${/* sc-selector */ Text} {
    margin-right: 2rem;
  }
`

const ToggleButton = styled(PrimaryToggleButton)`
  height: 4rem;
  width: 8.5rem;
`

const Result = () => {
  const [selected, setSelected] = useState<"chart" | "grid">("grid")
  const [count, setCount] = useState<number | undefined>()
  const { sm } = useScreenSize()
  const result = useSelector(selectors.query.getResult)
  const handleChartClick = useCallback(() => {
    setSelected("chart")
  }, [])
  const handleGridClick = useCallback(() => {
    setSelected("grid")
  }, [])
  const handleExportClick = useCallback(() => {
    window.bus.trigger("grid.publish.query")
  }, [])
  const handleRefreshClick = useCallback(() => {
    window.bus.trigger("grid.refresh")
  }, [])

  useEffect(() => {
    if (result?.type === QuestDB.Type.DQL) {
      setCount(result.count)
    }
  }, [result])

  useEffect(() => {
    const grid = document.getElementById("grid")
    const chart = document.getElementById("quick-vis")

    if (!grid || !chart) {
      return
    }

    if (selected === "grid") {
      grid.style.display = "flex"
      chart.style.display = "none"
      window.bus.trigger(BusEvent.MSG_ACTIVE_PANEL)
    } else {
      grid.style.display = "none"
      chart.style.display = "flex"
    }
  }, [selected])

  return (
    <Wrapper>
      <Menu>
        <ButtonWrapper>
          <ToggleButton
            onClick={handleGridClick}
            selected={selected === "grid"}
          >
            <Grid size="18px" />
            <span>Grid</span>
          </ToggleButton>

          <ToggleButton
            onClick={handleChartClick}
            selected={selected === "chart"}
          >
            <PieChart size="18px" />
            <span>Chart</span>
          </ToggleButton>
        </ButtonWrapper>

        <ButtonWrapper>
          {count && !sm && (
            <Text color="draculaForeground">
              {`${count} row${count > 1 ? "s" : ""}`}
            </Text>
          )}

          {!sm && (
            <PopperHover
              delay={350}
              placement="bottom"
              trigger={
                <SecondaryButton onClick={handleRefreshClick}>
                  <Refresh size="18px" />
                </SecondaryButton>
              }
            >
              <Tooltip>Refresh</Tooltip>
            </PopperHover>
          )}

          <PopperHover
            delay={350}
            placement="bottom"
            trigger={
              <SecondaryButton onClick={handleExportClick}>
                <Download2 size="18px" />
                <span>CSV</span>
              </SecondaryButton>
            }
          >
            <Tooltip>Download result as a CSV file</Tooltip>
          </PopperHover>
        </ButtonWrapper>
      </Menu>

      <Content>
        <div id="grid">
          <div className="qg-header-row" />
          <div className="qg-viewport">
            <div className="qg-canvas" />
          </div>
        </div>

        <div id="quick-vis">
          <div className="quick-vis-controls">
            <form className="v-fit" role="form">
              <div className="form-group">
                <label>Chart type</label>
                <select id="_qvis_frm_chart_type">
                  <option>bar</option>
                  <option>line</option>
                  <option>area</option>
                </select>
              </div>
              <div className="form-group">
                <label>Labels</label>
                <select id="_qvis_frm_axis_x" />
              </div>
              <div className="form-group">
                <label>Series</label>
                <select id="_qvis_frm_axis_y" multiple />
              </div>
              <button
                className="button-primary js-chart-draw"
                id="_qvis_frm_draw"
              >
                <i className="fa fa-play" />
                <span>Draw</span>
              </button>
            </form>
          </div>
          <div className="quick-vis-canvas" />
        </div>
      </Content>
    </Wrapper>
  )
}

export default Result
