import React, { MouseEvent } from "react"
import styled from "styled-components"
import { color, ColumnDefinition } from "utils"
import {
  ColumnTextAlignment,
  ComputedGridWidths,
  getColumnTextAlignment,
} from "../utils"
import { BusEvent } from "../../../../consts"

type Props = {
  columns: ColumnDefinition[]
  computedGridWidths: ComputedGridWidths
}

const Wrapper = styled.div`
  width: 100%;
  height: 33px;
  display: flex;
  overflow: hidden;
  white-space: nowrap;
  background: ${color("draculaBackgroundDarker")};
`

const Column = styled.div<{ alignment: ColumnTextAlignment }>`
  display: flex;
  align-items: center;
  padding: 0 3px;
  font-weight: 700;
  border-bottom: 1px solid ${color("black")};
  overflow: hidden;
  text-overflow: ellipsis;
  cursor: pointer;
  user-select: none;
  color: ${color("draculaYellow")};
  ${(props) => props.alignment === "left" && `justify-content: flex-start`};
  ${(props) => props.alignment === "right" && `justify-content: flex-end`};
`

const Header = ({ columns, computedGridWidths }: Props) => {
  const handleClick = (event: MouseEvent) => {
    window.bus.trigger(
      BusEvent.MSG_EDITOR_INSERT_COLUMN,
      event.currentTarget.getAttribute("data-name"),
    )
  }
  return (
    <Wrapper style={{ width: computedGridWidths.totalWidth }}>
      {columns.map((item, index) => (
        <Column
          alignment={getColumnTextAlignment(item.type)}
          data-name={item.name}
          key={item.name}
          onClick={handleClick}
          style={{ width: computedGridWidths.columnWidths[index] }}
        >
          {item.name}
        </Column>
      ))}
    </Wrapper>
  )
}

export default Header
