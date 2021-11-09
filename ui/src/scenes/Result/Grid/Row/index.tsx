import React from "react"
import styled from "styled-components"
import { color, ColumnDefinition, DatasetType } from "utils"
import {
  ColumnTextAlignment,
  ComputedGridWidths,
  getColumnTextAlignment,
} from "../utils"

type Props = {
  isScrolling: boolean
  data: DatasetType | undefined
  columns: ColumnDefinition[]
  computedGridWidths: ComputedGridWidths
  index: number
  selected: boolean | undefined
  onRowClick: (index: number) => void
}

const Wrapper = styled.div<{
  selected: boolean | undefined
  onClick: (event: MouseEvent) => void
}>`
  width: 100%;
  display: flex;
  overflow: hidden;
  white-space: nowrap;
  height: 28px;
  border-bottom: 1px solid ${color("draculaSelection")};

  &:hover {
    background-color: ${color("draculaSelection")};
  }

  &:focus {
    outline: none;
  }

  ${(props) =>
    props.selected &&
    `
      background-color: #6272a4;
      
      &:hover {
        background-color: #6272a4;
        color: ${color("draculaForeground")};
      }
  `}
`

const ColumnText = styled.span`
  position: relative;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`

const Column = styled.div<{ alignment: ColumnTextAlignment }>`
  display: flex;
  flex-shrink: 0;
  align-items: center;
  padding: 0 3px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  ${(props) => props.alignment === "left" && `justify-content: flex-start`};
  ${(props) => props.alignment === "right" && `justify-content: flex-end`};

  &:hover {
    box-shadow: inset 0 0 0 2px #8d95a5;
    border-bottom: 1px solid transparent;

    ${ColumnText} {
      transform: translateY(1px);
    }
  }
`

const Row = ({
  isScrolling,
  data,
  columns,
  computedGridWidths,
  selected,
  index,
  onRowClick,
}: Props) => {
  const handleRowClick = () => {
    onRowClick(index)
  }

  return (
    <Wrapper onClick={handleRowClick} selected={selected}>
      {columns.map((item, index) => (
        <Column
          alignment={getColumnTextAlignment(item.type)}
          key={item.name}
          style={{ width: computedGridWidths.columnWidths[index] }}
        >
          <ColumnText>
            {data?.[index] !== null ? data?.[index] : "null"}
          </ColumnText>
        </Column>
      ))}
    </Wrapper>
  )
}

export default Row
