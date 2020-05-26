import React, { useCallback } from "react"
import styled from "styled-components"
import { Table as Inbox } from "@styled-icons/remix-line"

import type { TableShape } from "types"
import { color } from "utils"
import Row from "../Row"

type Props = TableShape &
  Readonly<{
    expanded: boolean
    onChange: (name: string) => void
  }>

type TitleProps = Readonly<{ expanded: boolean }>

const Wrapper = styled.div`
  display: flex;
  flex-direction: column;
  align-items: stretch;
  margin-top: 6px;
`

const Title = styled(Row)`
  display: flex;
  align-items: stretch;
  font-weight: ${({ expanded }) => (expanded ? 800 : 400)};

  &:hover {
    cursor: pointer;
  }
`

const Columns = styled.div`
  position: relative;
  display: flex;
  margin-left: 3rem;
  flex-direction: column;

  &:before {
    position: absolute;
    height: 100%;
    width: 2px;
    left: -12px;
    top: 0;
    content: "";
    background: ${color("gray1")};
  }
`

const TitleIcon = styled(Inbox)`
  margin-right: 1rem;
  color: ${color("draculaCyan")};
`

const Table = ({ columns, description, expanded, name, onChange }: Props) => {
  const handleClick = useCallback(() => {
    onChange(expanded ? "" : name)
  }, [expanded])

  return (
    <Wrapper>
      <Title
        description={description}
        expanded={expanded}
        name={name}
        onClick={handleClick}
        tooltip
      >
        <TitleIcon size="18px" />
      </Title>

      {expanded && (
        <Columns>
          {columns.map((column) => (
            <Row {...column} key={column.name} />
          ))}
        </Columns>
      )}
    </Wrapper>
  )
}

export default Table
