import React, { useCallback, useState } from "react"
import { CSSTransition } from "react-transition-group"
import { from, combineLatest, of } from "rxjs"
import { delay, startWith } from "rxjs/operators"
import styled from "styled-components"
import { Loader4 } from "@styled-icons/remix-line/Loader4"
import { Table as TableIcon } from "@styled-icons/remix-line/Table"
import {
  collapseTransition,
  spinAnimation,
  TransitionDuration,
} from "components"
import { color } from "utils"
import * as QuestDB from "utils/questdb"
import Row from "../Row"

type Props = QuestDB.Table &
  Readonly<{
    expanded: boolean
    description?: string
    onChange: (tableName: string) => void
  }>

type TitleProps = Readonly<{ expanded: boolean }>

const Wrapper = styled.div`
  display: flex;
  margin-top: 0.5rem;
  align-items: stretch;
  flex-direction: column;
  overflow: hidden;

  ${collapseTransition};
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
    left: -1.2rem;
    top: 0;
    content: "";
    background: ${color("gray1")};
  }
`

const TitleIcon = styled(TableIcon)`
  min-height: 18px;
  min-width: 18px;
  margin-right: 1rem;
  color: ${color("draculaCyan")};
`

const Loader = styled(Loader4)`
  margin-left: 1rem;
  color: ${color("draculaOrange")};
  ${spinAnimation};
`

const Table = ({ description, expanded, onChange, tableName }: Props) => {
  const [quest] = useState(new QuestDB.Client())
  const [loading, setLoading] = useState(false)
  const [columns, setColumns] = useState<QuestDB.Column[]>()

  const handleClick = useCallback(() => {
    if (expanded) {
      onChange("")
    } else {
      setColumns(undefined)
      onChange(tableName)

      combineLatest(
        from(quest.showColumns(tableName)).pipe(startWith(null)),
        of(true).pipe(delay(1000), startWith(false)),
      ).subscribe(([response, loading]) => {
        if (response && response.type === QuestDB.Type.DQL) {
          setColumns(response.data)
          setLoading(false)
        } else {
          setLoading(loading)
        }
      })
    }
  }, [expanded, onChange, quest, tableName])

  return (
    <Wrapper _height={columns ? columns.length * 30 : 0}>
      <Title
        description={description}
        expanded={expanded}
        name={tableName}
        onClick={handleClick}
        prefix={<TitleIcon size="18px" />}
        suffix={loading && <Loader size="18px" />}
        tooltip={!!description}
      />

      <CSSTransition
        classNames="collapse"
        in={expanded}
        timeout={TransitionDuration.REG}
        unmountOnExit
      >
        <Columns>
          {columns &&
            columns.map(({ columnName, columnType }) => (
              <Row key={columnName} name={columnName} type={columnType} />
            ))}
        </Columns>
      </CSSTransition>
    </Wrapper>
  )
}

export default Table
