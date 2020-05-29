import React, { useCallback, useState } from "react"
import { CSSTransition } from "react-transition-group"
import { from, combineLatest, of } from "rxjs"
import { delay, startWith } from "rxjs/operators"
import styled, { css } from "styled-components"
import { Table as Inbox, Loader4 } from "@styled-icons/remix-line"

import { spinCss } from "components"
import { color, QuestDB, QuestDBColumn, QuestDBTable } from "utils"
import Row from "../Row"

type Props = QuestDBTable &
  Readonly<{
    expanded: boolean
    description?: string
    onChange: (tableName: string) => void
  }>

type TitleProps = Readonly<{ expanded: boolean }>

const TRANSITION_MS = 120

const slideCss = css<{ _height: number }>`
  .slide-enter {
    max-height: 0;
  }

  .slide-enter-active {
    max-height: ${({ _height }) => _height}px;
    transition: all ${TRANSITION_MS}ms;
  }

  .slide-exit {
    max-height: ${({ _height }) => _height}px;
  }

  .slide-exit-active {
    max-height: 0;
    transition: all ${TRANSITION_MS}ms;
  }
`

const Wrapper = styled.div`
  display: flex;
  margin-top: 6px;
  align-items: stretch;
  flex-direction: column;
  overflow: hidden;

  ${slideCss};
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

const Loader = styled(Loader4)`
  margin-left: 1rem;
  color: ${color("draculaOrange")};
  ${spinCss};
`

const Table = ({ description, expanded, onChange, tableName }: Props) => {
  const [quest] = useState(new QuestDB({ port: BACKEND_PORT }))
  const [loading, setLoading] = useState(false)
  const [columns, setColumns] = useState<QuestDBColumn[]>()

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
        if (response) {
          setColumns(response.data)
          setLoading(false)
        } else {
          setLoading(loading)
        }
      })
    }
  }, [expanded])

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
        classNames="slide"
        in={expanded}
        timeout={TRANSITION_MS}
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
