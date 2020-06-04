import React, { useCallback, useEffect, useRef, useState } from "react"
import { from, combineLatest, of } from "rxjs"
import { delay, startWith } from "rxjs/operators"
import styled, { css } from "styled-components"
import { Database } from "@styled-icons/entypo/Database"
import { Loader3 } from "@styled-icons/remix-fill/Loader3"
import { Refresh } from "@styled-icons/remix-line/Refresh"

import {
  PaneContent,
  PaneWrapper,
  PopperHover,
  PaneMenu,
  SecondaryButton,
  spinAnimation,
  Text,
  Tooltip,
} from "components"
import { color } from "utils"
import * as QuestDB from "utils/questdb"

import Table from "./Table"

type Props = Readonly<{
  widthOffset?: number
}>

const loadingStyles = css`
  display: flex;
  justify-content: center;
`

const Wrapper = styled(PaneWrapper)<{
  basis?: number
}>`
  flex-grow: 0;
  flex-shrink: 1;
  ${({ basis }) => basis && `flex-basis: ${basis}px`};
  overflow-x: auto;
`

const Menu = styled(PaneMenu)`
  justify-content: space-between;
`

const Content = styled(PaneContent)<{
  _loading: boolean
}>`
  display: block;
  font-family: ${({ theme }) => theme.fontMonospace};
  ${({ _loading }) => _loading && loadingStyles};
`

const DatabaseIcon = styled(Database)`
  margin-right: 1rem;
`

const Loader = styled(Loader3)`
  margin-left: 1rem;
  align-self: center;
  color: ${color("draculaForeground")};
  ${spinAnimation};
`

const FlexSpacer = styled.div`
  flex: 1;
`

const Schema = ({ widthOffset }: Props) => {
  const [quest] = useState(new QuestDB.Client({ port: BACKEND_PORT }))
  const [loading, setLoading] = useState(false)
  const element = useRef<HTMLDivElement | null>(null)
  const [width, setWidth] = useState<number>()
  const [tables, setTables] = useState<QuestDB.Table[]>()
  const [opened, setOpened] = useState<string>()

  const handleChange = useCallback((name: string) => {
    setOpened(name)
  }, [])

  const fetchTables = useCallback(() => {
    combineLatest(
      from(quest.showTables()).pipe(startWith(null)),
      of(true).pipe(delay(1000), startWith(false)),
    ).subscribe(([response, loading]) => {
      if (response && response.type === QuestDB.Type.DQL) {
        setTables(response.data)
        setLoading(false)
      } else {
        setLoading(loading)
      }
    })
  }, [quest])

  useEffect(() => {
    void fetchTables()
  }, [fetchTables])

  useEffect(() => {
    if (element.current && widthOffset) {
      const width = element.current.getBoundingClientRect().width + widthOffset
      localStorage.setItem("splitter.schema", `${width}`)
      setWidth(width)
    }
  }, [widthOffset])

  useEffect(() => {
    const size = parseInt(localStorage.getItem("splitter.schema") || "0", 10)

    if (size) {
      setWidth(size)
    } else {
      setWidth(350)
    }
  }, [])

  if (!width) {
    return null
  }

  return (
    <Wrapper basis={width} ref={element}>
      <Menu>
        <Text color="draculaForeground">
          <DatabaseIcon size="18px" />
          Tables
        </Text>

        <PopperHover
          delay={350}
          placement="bottom"
          trigger={
            <SecondaryButton onClick={fetchTables}>
              <Refresh size="16px" />
            </SecondaryButton>
          }
        >
          <Tooltip>Refresh</Tooltip>
        </PopperHover>
      </Menu>

      <Content _loading={loading}>
        {loading && <Loader size="48px" />}
        {!loading &&
          tables &&
          tables.map(({ tableName }) => (
            <Table
              expanded={tableName === opened}
              key={tableName}
              onChange={handleChange}
              tableName={tableName}
            />
          ))}
        {!loading && <FlexSpacer />}
      </Content>
    </Wrapper>
  )
}

export default Schema
