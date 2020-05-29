import React, { useCallback, useEffect, useState } from "react"
import { TransitionGroup } from "react-transition-group"
import { from, combineLatest, of } from "rxjs"
import { delay, startWith } from "rxjs/operators"
import styled from "styled-components"
import { Database } from "@styled-icons/entypo"
import { Loader3 } from "@styled-icons/remix-fill"
import { Refresh } from "@styled-icons/remix-line"

import {
  Pane,
  PopperHover,
  PaneTitle,
  SecondaryButton,
  spinCss,
  Text,
  Tooltip,
} from "components"
import { theme } from "theme"
import { color, QuestDB, QuestDBTable } from "utils"

import Table from "./Table"

const Title = styled(PaneTitle)`
  justify-content: space-between;
`

const Wrapper = styled(Pane)`
  font-family: ${theme.fontMonospace};
`

const DatabaseIcon = styled(Database)`
  margin-right: 1rem;
`

const Loader = styled(Loader3)`
  margin-left: 1rem;
  align-self: center;
  color: ${color("draculaForeground")};
  ${spinCss};
`

const FlexSpacer = styled.div`
  flex: 1;
`

const Schema = () => {
  const [quest] = useState(new QuestDB({ port: BACKEND_PORT }))
  const [loading, setLoading] = useState(false)
  const [tables, setTables] = useState<QuestDBTable[]>()
  const [opened, setOpened] = useState<string>()

  const handleChange = useCallback((name: string) => {
    setOpened(name)
  }, [])

  const fetchTables = useCallback(() => {
    combineLatest(
      from(quest.showTables()).pipe(startWith(null)),
      of(true).pipe(delay(1000), startWith(false)),
    ).subscribe(([response, loading]) => {
      if (response) {
        setTables(response.data)
        setLoading(false)
      } else {
        setLoading(loading)
      }
    })
  }, [])

  useEffect(() => {
    void fetchTables()
  }, [])

  return (
    <>
      <Title>
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
      </Title>

      <Wrapper>
        {loading && <Loader size="48px" />}
        {!loading && tables && (
          <TransitionGroup>
            {tables.map(({ tableName }) => (
              <Table
                expanded={tableName === opened}
                key={tableName}
                onChange={handleChange}
                tableName={tableName}
              />
            ))}
          </TransitionGroup>
        )}
        {!loading && <FlexSpacer />}
      </Wrapper>
    </>
  )
}

export default Schema
