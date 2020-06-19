import React, {
  CSSProperties,
  forwardRef,
  Ref,
  useCallback,
  useEffect,
  useState,
} from "react"
import { from, combineLatest, of } from "rxjs"
import { delay, startWith } from "rxjs/operators"
import styled, { css } from "styled-components"
import { Database2 } from "@styled-icons/remix-line/Database2"
import { Loader3 } from "@styled-icons/remix-line/Loader3"
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
  hideMenu?: boolean
  style?: CSSProperties
}>

const loadingStyles = css`
  display: flex;
  justify-content: center;
`

const Wrapper = styled(PaneWrapper)`
  overflow-x: auto;
`

const Menu = styled(PaneMenu)`
  justify-content: space-between;
`

const Header = styled(Text)`
  display: flex;
  align-items: center;
`

const Content = styled(PaneContent)<{
  _loading: boolean
}>`
  display: block;
  font-family: ${({ theme }) => theme.fontMonospace};
  overflow: auto;
  ${({ _loading }) => _loading && loadingStyles};
`

const DatabaseIcon = styled(Database2)`
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

const Schema = ({
  innerRef,
  ...rest
}: Props & { innerRef: Ref<HTMLDivElement> }) => {
  const [quest] = useState(new QuestDB.Client())
  const [loading, setLoading] = useState(false)
  const [tables, setTables] = useState<QuestDB.Table[]>()
  const [opened, setOpened] = useState<string>()
  const [refresh, setRefresh] = useState(Date.now())

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
        setRefresh(Date.now())
      } else {
        setLoading(loading)
      }
    })
  }, [quest])

  useEffect(() => {
    void fetchTables()
  }, [fetchTables])

  return (
    <Wrapper ref={innerRef} {...rest}>
      <Menu>
        <Header color="draculaForeground">
          <DatabaseIcon size="18px" />
          Tables
        </Header>

        <PopperHover
          delay={350}
          placement="bottom"
          trigger={
            <SecondaryButton onClick={fetchTables}>
              <Refresh size="18px" />
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
          tables.map(({ table }) => (
            <Table
              expanded={table === opened}
              key={table}
              onChange={handleChange}
              refresh={refresh}
              table={table}
            />
          ))}
        {!loading && <FlexSpacer />}
      </Content>
    </Wrapper>
  )
}

const SchemaWithRef = (props: Props, ref: Ref<HTMLDivElement>) => (
  <Schema {...props} innerRef={ref} />
)

export default forwardRef(SchemaWithRef)
