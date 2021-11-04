/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

import React, {
  CSSProperties,
  forwardRef,
  Ref,
  useRef,
  useCallback,
  useEffect,
  useState,
} from "react"
import { useSelector } from "react-redux"
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
  VirtualList,
} from "components"
import { selectors } from "store"
import { color, ErrorResult } from "utils"
import * as QuestDB from "utils/questdb"

import Table from "./Table"
import LoadingError from "./LoadingError"
import { BusEvent } from "../../consts"

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
  height: 100%;
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
  const [loadingError, setLoadingError] = useState<ErrorResult | null>(null)
  const errorRef = useRef<ErrorResult | null>(null)
  const [tables, setTables] = useState<QuestDB.Table[]>()
  const [opened, setOpened] = useState<string>()
  const [refresh, setRefresh] = useState(Date.now())
  const [isScrolling, setIsScrolling] = useState(false)
  const { readOnly } = useSelector(selectors.console.getConfig)

  const handleChange = useCallback((name: string) => {
    setOpened(name)
  }, [])

  const handleScrollingStateChange = useCallback(
    (isScrolling) => {
      setIsScrolling(isScrolling)
    },
    [setIsScrolling],
  )

  const listItemContent = useCallback(
    (index: number) => {
      if (tables) {
        const table = tables[index]

        return (
          <Table
            designatedTimestamp={table.designatedTimestamp}
            expanded={table.name === opened}
            isScrolling={isScrolling}
            key={table.name}
            name={table.name}
            onChange={handleChange}
            partitionBy={table.partitionBy}
            refresh={refresh}
          />
        )
      }
    },
    [handleChange, isScrolling, opened, refresh, tables],
  )

  const fetchTables = useCallback(() => {
    setLoading(true)
    combineLatest(
      from(quest.showTables()).pipe(startWith(null)),
      of(true).pipe(delay(1000), startWith(false)),
    ).subscribe(
      ([response, loading]) => {
        if (response && response.type === QuestDB.Type.DQL) {
          setLoadingError(null)
          errorRef.current = null
          setTables(response.data)
          setRefresh(Date.now())
        } else {
          setLoading(false)
        }
      },
      (error) => {
        setLoadingError(error)
      },
      () => {
        setLoading(false)
      },
    )
  }, [quest])

  useEffect(() => {
    void fetchTables()

    window.bus.on(BusEvent.MSG_QUERY_SCHEMA, () => {
      void fetchTables()
    })

    window.bus.on(
      BusEvent.MSG_CONNECTION_ERROR,
      (_event, error: ErrorResult) => {
        errorRef.current = error
        setLoadingError(error)
      },
    )

    window.bus.on(BusEvent.MSG_CONNECTION_OK, () => {
      // The connection has been re-established, as we have an error in memory
      if (errorRef.current) {
        void fetchTables()
      }
    })
  }, [errorRef, fetchTables])

  return (
    <Wrapper ref={innerRef} {...rest}>
      <Menu>
        <Header color="draculaForeground">
          <DatabaseIcon size="18px" />
          Tables
        </Header>

        {readOnly === false && (
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
        )}
      </Menu>

      <Content _loading={loading}>
        {loading ? (
          <Loader size="48px" />
        ) : loadingError ? (
          <LoadingError error={loadingError} />
        ) : (
          <VirtualList
            isScrolling={handleScrollingStateChange}
            itemContent={listItemContent}
            totalCount={tables?.length}
          />
        )}
        {!loading && <FlexSpacer />}
      </Content>
    </Wrapper>
  )
}

const SchemaWithRef = (props: Props, ref: Ref<HTMLDivElement>) => (
  <Schema {...props} innerRef={ref} />
)

export default forwardRef(SchemaWithRef)
