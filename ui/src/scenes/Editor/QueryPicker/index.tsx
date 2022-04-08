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

import React, { forwardRef, Ref, useCallback, useEffect, useState } from "react"
import styled from "styled-components"
import { DownArrowSquare } from "@styled-icons/boxicons-solid/DownArrowSquare"
import { UpArrowSquare } from "@styled-icons/boxicons-solid/UpArrowSquare"

import { Text, useKeyPress } from "components"
import type { Query, QueryGroup } from "types"
import { color } from "utils"

import QueryRow from "./Row"
import { useEditor } from "../../../providers"

type Props = {
  hidePicker: () => void
  queries: Array<Query | QueryGroup>
  ref: Ref<HTMLDivElement>
}

const Wrapper = styled.div`
  display: flex;
  max-height: 650px;
  width: 600px;
  max-width: 100vw;
  padding: 0.6rem 0;
  flex-direction: column;
  background: ${color("draculaBackgroundDarker")};
  box-shadow: ${color("black")} 0px 5px 8px;
  border: 1px solid ${color("black")};
  border-radius: 4px;
  overflow: auto;
`

const Helper = styled(Text)`
  padding: 1rem;
  text-align: center;
  opacity: 0.5;
`

const Esc = styled(Text)`
  padding: 0 2px;
  background: ${color("draculaForeground")};
  border-radius: 2px;
`

type QueryListItem =
  | { type: "query"; id: number; data: Query }
  | { type: "descriptor"; id: number; data: QueryGroup }

const prepareQueriesList = (queries: Props["queries"]) => {
  const queue = [...queries]
  const list: QueryListItem[] = []

  let id = 0
  while (queue.length) {
    const current = queue.shift()

    if (typeof (current as QueryGroup).queries === "undefined") {
      list.push({
        type: "query",
        id,
        data: current as Query,
      })
    } else {
      const data = current as QueryGroup
      list.push({
        type: "descriptor",
        id,
        data,
      })
      queue.unshift(...data.queries)
    }
    id++
  }

  return list
}

const isQuery = ({ type }: QueryListItem) => type === "query"

const Title = styled(Text)`
  padding: 0.6rem 1.2rem 0.4rem;
  margin-top: 0.6rem;
  border-top: 1px solid ${({ theme }) => theme.color.draculaSelection};
  background: ${({ theme }) => theme.color.blackAlpha40};
`

const Description = styled(Text)`
  padding: 0 1.2rem 1rem;
  color: ${({ theme }) => theme.color.draculaForeground};
  opacity: 0.7;
  background: ${({ theme }) => theme.color.blackAlpha40};
`

const QueryPicker = ({ hidePicker, queries, ref }: Props) => {
  const downPress = useKeyPress("ArrowDown")
  const upPress = useKeyPress("ArrowUp")
  const enterPress = useKeyPress("Enter")
  const [cursor, setCursor] = useState<number>(-1)
  const [queryList, setQueryList] = useState<
    ReturnType<typeof prepareQueriesList>
  >([])
  const { appendQuery } = useEditor()

  useEffect(() => {
    setQueryList(prepareQueriesList(queries))
  }, [queries])

  const addQuery = useCallback(
    (query: Query) => {
      hidePicker()
      appendQuery(query.value)
    },
    [hidePicker],
  )

  useEffect(() => {
    if (queryList.length) {
      if (downPress) {
        const firstQueryIndex = queryList.find(isQuery)?.id ?? 0
        const nextIndex =
          queryList.slice(cursor + 1).find(isQuery)?.id ?? firstQueryIndex
        setCursor(nextIndex)
      }

      if (upPress) {
        const reversedList = [...queryList].reverse()
        const lastQueryIndex =
          reversedList.find(isQuery)?.id ?? queryList.length - 1

        const prevIndex =
          [...queryList].slice(0, cursor).reverse().find(isQuery)?.id ??
          lastQueryIndex
        setCursor(prevIndex)
      }
    }
  }, [upPress, downPress, queryList])

  useEffect(() => {
    if (enterPress) {
      const query = queryList.find(
        ({ id, type }) => id === cursor && type === "query",
      )
      if (query) {
        addQuery(query.data as Query)
      }
    }
  }, [cursor, enterPress, hidePicker, queries, addQuery])

  return (
    <Wrapper ref={ref}>
      <Helper _style="italic" color="draculaForeground" size="xs">
        Navigate the list with <UpArrowSquare size="16px" />
        <DownArrowSquare size="16px" /> keys, exit with&nbsp;
        <Esc _style="normal" size="ms" weight={700}>
          Esc
        </Esc>
      </Helper>

      {queryList.map((entry) => {
        if (entry.type === "query") {
          const query = entry.data
          return (
            <QueryRow
              active={entry.id === cursor}
              hidePicker={hidePicker}
              key={entry.id}
              onClick={() => addQuery(query)}
              onMouseEnter={() => setCursor(entry.id)}
              onMouseLeave={() => setCursor(-1)}
              query={query}
            />
          )
        }

        const { title, description } = entry.data
        return (
          <React.Fragment key={entry.id}>
            <Title color="draculaForeground" size="md">
              {title}
            </Title>
            <Description color="draculaForeground">{description}</Description>
          </React.Fragment>
        )
      })}
    </Wrapper>
  )
}

const QueryPickerWithRef = (props: Props, ref: Ref<HTMLDivElement>) => (
  <QueryPicker {...props} ref={ref} />
)

export default forwardRef(QueryPickerWithRef)
