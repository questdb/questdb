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
import { QueryShape } from "types"
import { color } from "utils"

import Row from "./Row"
import { useEditor } from "../../../providers"

type Props = {
  hidePicker: () => void
  queries: QueryShape[]
  ref: Ref<HTMLDivElement>
}

const Wrapper = styled.div`
  display: flex;
  max-height: 600px;
  width: 600px;
  max-width: 100vw;
  padding: 0.6rem 0;
  flex-direction: column;
  background: ${color("draculaBackgroundDarker")};
  box-shadow: ${color("black")} 0px 5px 8px;
  border: 1px solid ${color("black")};
  border-radius: 4px;
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

const QueryPicker = ({ hidePicker, queries, ref }: Props) => {
  const downPress = useKeyPress("ArrowDown")
  const upPress = useKeyPress("ArrowUp")
  const enterPress = useKeyPress("Enter", { preventDefault: true })
  const [cursor, setCursor] = useState(0)
  const [hovered, setHovered] = useState<QueryShape | undefined>()
  const { appendQuery } = useEditor()

  const addQuery = useCallback(
    (query: QueryShape) => {
      hidePicker()
      appendQuery(query.value)
    },
    [hidePicker],
  )

  useEffect(() => {
    if (queries.length && downPress) {
      setCursor((prevState) =>
        prevState < queries.length - 1 ? prevState + 1 : prevState,
      )
    }
  }, [downPress, queries])

  useEffect(() => {
    if (queries.length && upPress) {
      setCursor((prevState) => (prevState > 0 ? prevState - 1 : prevState))
    }
  }, [upPress, queries])

  useEffect(() => {
    if (enterPress && queries[cursor]) {
      addQuery(queries[cursor])
    }
  }, [cursor, enterPress, hidePicker, queries, addQuery])

  useEffect(() => {
    if (hovered) {
      setCursor(queries.indexOf(hovered))
    }
  }, [hovered, queries])

  return (
    <Wrapper ref={ref}>
      <Helper _style="italic" color="draculaForeground" size="xs">
        Navigate the list with <UpArrowSquare size="16px" />
        <DownArrowSquare size="16px" /> keys, exit with&nbsp;
        <Esc _style="normal" size="ms" weight={700}>
          Esc
        </Esc>
      </Helper>

      {queries.map((query, i) => (
        <Row
          active={i === cursor}
          hidePicker={hidePicker}
          key={query.value}
          onAdd={addQuery}
          onHover={setHovered}
          query={query}
        />
      ))}
    </Wrapper>
  )
}

const QueryPickerWithRef = (props: Props, ref: Ref<HTMLDivElement>) => (
  <QueryPicker {...props} ref={ref} />
)

export default forwardRef(QueryPickerWithRef)
