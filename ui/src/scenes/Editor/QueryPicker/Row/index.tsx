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

import React, { useCallback } from "react"
import styled, { css } from "styled-components"
import { FileCode } from "@styled-icons/remix-line/FileCode"

import { Text, TransitionDuration } from "components"
import { BusEvent } from "consts"
import { QueryShape } from "types"
import { color } from "utils"

type Props = Readonly<{
  active: boolean
  hidePicker: () => void
  onHover: (query?: QueryShape) => void
  query: QueryShape
}>

const activeStyles = css`
  background: ${color("draculaSelection")};
`

const Wrapper = styled.div<{ active: boolean }>`
  display: flex;
  height: 2.4rem;
  padding: 0 0.6rem;
  line-height: 2.4rem;
  align-items: center;
  transition: background ${TransitionDuration.FAST}ms;
  cursor: pointer;
  user-select: none;

  ${({ active }) => active && activeStyles};

  > span:not(:last-child) {
    margin-right: 0.6rem;
  }
`

const Query = styled(Text)`
  flex: 1 1 auto;
  text-overflow: ellipsis;
  overflow: hidden;
  white-space: nowrap;
  opacity: 0.7;
`

const FileIcon = styled(FileCode)`
  height: 2.2rem;
  flex: 0 0 12px;
  margin: 0 0.6rem;
  color: ${color("draculaOrange")};
`

const Name = styled(Text)`
  flex: 0 0 auto;
`

const Row = ({ active, hidePicker, onHover, query }: Props) => {
  const handleClick = useCallback(() => {
    hidePicker()
    window.bus.trigger(BusEvent.MSG_EDITOR_SET, query.value)
  }, [hidePicker, query])
  const handleMouseEnter = useCallback(() => {
    onHover(query)
  }, [query, onHover])
  const handleMouseLeave = useCallback(() => {
    onHover()
  }, [onHover])

  return (
    <Wrapper
      active={active}
      onClick={handleClick}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      <FileIcon size="12px" />

      {query.name && (
        <Name color="draculaForeground" size="sm">
          {query.name}
        </Name>
      )}

      <Query color="draculaForeground" size="sm">
        {query.value}
      </Query>
    </Wrapper>
  )
}

export default Row
