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

import React from "react"
import styled, { css } from "styled-components"
import { FileCode } from "@styled-icons/remix-line/FileCode"

import { Text, TransitionDuration } from "components"
import type { Query } from "types"
import { color } from "utils"

type MouseAction = (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => void

type Props = Readonly<{
  active: boolean
  hidePicker: () => void
  onMouseEnter: MouseAction
  onMouseLeave: MouseAction
  onClick: MouseAction
  query: Query
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

const Value = styled(Text)`
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

const Row = ({ active, onMouseEnter, onMouseLeave, onClick, query }: Props) => (
  <Wrapper
    active={active}
    onClick={onClick}
    onMouseEnter={onMouseEnter}
    onMouseLeave={onMouseLeave}
  >
    <FileIcon size="12px" />

    {query.name && (
      <Name color="draculaForeground" size="sm">
        {query.name}
      </Name>
    )}

    <Value color="draculaForeground" size="sm">
      {query.value}
    </Value>
  </Wrapper>
)

export default Row
