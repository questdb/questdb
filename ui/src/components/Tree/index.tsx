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

import React, { useState, useCallback } from "react"
import { CSSTransition } from "react-transition-group"
import styled from "styled-components"

import { Text, TransitionDuration } from "components"

export type TreeInterface = {
  name: string
  payload: unknown
  children?: TreeInterface[]
}

type Props = {
  root: TreeInterface[]
  onLeafOpen: (payload: unknown) => Promise<TreeInterface[]>
}

const Ul = styled.ul`
  padding: 0.5rem 0;
  margin-left: 0;
`

const Li = styled.li`
  list-style: none;
  padding-left: 1rem;
`

const Leaf = ({
  label,
  payload,
  onOpen,
}: {
  label: string
  payload: unknown
  onOpen: (payload: unknown) => Promise<TreeInterface[]>
}) => {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [content, setContent] = useState<TreeInterface[]>([])

  const toggleOpen = useCallback(async () => {
    setLoading(true)

    if (!loading && !open) {
      setContent(await onOpen(payload))
    }

    setLoading(false)
    setOpen(!open)
  }, [open, loading, onOpen, payload])

  return (
    <Li>
      <Text color="draculaForeground" ellipsis onClick={toggleOpen}>
        {label}
      </Text>

      <CSSTransition
        classNames="collapse"
        in={open && content.length > 0}
        timeout={TransitionDuration.REG}
        unmountOnExit
      >
        <Tree onLeafOpen={onOpen} root={content} />
      </CSSTransition>
    </Li>
  )
}

export const Tree: React.FunctionComponent<Props> = ({ root, onLeafOpen }) => {
  return (
    <Ul>
      {root.map(({ name, payload }) => (
        <Leaf key={name} label={name} onOpen={onLeafOpen} payload={payload} />
      ))}
    </Ul>
  )
}
