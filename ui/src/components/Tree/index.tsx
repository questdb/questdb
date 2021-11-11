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

import React, { useState, useCallback, useEffect } from "react"
import Row from "../../scenes/Schema/Row"
import styled from "styled-components"

export type TreeNodeKind = "column" | "table" | "folder"

export type TreeNodeRenderParams = {
  toggleOpen: () => void
  isOpen: boolean
}

export type TreeNodeRender = ({
  toggleOpen,
  isOpen,
}: TreeNodeRenderParams) => React.ReactElement

export type TreeNode = {
  name: string
  kind?: TreeNodeKind
  render?: TreeNodeRender
  initiallyOpen?: boolean
  wrapper?: React.FunctionComponent
  onOpen?: (leaf: TreeNode) => Promise<TreeNode[]> | undefined
  children?: TreeNode[]
}

const Ul = styled.ul`
  padding: 0;
  margin: 0;
`

const Li = styled.li`
  list-style: none;
  padding: 0 0 0 1rem;
`

const WrapWithIf: React.FunctionComponent<{
  condition: boolean
  wrapper: (children: React.ReactElement) => JSX.Element
  children: React.ReactElement
}> = ({ condition, wrapper, children }) =>
  condition ? wrapper(children) : children

const Leaf = (leaf: TreeNode) => {
  const {
    name,
    kind,
    initiallyOpen,
    onOpen,
    render,
    children = [],
    wrapper,
  } = leaf
  const [open, setOpen] = useState(initiallyOpen ?? false)
  const [loading, setLoading] = useState(false)
  const [content, setContent] = useState<TreeNode[]>([])

  const loadNewContent = useCallback(async () => {
    if (typeof onOpen === "function") {
      setLoading(true)
      const onOpenCandidate = onOpen(leaf)
      if (onOpenCandidate instanceof Promise) {
        const newContent = await onOpenCandidate
        setContent(newContent ?? [])
      }
      setLoading(false)
    }
  }, [leaf, onOpen])

  const toggleOpen: () => void = useCallback(async () => {
    if (!loading && !open) {
      await loadNewContent()
    }
    setOpen(!open)
  }, [open, loading, loadNewContent])

  useEffect(() => {
    const loadInitialContent: () => void = async () => {
      await loadNewContent()
    }

    if (open && typeof onOpen === "function" && content.length === 0) {
      if (!loading) {
        loadInitialContent()
      }
    }
  }, [])

  useEffect(() => {
    setOpen(initiallyOpen ?? false)
  }, [initiallyOpen])

  return (
    <Li>
      {typeof render === "function" ? (
        render({ toggleOpen, isOpen: open })
      ) : (
        <Row kind={kind ?? "folder"} name={name} />
      )}

      {open && (
        <WrapWithIf
          condition={Boolean(wrapper)}
          wrapper={(children) =>
            React.createElement(wrapper ?? React.Fragment, {}, children)
          }
        >
          <Tree root={content.length > 0 ? content : children} />
        </WrapWithIf>
      )}
    </Li>
  )
}

export const Tree: React.FunctionComponent<{
  root: TreeNode[]
}> = ({ root }) => (
  <Ul>
    {root.map((leaf: TreeNode) => (
      <Leaf key={leaf.name} {...leaf} />
    ))}
  </Ul>
)
