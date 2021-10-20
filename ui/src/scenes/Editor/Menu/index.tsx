/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import docsearch from "docsearch.js"
import React, { useCallback, useEffect, useState } from "react"
import { useDispatch, useSelector } from "react-redux"
import { CSSTransition } from "react-transition-group"
import styled from "styled-components"
import { Add } from "@styled-icons/remix-line/Add"
import { Close as _CloseIcon } from "@styled-icons/remix-line/Close"
import { Menu as _MenuIcon } from "@styled-icons/remix-fill/Menu"
import { Play } from "@styled-icons/remix-line/Play"
import { Stop } from "@styled-icons/remix-line/Stop"

import {
  ErrorButton,
  Input,
  PaneMenu,
  PopperToggle,
  SecondaryButton,
  SuccessButton,
  TransitionDuration,
  TransparentButton,
  useKeyPress,
  useScreenSize,
} from "components"
import { actions, selectors } from "store"
import { color } from "utils"

import QueryPicker from "../QueryPicker"

const Wrapper = styled(PaneMenu)<{ _display: string }>`
  z-index: 15;

  .algolia-autocomplete {
    display: ${({ _display }) => _display} !important;
    flex: 0 1 168px;
  }
`

const Separator = styled.div`
  flex: 1;
`

const DocsearchInput = styled(Input)`
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`

const QueryPickerButton = styled(SecondaryButton)`
  margin: 0 1rem;
  flex: 0 0 auto;
`

const MenuIcon = styled(_MenuIcon)`
  color: ${color("draculaForeground")};
`

const CloseIcon = styled(_CloseIcon)`
  color: ${color("draculaForeground")};
`

const SideMenuMenuButton = styled(TransparentButton)`
  padding: 0;

  .fade-enter {
    opacity: 0;
  }

  .fade-enter-active {
    opacity: 1;
    transition: opacity ${TransitionDuration.REG}ms;
  }

  .fade-exit {
    opacity: 0;
  }

  .fade-exit-active {
    opacity: 1;
    transition: opacity ${TransitionDuration.REG}ms;
  }
`

const Menu = () => {
  const dispatch = useDispatch()
  const [popperActive, setPopperActive] = useState<boolean>()
  const escPress = useKeyPress("Escape")
  const { savedQueries } = useSelector(selectors.console.getConfig)
  const running = useSelector(selectors.query.getRunning)
  const opened = useSelector(selectors.console.getSideMenuOpened)
  const { sm } = useScreenSize()

  const handleClick = useCallback(() => {
    dispatch(actions.query.toggleRunning())
  }, [dispatch])
  const handleToggle = useCallback((active) => {
    setPopperActive(active)
  }, [])
  const handleHidePicker = useCallback(() => {
    setPopperActive(false)
  }, [])
  const handleSideMenuButtonClick = useCallback(() => {
    dispatch(actions.console.toggleSideMenu())
  }, [dispatch])

  useEffect(() => {
    setPopperActive(false)
  }, [escPress])

  useEffect(() => {
    docsearch({
      apiKey: "b2a69b4869a2a85284a82fb57519dcda",
      indexName: "questdb",
      inputSelector: "#docsearch-input",
      handleSelected: (input, event, suggestion, datasetNumber, context) => {
        if (context.selectionMethod === "click") {
          input.setVal("")
          const win = window.open(suggestion.url, "_blank")

          if (win) {
            win.focus()
          }
        }
      },
    })
  }, [])

  useEffect(() => {
    if (!sm && opened) {
      dispatch(actions.console.toggleSideMenu())
    }
  }, [dispatch, opened, sm])

  return (
    <Wrapper _display={sm ? "none" : "inline"}>
      {running && (
        <ErrorButton onClick={handleClick}>
          <Stop size="18px" />
          <span>Cancel</span>
        </ErrorButton>
      )}

      {!running && (
        <SuccessButton onClick={handleClick} title="Ctrl+Enter">
          <Play size="18px" />
          <span>Run</span>
        </SuccessButton>
      )}
      <Separator />

      {savedQueries.length > 0 && (
        <PopperToggle
          active={popperActive}
          onToggle={handleToggle}
          trigger={
            <QueryPickerButton onClick={handleClick}>
              <Add size="18px" />
              <span>Example queries</span>
            </QueryPickerButton>
          }
        >
          <QueryPicker hidePicker={handleHidePicker} queries={savedQueries} />
        </PopperToggle>
      )}

      <Separator />

      <DocsearchInput
        id="docsearch-input"
        placeholder="Search documentation"
        title="Search..."
      />

      {sm && (
        <SideMenuMenuButton onClick={handleSideMenuButtonClick}>
          <CSSTransition
            classNames="fade"
            in={opened}
            timeout={TransitionDuration.REG}
          >
            {opened ? <CloseIcon size="26px" /> : <MenuIcon size="26px" />}
          </CSSTransition>
        </SideMenuMenuButton>
      )}
    </Wrapper>
  )
}

export default Menu
