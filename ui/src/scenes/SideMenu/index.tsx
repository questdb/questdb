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
import { useDispatch, useSelector } from "react-redux"
import { CSSTransition } from "react-transition-group"
import styled from "styled-components"

import { TransitionDuration } from "components"
import { actions, selectors } from "store"
import { color } from "utils"

import Schema from "../Schema"

const WIDTH = 280

const Backdrop = styled.div`
  position: fixed;
  top: 4rem;
  right: 0;
  bottom: 0;
  left: 0;
  background: rgba(0, 0, 0, 0.3);
  z-index: 24;

  &:hover {
    cursor: pointer;
  }
`

const Wrapper = styled.div`
  position: fixed;
  top: 4rem;
  right: 0;
  bottom: 0;
  width: ${WIDTH}px;
  background: ${color("draculaBackgroundDarker")};
  border-left: 1px solid ${color("black")};
  z-index: 25;

  &.side-menu-slide-enter {
    width: 0;
  }

  &.side-menu-slide-enter-active {
    width: ${WIDTH}px;
    transition: width ${TransitionDuration.REG}ms;
  }

  &.side-menu-slide-exit {
    width: ${WIDTH}px;
  }

  &.side-menu-slide-exit-active {
    width: 0;
    transition: width ${TransitionDuration.REG}ms;
  }
`

const SideMenu = () => {
  const opened = useSelector(selectors.console.getSideMenuOpened)
  const dispatch = useDispatch()
  const handleBackdropClick = useCallback(() => {
    dispatch(actions.console.toggleSideMenu())
  }, [dispatch])

  return (
    <>
      <CSSTransition
        classNames="fade-reg"
        in={opened}
        timeout={TransitionDuration.REG}
        unmountOnExit
      >
        <Backdrop onClick={handleBackdropClick} />
      </CSSTransition>

      <CSSTransition
        classNames="side-menu-slide"
        in={opened}
        timeout={TransitionDuration.REG}
        unmountOnExit
      >
        <Wrapper>
          <Schema />
        </Wrapper>
      </CSSTransition>
    </>
  )
}

export default SideMenu
