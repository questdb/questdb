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
  MouseEvent,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react"
import { useDispatch, useSelector } from "react-redux"
import { CSSTransition } from "react-transition-group"
import styled from "styled-components"

import { TransitionDuration } from "components"
import { ModalId } from "consts"
import { actions, selectors } from "store"

import PowerUser from "./PowerUser"

const Backdrop = styled.div<{ active: boolean }>`
  position: fixed;
  display: flex;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  align-items: center;
  justify-content: center;
  z-index: ${({ active }) => (active ? 50 : -1)};
  background: ${({ active, theme }) =>
    active ? theme.color.blackAlpha40 : theme.color.transparent};
  transition: background ${TransitionDuration.REG}ms
    cubic-bezier(0, 0, 0.38, 0.9);
`

const ModalComp = {
  [ModalId.POWER_USER]: PowerUser,
}

const Modal = () => {
  const [ready, setReady] = useState(false)
  const modalId = useSelector(selectors.console.getModalId)
  const modalNode = useRef<HTMLDivElement | null>(null)
  const dispatch = useDispatch()
  const handleClick = useCallback(
    (event: MouseEvent) => {
      if (ModalId.POWER_USER === modalId) {
        return
      }

      if (
        modalNode.current != null &&
        event.target instanceof HTMLElement &&
        !modalNode.current.contains(event.target)
      ) {
        dispatch(actions.console.setModalId())
      }
    },
    [dispatch, modalId],
  )

  useEffect(() => {
    setTimeout(() => {
      setReady(true)
    }, 10)
  }, [])

  return (
    <CSSTransition
      classNames="fade-reg"
      in={ready && modalId != null}
      timeout={TransitionDuration.REG}
      unmountOnExit
    >
      <Backdrop active={ready} onClick={handleClick}>
        {modalId != null &&
          React.createElement(ModalComp[modalId], {
            ref: modalNode,
          })}
      </Backdrop>
    </CSSTransition>
  )
}

export default Modal
