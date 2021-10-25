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

import React, { useEffect, useRef, useState } from "react"
import { CSSTransition } from "react-transition-group"
import styled, { keyframes } from "styled-components"

import { TransitionDuration } from "components"
import { color } from "utils"

type Props = Readonly<{
  show: boolean
}>

const move = keyframes`
  0% {
    background-position: left bottom;
  }

  100% {
    background-position: right bottom;
  }
`

const Wrapper = styled.div`
  position: fixed;
  height: 0.4rem;
  top: 0;
  left: 0;
  right: 0;
  z-index: 15;
  background: linear-gradient(
      to left,
      ${color("draculaSelection")} 30%,
      ${color("draculaForeground")} 80%,
      ${color("draculaSelection")} 100%
    )
    repeat;
  background-size: 50% 100%;
  animation-name: ${move};
  animation-duration: 1s;
  animation-iteration-count: infinite;
  animation-timing-function: linear;
`

const Loader = ({ show }: Props) => {
  const [visible, setVisible] = useState(false)
  const timeoutId = useRef<number | undefined>()

  useEffect(() => {
    return () => {
      clearTimeout(timeoutId.current)
    }
  }, [])

  useEffect(() => {
    clearTimeout(timeoutId.current)

    if (!show) {
      setVisible(false)
    } else {
      timeoutId.current = window.setTimeout(() => {
        setVisible(true)
      }, 5e2)
    }
  }, [show])

  return (
    <>
      <CSSTransition
        classNames="fade-slow"
        in={visible && show}
        timeout={TransitionDuration.SLOW}
        unmountOnExit
      >
        <Wrapper />
      </CSSTransition>
    </>
  )
}

export default Loader
