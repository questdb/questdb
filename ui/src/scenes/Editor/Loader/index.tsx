import React, { useEffect, useRef, useState } from "react"
import { CSSTransition } from "react-transition-group"
import styled, { keyframes } from "styled-components"

import { createGlobalFadeTransition, TransitionDuration } from "components"
import { color } from "utils"

type Props = Readonly<{
  show: boolean
}>

const GlobalTransitionStyles = createGlobalFadeTransition(
  "editor-loader-fade",
  TransitionDuration.SLOW,
)

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
      timeoutId.current = setTimeout(() => {
        setVisible(true)
      }, 5e2)
    }
  }, [show])

  return (
    <>
      <GlobalTransitionStyles />
      <CSSTransition
        classNames="editor-loader-fade"
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
