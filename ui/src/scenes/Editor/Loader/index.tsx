import React from "react"
import styled, { keyframes } from "styled-components"

import { color } from "utils"

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
  z-index: 5;
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

const Loader = () => <Wrapper />

export default Loader
