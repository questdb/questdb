import { css, keyframes } from "styled-components"

const spin = keyframes`
  from {
    transform: rotate(0deg);
  }

  to {
    transform: rotate(360deg);
  }
`

export const spinAnimation = css`
  animation: ${spin} 1.5s cubic-bezier(0.62, 0.28, 0.23, 0.99) infinite;
`
