import { css, keyframes } from "styled-components"

const spin = keyframes`
  from {
    transform: rotate(0deg);
  }

  to {
    transform: rotate(360deg);
  }
`

export const spinCss = css`
  animation: ${spin} 1.5s ease-in-out infinite;
`
