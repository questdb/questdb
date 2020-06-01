import { createGlobalStyle, css } from "styled-components"

export enum TransitionDuration {
  SLOW = 1500,
  REG = 200,
  FAST = 70,
}

export const createGlobalFadeTransition = (
  duration: TransitionDuration,
) => createGlobalStyle`
  .fade-enter {
    opacity: 0;
  }

  .fade-enter-active {
    opacity: 1;
    transition: all ${duration}ms;
  }

  .fade-exit {
    opacity: 1;
  }

  .fade-exit-active {
    opacity: 0;
    transition: all ${duration}ms;
  }
`

export const slideTransition = css<{ _height: number }>`
  .slide-enter {
    max-height: 0;
  }

  .slide-enter-active {
    max-height: ${({ _height }) => _height}px;
    transition: all ${TransitionDuration.REG}ms;
  }

  .slide-exit {
    max-height: ${({ _height }) => _height}px;
  }

  .slide-exit-active {
    max-height: 0;
    transition: all ${TransitionDuration.REG}ms;
  }
`

export const buttonTransition = css`
  transition: all ${TransitionDuration.FAST}ms cubic-bezier(0, 0, 0.38, 0.9);
`
