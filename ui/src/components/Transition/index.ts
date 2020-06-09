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

export const collapseTransition = css<{ duration?: number; _height: number }>`
  .collapse-enter {
    max-height: 0;
  }

  .collapse-enter-active {
    max-height: ${({ _height }) => _height}px;
    transition: all ${({ duration }) => duration || TransitionDuration.REG}ms;
  }

  .collapse-exit {
    max-height: ${({ _height }) => _height}px;
  }

  .collapse-exit-active {
    max-height: 0;
    transition: all ${({ duration }) => duration || TransitionDuration.REG}ms;
  }
`

export const slideTransition = css<{ left: number }>`
  .slide-enter {
    opacity: 0;
  }

  .slide-enter-active {
    opacity: 1;
    transition: all ${TransitionDuration.FAST}ms cubic-bezier(0, 0, 0.38, 0.9);
  }

  .slide-exit {
    transform: translate(0, 0);
  }

  .slide-exit-active {
    transform: translate(${({ left }) => left}px, 0);
    transition: all ${TransitionDuration.FAST}ms cubic-bezier(0, 0, 0.38, 0.9);
  }
`

export const bezierTransition = css`
  transition: all ${TransitionDuration.FAST}ms cubic-bezier(0, 0, 0.38, 0.9);
`
