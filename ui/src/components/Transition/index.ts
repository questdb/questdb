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

import { createGlobalStyle, css } from "styled-components"

export enum TransitionDuration {
  SLOW = 1500,
  REG = 200,
  FAST = 70,
}

export const createGlobalFadeTransition = (
  className: string,
  duration: TransitionDuration,
) => createGlobalStyle`
  .${className}-enter {
    opacity: 0;
  }

  .${className}-enter-active {
    opacity: 1;
    transition: all ${duration}ms;
  }

  .${className}-exit {
    opacity: 1;
  }

  .${className}-exit-active {
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
