/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import type { Placement, Options } from "@popperjs/core"
import React, {
  ReactNode,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react"
import ReactDOM from "react-dom"
import { usePopper } from "react-popper"
import { CSSTransition } from "react-transition-group"

import { usePopperStyles, useTransition } from "../Hooks"
import { TransitionDuration } from "../Transition"

type Props = Readonly<{
  active?: boolean
  children: ReactNode
  placement: Placement
  modifiers: Options["modifiers"]
  onToggle?: (_active: boolean) => void
  trigger: ReactNode
}>

export const PopperToggle = ({
  active,
  children,
  modifiers,
  onToggle,
  placement,
  trigger,
}: Props) => {
  const [container] = useState<HTMLElement>(document.createElement("div"))
  const transitionTimeoutId = useRef<number | undefined>()
  const [_active, setActive] = useState(false)
  const [triggerElement, setTriggerElement] = useState<HTMLElement | null>(null)
  const { attributes, styles } = usePopper(triggerElement, container, {
    modifiers: [
      ...modifiers,
      {
        name: "eventListeners",
        enabled: _active,
      },
    ],
    placement,
  })

  const handleClick = useCallback(() => {
    const state = !_active
    setActive(state)

    if (onToggle) {
      onToggle(state)
    }
  }, [_active, onToggle])

  const handleMouseDown = useCallback(
    (event: TouchEvent | MouseEvent) => {
      const target = event.target as Element

      if (container.contains(target) || triggerElement?.contains(target)) {
        return
      }

      setActive(false)

      if (onToggle) {
        onToggle(false)
      }
    },
    [container, onToggle, triggerElement],
  )

  usePopperStyles(container, styles.popper)

  useTransition(container, _active, transitionTimeoutId)

  useEffect(() => {
    setActive(typeof active === "undefined" ? _active || false : active)
  }, [active, _active])

  useEffect(() => {
    document.addEventListener("mousedown", handleMouseDown)
    document.addEventListener("touchstart", handleMouseDown)

    return () => {
      // eslint-disable-next-line react-hooks/exhaustive-deps
      clearTimeout(transitionTimeoutId.current)
      document.removeEventListener("mousedown", handleMouseDown)
      document.removeEventListener("touchstart", handleMouseDown)
      document.body.contains(container) && document.body.removeChild(container)
    }
  }, [container, handleMouseDown])

  return (
    <>
      {React.isValidElement(trigger) &&
        React.cloneElement(trigger, {
          onClick: handleClick,
          ref: setTriggerElement,
        })}

      {React.isValidElement(children) && (
        <CSSTransition
          classNames="fade-reg"
          in={_active}
          timeout={TransitionDuration.REG}
          unmountOnExit
        >
          <>
            {ReactDOM.createPortal(
              React.cloneElement(children, {
                ...attributes.popper,
              }),
              container,
            )}
          </>
        </CSSTransition>
      )}
    </>
  )
}

PopperToggle.defaultProps = {
  modifiers: [],
  placement: "auto",
}
