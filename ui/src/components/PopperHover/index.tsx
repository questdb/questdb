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
  children: ReactNode
  delay?: number
  placement: Placement
  modifiers: Options["modifiers"]
  trigger: ReactNode
}>

export const PopperHover = ({
  children,
  delay,
  modifiers,
  placement,
  trigger,
}: Props) => {
  const [container] = useState<HTMLElement>(document.createElement("div"))
  const delayTimeoutId = useRef<number | undefined>()
  const transitionTimeoutId = useRef<number | undefined>()
  const [active, setActive] = useState(false)
  const [triggerElement, setTriggerElement] = useState<HTMLElement | null>(null)
  const [arrowElement, setArrowElement] = useState<HTMLElement | null>(null)
  const { attributes, styles, forceUpdate } = usePopper(
    triggerElement,
    container,
    {
      modifiers: [
        ...modifiers,
        {
          name: "arrow",
          options: { element: arrowElement },
        },
        {
          name: "offset",
          options: { offset: [0, 6] },
        },
        {
          name: "eventListeners",
          enabled: active,
        },
      ],
      placement,
    },
  )

  const handleMouseEnter = useCallback(() => {
    if (delay) {
      delayTimeoutId.current = setTimeout(() => {
        setActive(true)
      }, delay)
    } else {
      setActive(true)
    }
  }, [delay])

  const handleMouseLeave = useCallback(() => {
    clearTimeout(delayTimeoutId.current)
    setActive(false)
  }, [])

  useEffect(() => {
    return () => {
      // eslint-disable-next-line react-hooks/exhaustive-deps
      clearTimeout(transitionTimeoutId.current)
      clearTimeout(delayTimeoutId.current)
      document.body.contains(container) && document.body.removeChild(container)
    }
  }, [container])

  usePopperStyles(container, styles.popper)

  useTransition(container, active, transitionTimeoutId, forceUpdate)

  return (
    <>
      {React.isValidElement(trigger) &&
        React.cloneElement(trigger, {
          onMouseEnter: handleMouseEnter,
          onMouseLeave: handleMouseLeave,
          ref: setTriggerElement,
        })}

      {ReactDOM.createPortal(
        <CSSTransition
          classNames="popper-fade"
          in={active}
          timeout={TransitionDuration.REG}
          unmountOnExit
        >
          {React.isValidElement(children) &&
            React.cloneElement(children, {
              ...attributes.popper,
              arrow: { setArrowElement, styles: styles.arrow },
            })}
        </CSSTransition>,
        container,
      )}
    </>
  )
}

PopperHover.defaultProps = {
  modifiers: [],
  placement: "auto",
}
