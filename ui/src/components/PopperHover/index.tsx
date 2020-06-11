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
import { createGlobalFadeTransition, TransitionDuration } from "../Transition"

type Props = Readonly<{
  children: ReactNode
  delay?: number
  placement: Placement
  modifiers: Options["modifiers"]
  trigger: ReactNode
}>

const GlobalTransitionStyles = createGlobalFadeTransition(
  "popper-hover-fade",
  TransitionDuration.REG,
)

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
  const { attributes, styles } = usePopper(triggerElement, container, {
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
  })

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

  usePopperStyles(container, styles.popper)

  useTransition(container, active, transitionTimeoutId)

  useEffect(() => {
    return () => {
      // eslint-disable-next-line react-hooks/exhaustive-deps
      clearTimeout(transitionTimeoutId.current)
      clearTimeout(delayTimeoutId.current)
      document.body.contains(container) && document.body.removeChild(container)
    }
  }, [container])

  return (
    <>
      <GlobalTransitionStyles />

      {React.isValidElement(trigger) &&
        React.cloneElement(trigger, {
          onMouseEnter: handleMouseEnter,
          onMouseLeave: handleMouseLeave,
          ref: active ? setTriggerElement : null,
        })}

      {React.isValidElement(children) && (
        <CSSTransition
          classNames="popper-toggle-fade"
          in={active}
          timeout={TransitionDuration.REG}
          unmountOnExit
        >
          <>
            {ReactDOM.createPortal(
              React.cloneElement(children, {
                ...attributes.popper,
                arrow: { setArrowElement, styles: styles.arrow },
              }),
              container,
            )}
          </>
        </CSSTransition>
      )}
    </>
  )
}

PopperHover.defaultProps = {
  modifiers: [],
  placement: "auto",
}
