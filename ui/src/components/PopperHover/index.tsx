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
import { createGlobalStyle } from "styled-components"

type Props = Readonly<{
  children: ReactNode
  delay?: number
  placement: Placement
  modifiers: Options["modifiers"]
  strategy: "absolute" | "fixed"
  trigger: ReactNode
}>

const TRANSITION_MS = 200

const GlobalTransitionCss = createGlobalStyle`
  .fade-enter {
    opacity: 0;
  }

  .fade-enter-active {
    opacity: 1;
    transition: all ${TRANSITION_MS}ms;
  }

  .fade-exit {
    opacity: 1;
  }

  .fade-exit-active {
    opacity: 0;
    transition: all ${TRANSITION_MS}ms;
  }
`

export const PopperHover = ({
  children,
  delay,
  modifiers,
  placement,
  strategy,
  trigger,
}: Props) => {
  const [container] = useState<HTMLElement>(document.createElement("div"))
  const delayTimeoutId = useRef<number | undefined>()
  const transitionTimeoutId = useRef<number | undefined>()
  const [active, setActive] = useState(false)
  const [ready, setReady] = useState(false)
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
        enabled: ready,
      },
    ],
    placement,
    strategy,
  })
  const handleMouseEnter = useCallback(() => {
    if (delay) {
      delayTimeoutId.current = setTimeout(() => {
        setActive(true)
      }, delay)
    } else {
      setActive(true)
    }
  }, [])
  const handleMouseLeave = useCallback(() => {
    clearTimeout(delayTimeoutId.current)
    setReady(false)
    setActive(false)
  }, [])

  useEffect(() => {
    if (active && !document.body.contains(container)) {
      document.body.appendChild(container)
      setReady(true)
    } else {
      transitionTimeoutId.current = setTimeout(() => {
        document.body.contains(container) &&
          document.body.removeChild(container)
      }, 200)
    }
  }, [active])

  useEffect(() => {
    return () => {
      clearTimeout(delayTimeoutId.current)
      clearTimeout(transitionTimeoutId.current)
      document.body.contains(container) && document.body.removeChild(container)
    }
  }, [])

  useEffect(() => {
    const css = Object.entries(styles.popper).reduce(
      (acc, [prop, value]: [string, string]) => `${acc} ${prop}: ${value};`,
      "z-index: 100;",
    )
    container.setAttribute("style", css)
  }, [styles.popper])

  return (
    <>
      <GlobalTransitionCss />

      {React.isValidElement(trigger) &&
        React.cloneElement(trigger, {
          onMouseEnter: handleMouseEnter,
          onMouseLeave: handleMouseLeave,
          ref: ready ? setTriggerElement : null,
        })}

      {React.isValidElement(children) && (
        <CSSTransition
          classNames="fade"
          in={ready}
          timeout={TRANSITION_MS}
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
  strategy: "fixed",
}
