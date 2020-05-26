import type { Placement, Options } from "@popperjs/core"
import React, { useCallback, useEffect, useState } from "react"
import ReactDOM from "react-dom"
import { usePopper } from "react-popper"

type Props = Readonly<{
  children: React.ReactNode
  placement: Placement
  modifiers: Options["modifiers"]
  trigger: React.ReactNode
}>

export const PopperHover = ({
  children,
  modifiers,
  placement,
  trigger,
}: Props) => {
  const [container] = useState<HTMLElement>(document.createElement("div"))
  const [active, setActive] = React.useState(false)
  const [triggerElement, setTriggerElement] = useState<HTMLElement | null>(null)
  const { attributes, styles } = usePopper(triggerElement, container, {
    modifiers: [
      ...modifiers,
      {
        name: "eventListeners",
        enabled: active,
      },
    ],
    placement,
  })
  const handleMouseEnter = useCallback(() => {
    setActive(true)
  }, [])
  const handleMouseLeave = useCallback(() => {
    setActive(false)
  }, [])

  useEffect(() => {
    if (active && !document.body.contains(container)) {
      document.body.appendChild(container)
    } else {
      document.body.contains(container) && document.body.removeChild(container)
    }
  }, [active])

  useEffect(() => {
    return () => {
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
      {React.isValidElement(trigger) &&
        React.cloneElement(trigger, {
          onMouseEnter: handleMouseEnter,
          onMouseLeave: handleMouseLeave,
          ref: setTriggerElement,
        })}

      {React.isValidElement(children) &&
        ReactDOM.createPortal(
          React.cloneElement(children, {
            ...attributes.popper,
          }),
          container,
        )}
    </>
  )
}

PopperHover.defaultProps = {
  modifiers: [],
  placement: "auto",
}
