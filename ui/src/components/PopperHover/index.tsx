import type { Placement, Options } from "@popperjs/core"
import React, { useCallback, useEffect, useState } from "react"
import ReactDOM from "react-dom"
import { usePopper } from "react-popper"
import styled from "styled-components"

const Wrapper = styled.div`
  display: flex;
  align-items: center;
`

type Props = Readonly<{
  children: React.ReactNode
  placement: Placement
  modifiers?: Options["modifiers"]
  trigger: React.ReactNode
}>

export const PopperHover = ({
  children,
  modifiers,
  placement,
  trigger,
}: Props) => {
  const [container] = useState<Element>(document.createElement("div"))
  const [active, setActive] = React.useState(false)
  const [triggerElement, setTriggerElement] = useState<HTMLElement | null>(null)
  const [popperElement, setPopperElement] = useState<HTMLElement | null>(null)
  const { attributes, styles } = usePopper(triggerElement, popperElement, {
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

  return (
    <>
      <Wrapper
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        ref={setTriggerElement}
      >
        {trigger}
      </Wrapper>

      {ReactDOM.createPortal(
        <div
          ref={setPopperElement}
          style={{ ...styles.popper, zIndex: 100 }}
          {...attributes.popper}
        >
          {children}
        </div>,
        container,
      )}
    </>
  )
}

PopperHover.defaultProps = {
  placement: "auto",
}
