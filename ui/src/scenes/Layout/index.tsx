import React, { useCallback, useEffect, useRef, useState } from "react"
import { createPortal } from "react-dom"
import styled from "styled-components"

import { Splitter } from "components"
import { BusEvent } from "utils"

import Editor from "../Editor"
import Footer from "../Footer"
import Notifications from "../Notifications"
import Result from "../Result"
import Schema from "../Schema"
import Sidebar from "../Sidebar"

const Top = styled.div<{
  basis?: number
}>`
  position: relative;
  display: flex;
  flex-grow: 0;
  flex-shrink: 1;
  ${({ basis }) => basis && `flex-basis: ${basis}px`};
  overflow: hidden;
`

const Layout = () => {
  const consoleNode = document.getElementById("console")
  const footerNode = document.getElementById("footer")
  const notificationsNode = document.getElementById("notifications")
  const [schemaWidthOffset, setSchemaWidthOffset] = useState<number>()
  const [topHeightOffset, setTopHeightOffset] = useState<number>()
  const topElement = useRef<HTMLDivElement | null>(null)

  const handleSchemaSplitterChange = useCallback((offset) => {
    setSchemaWidthOffset(offset)
  }, [])
  const handleResultSplitterChange = useCallback((change: number) => {
    if (topElement.current) {
      const offset = topElement.current.getBoundingClientRect().height + change
      localStorage.setItem("splitter.position", `${offset}`)
      setTopHeightOffset(offset)
      setTimeout(() => {
        window.bus.trigger(BusEvent.MSG_ACTIVE_PANEL)
      }, 0)
    }
  }, [])

  useEffect(() => {
    const size = parseInt(localStorage.getItem("splitter.position") || "0", 10)

    if (size) {
      setTopHeightOffset(size)
    } else {
      setTopHeightOffset(350)
    }
  }, [])

  useEffect(() => {
    if (topElement.current) {
      window.bus.trigger(BusEvent.REACT_READY)
    }
  }, [topElement])

  return (
    <>
      <Sidebar />
      {consoleNode &&
        createPortal(
          <>
            <Top basis={topHeightOffset} ref={topElement}>
              <Schema widthOffset={schemaWidthOffset} />
              <Splitter
                direction="horizontal"
                max={300}
                min={200}
                onChange={handleSchemaSplitterChange}
              />
              <Editor />
            </Top>
            <Splitter
              direction="vertical"
              max={300}
              min={200}
              onChange={handleResultSplitterChange}
            />
            <Result />
          </>,
          consoleNode,
        )}
      {footerNode && createPortal(<Footer />, footerNode)}
      {notificationsNode && createPortal(<Notifications />, notificationsNode)}
    </>
  )
}

export default Layout
