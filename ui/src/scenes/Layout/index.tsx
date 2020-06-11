import React, { useCallback, useEffect } from "react"
import { createPortal } from "react-dom"
import styled from "styled-components"

import { Splitter, useScreenSize } from "components"
import { BusEvent } from "utils"

import Editor from "../Editor"
import Footer from "../Footer"
import Notifications from "../Notifications"
import Result from "../Result"
import SideMenu from "../SideMenu"
import Schema from "../Schema"
import Sidebar from "../Sidebar"

const Top = styled.div`
  position: relative;
  overflow: hidden;
`

const Layout = () => {
  const consoleNode = document.getElementById("console")
  const footerNode = document.getElementById("footer")
  const notificationsNode = document.getElementById("notifications")
  const sideMenuNode = document.getElementById("sideMenu")
  const { sm } = useScreenSize()

  const handleResultSplitterChange = useCallback(() => {
    setTimeout(() => {
      window.bus.trigger(BusEvent.MSG_ACTIVE_PANEL)
    }, 0)
  }, [])

  useEffect(() => {
    window.bus.trigger(BusEvent.REACT_READY)
  }, [])

  return (
    <>
      <Sidebar />
      {consoleNode &&
        createPortal(
          <Splitter
            direction="vertical"
            fallback={350}
            max={300}
            min={200}
            name="position" /* "position" is for legacy reasons */
            onChange={handleResultSplitterChange}
          >
            <Top>
              <Splitter
                direction="horizontal"
                fallback={350}
                max={300}
                min={200}
                name="schema"
              >
                {sm === false && <Schema />}
                <Editor />
              </Splitter>
            </Top>
            <Result />
          </Splitter>,
          consoleNode,
        )}
      {footerNode && createPortal(<Footer />, footerNode)}
      {notificationsNode && createPortal(<Notifications />, notificationsNode)}
      {sideMenuNode && createPortal(<SideMenu />, sideMenuNode)}
    </>
  )
}

export default Layout
