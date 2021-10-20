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

import React, { useCallback, useEffect } from "react"
import { createPortal } from "react-dom"
import styled from "styled-components"

import { Splitter, useScreenSize } from "components"
import { BusEvent } from "consts"

import Editor from "../Editor"
import Footer from "../Footer"
import Modal from "../Modal"
import Notifications from "../Notifications"
import Result from "../Result"
import Settings from "../Settings"
import SideMenu from "../SideMenu"
import Schema from "../Schema"
import Sidebar from "../Sidebar"
import { LocalStorageProvider } from "providers/LocalStorageProvider"
import { QuestProvider } from "providers"

const Console = styled.div`
  display: flex;
  flex-direction: column;
  flex: 1;
  max-height: 100%;
`

const Top = styled.div`
  position: relative;
  overflow: hidden;
`

const Layout = () => {
  const consoleNode = document.getElementById("console")
  const settingsNode = document.getElementById("settings")
  const sideMenuNode = document.getElementById("sideMenu")
  const modalNode = document.getElementById("modal")
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
    <LocalStorageProvider>
      <QuestProvider>
        <Sidebar />
        <Footer />
        {consoleNode &&
          createPortal(
            <Console>
              <Splitter
                direction="vertical"
                fallback={350}
                max={300}
                min={200}
                onChange={handleResultSplitterChange}
              >
                <Top>
                  <Splitter
                    direction="horizontal"
                    fallback={350}
                    max={300}
                    min={200}
                  >
                    {!sm && <Schema />}
                    <Editor />
                  </Splitter>
                </Top>
                <Result />
              </Splitter>
              <Notifications />
            </Console>,
            consoleNode,
          )}
        {sideMenuNode && createPortal(<SideMenu />, sideMenuNode)}
        {modalNode && createPortal(<Modal />, modalNode)}
        {settingsNode && createPortal(<Settings />, settingsNode)}
      </QuestProvider>
    </LocalStorageProvider>
  )
}

export default Layout
