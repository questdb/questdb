import React, { useCallback, useState } from "react"
import { createPortal } from "react-dom"

import { Splitter } from "components"

import Editor from "../Editor"
import Footer from "../Footer"
import Notifications from "../Notifications"
import Schema from "../Schema"
import Sidebar from "../Sidebar"

const Layout = () => {
  const editorNode = document.getElementById("editor")
  const footerNode = document.getElementById("footer")
  const notificationsNode = document.getElementById("notifications")
  const [schemaWidthOffset, setSchemaWidthOffset] = useState<number>()
  const handleSplitterChange = useCallback((offset) => {
    setSchemaWidthOffset(offset)
  }, [])

  return (
    <>
      <Sidebar />
      {editorNode &&
        createPortal(
          <>
            <Schema widthOffset={schemaWidthOffset} />
            <Splitter max={300} min={200} onChange={handleSplitterChange} />
            <Editor />
          </>,
          editorNode,
        )}
      {footerNode && createPortal(<Footer />, footerNode)}
      {notificationsNode && createPortal(<Notifications />, notificationsNode)}
    </>
  )
}

export default Layout
