import React from "react"
import { createPortal } from "react-dom"

import Editor from "../Editor"
import Footer from "../Footer"
import Schema from "../Schema"
import Sidebar from "../Sidebar"

const Layout = () => {
  const schemaNode = document.getElementById("schema-content")
  const editorNode = document.getElementById("editor-pane-title")
  const footerNode = document.getElementById("footer")

  return (
    <>
      <Sidebar />
      {schemaNode && createPortal(<Schema />, schemaNode)}
      {editorNode && createPortal(<Editor />, editorNode)}
      {footerNode && createPortal(<Footer />, footerNode)}
    </>
  )
}

export default Layout
