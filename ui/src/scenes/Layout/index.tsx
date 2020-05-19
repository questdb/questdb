import React from "react"
import { createPortal } from "react-dom"

import Schema from "../Schema"

const Layout = () => {
  const schemaNode = document.getElementById("schema-content")

  return <>{schemaNode && createPortal(<Schema />, schemaNode)}</>
}

export default Layout
