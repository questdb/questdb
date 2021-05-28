import React from "react"
import { ContextMenu, MenuItem } from "components/ContextMenu"

type Props = {
  name: string
}

const ContextualMenu = ({ name }: Props) => {
  return (
    <ContextMenu id={name}>
      <MenuItem data={{ foo: "bar" }}>Query</MenuItem>
      <MenuItem data={{ foo: "bar" }}>Copy Schema To Clipboard</MenuItem>
      <MenuItem divider />
      <MenuItem data={{ foo: "bar" }}>Close</MenuItem>
    </ContextMenu>
  )
}

export default ContextualMenu
