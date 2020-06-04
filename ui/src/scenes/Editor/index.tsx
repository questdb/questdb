import React from "react"

import { PaneWrapper } from "components"

import Ace from "./Ace"
import Menu from "./Menu"

const Editor = () => (
  <PaneWrapper>
    <Menu />
    <Ace />
  </PaneWrapper>
)

export default Editor
