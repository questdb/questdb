import "core-js/features/promise"
import "./js/console"

import React from "react"
import ReactDOM from "react-dom"
import { ThemeProvider } from "styled-components"

import Layout from "./scenes/Layout"
import { theme } from "./theme"

ReactDOM.render(
  <ThemeProvider theme={theme}>
    <Layout />
  </ThemeProvider>,
  document.getElementById("root"),
)
