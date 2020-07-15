import "core-js/features/promise"
import "./js/console"

import React from "react"
import ReactDOM from "react-dom"
import { Provider } from "react-redux"
import { applyMiddleware, compose, createStore } from "redux"
import { createEpicMiddleware } from "redux-observable"
import { ThemeProvider } from "styled-components"

import { ScreenSizeProvider } from "components"
import { actions, rootEpic, rootReducer } from "store"
import { StoreAction, StoreShape } from "types"

import Layout from "./scenes/Layout"
import { theme } from "./theme"

const epicMiddleware = createEpicMiddleware<
  StoreAction,
  StoreAction,
  StoreShape
>()

const store = createStore(rootReducer, compose(applyMiddleware(epicMiddleware)))

epicMiddleware.run(rootEpic)
store.dispatch(actions.console.bootstrap())

ReactDOM.render(
  <ScreenSizeProvider>
    <Provider store={store}>
      <ThemeProvider theme={theme}>
        <Layout />
      </ThemeProvider>
    </Provider>
  </ScreenSizeProvider>,
  document.getElementById("root"),
)
