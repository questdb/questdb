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

import { Range } from "ace-builds"
import React, {
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react"
import ReactAce from "react-ace"
import { useDispatch, useSelector } from "react-redux"
import ResizeObserver from "resize-observer-polyfill"
import styled from "styled-components"

import { PaneContent, Text } from "components"
import { BusEvent } from "consts"
import { actions, selectors } from "store"
import { theme } from "theme"
import { NotificationType } from "types"
import { color, ErrorResult } from "utils"
import * as QuestDB from "utils/questdb"

import Loader from "../Loader"
import QueryResult from "../QueryResult"
import questdbMode from "./questdbMode"
import {
  getQueryFromCursor,
  getQueryFromSelection,
  Request,
  toTextPosition,
} from "./utils"
import { usePreferences } from "./usePreferences"
import { QuestContext } from "providers"

const Content = styled(PaneContent)`
  position: relative;

  .ace-dracula .ace_marker-layer .ace_selected-word {
    box-shadow: inset 0px 0px 0px 1px ${color("draculaOrange")};
    transform: scale(1.05);
    z-index: 6;
  }

  .syntax-error {
    position: absolute;
    border-bottom: 1px solid ${color("draculaRed")};
    cursor: pointer;
    pointer-events: auto;
  }
`

enum Command {
  EXECUTE = "execute",
  EXECUTE_AT = "execute_at",
  FOCUS_GRID = "focus_grid",
  CLEANUP_NOTIFICATIONS = "clean_notifications",
}

const Ace = () => {
  const { loadPreferences, savePreferences } = usePreferences()
  const { quest } = useContext(QuestContext)
  const [request, setRequest] = useState<Request | undefined>()
  const [value, setValue] = useState("")
  const aceEditor = useRef<ReactAce | null>(null)
  const wrapper = useRef<HTMLDivElement | null>(null)
  const dispatch = useDispatch()
  const running = useSelector(selectors.query.getRunning)

  const handleChange = useCallback((value) => {
    setValue(value)
  }, [])

  useEffect(() => {
    if (!running && request) {
      quest.abort()
      dispatch(actions.query.stopRunning())
      setRequest(undefined)
    }
  }, [request, quest, dispatch, running])

  // Save preferences when we run a query
  useEffect(() => {
    const editor = aceEditor?.current?.editor

    if (running && editor) {
      savePreferences(editor)
    }
  }, [running, savePreferences])

  useEffect(() => {
    const editor = aceEditor?.current?.editor

    if (running && editor) {
      const markers = editor.session.getMarkers(true)

      if (markers) {
        Object.keys(markers).forEach((marker) => {
          editor.session.removeMarker(parseInt(marker, 10))
        })
      }

      const request =
        editor.getSelectedText().length === 0
          ? getQueryFromCursor(editor)
          : getQueryFromSelection(editor)

      if (request?.query) {
        void quest
          .queryRaw(request.query, { limit: "0,1000" })
          .then((result) => {
            setRequest(undefined)
            dispatch(actions.query.stopRunning())
            dispatch(actions.query.setResult(result))

            if (result.type === QuestDB.Type.DDL) {
              dispatch(
                actions.query.addNotification({
                  content: (
                    <Text color="draculaForeground" ellipsis>
                      {result.query}
                    </Text>
                  ),
                }),
              )
            }

            if (result.type === QuestDB.Type.DQL) {
              dispatch(
                actions.query.addNotification({
                  content: (
                    <QueryResult {...result.timings} rowCount={result.count} />
                  ),
                  sideContent: (
                    <Text color="draculaForeground" ellipsis>
                      {result.query}
                    </Text>
                  ),
                }),
              )
              bus.trigger(BusEvent.MSG_QUERY_DATASET, result)
            }
          })
          .catch((error: ErrorResult) => {
            setRequest(undefined)
            dispatch(actions.query.stopRunning())
            dispatch(
              actions.query.addNotification({
                content: <Text color="draculaRed">{error.error}</Text>,
                sideContent: (
                  <Text color="draculaForeground" ellipsis>
                    {request.query}
                  </Text>
                ),
                type: NotificationType.ERROR,
              }),
            )

            const position = toTextPosition(request, error.position)
            const token = editor.session.getTokenAt(
              position.row - 1,
              position.column,
            ) ?? {
              value: "",
            }
            const range = new Range(
              position.row - 1,
              position.column - 1,
              position.row - 1,
              position.column + token.value.length - 1,
            )
            editor.session.addMarker(range, "syntax-error", "text", true)
            editor.gotoLine(position.row, position.column - 1, true)
            editor.focus()
          })

        setRequest(request)
      } else {
        dispatch(actions.query.stopRunning())
      }
    }
  }, [quest, dispatch, running])

  useEffect(() => {
    if (!aceEditor.current) {
      return
    }

    const { editor } = aceEditor.current

    const toggleRunning = () => {
      dispatch(actions.query.toggleRunning())
    }
    const ro = new ResizeObserver(() => {
      editor.resize()
    })

    if (wrapper.current) {
      ro.observe(wrapper.current)
    }

    editor.container.style.lineHeight = "1.5"
    editor.renderer.updateFontSize()
    editor.getSession().setMode(questdbMode)

    editor.commands.addCommand({
      bindKey: "F9",
      name: Command.EXECUTE,
      exec: toggleRunning,
    })

    editor.commands.addCommand({
      bindKey: {
        mac: "Command-Enter",
        win: "Ctrl-Enter",
      },
      exec: toggleRunning,
      name: Command.EXECUTE_AT,
    })

    editor.commands.addCommand({
      bindKey: "F2",
      exec: () => {
        window.bus.trigger("grid.focus")
      },
      name: Command.FOCUS_GRID,
    })

    editor.commands.addCommand({
      bindKey: {
        mac: "Command-K",
        win: "Ctrl-K",
      },
      exec: () => {
        dispatch(actions.query.cleanupNotifications())
      },
      name: Command.CLEANUP_NOTIFICATIONS,
    })

    window.bus.on(BusEvent.MSG_QUERY_FIND_N_EXEC, (_event, query: string) => {
      const row = editor.session.getLength()
      const text = `\n${query};`

      editor.find(`'${query}'`, {
        wrap: true,
        caseSensitive: true,
        preventScroll: false,
      })

      editor.session.insert({ column: 0, row }, text)
      editor.selection.moveCursorToPosition({ column: 0, row: row + 1 })
      editor.selection.selectLine()

      toggleRunning()
    })

    window.bus.on(BusEvent.MSG_QUERY_EXEC, (_event, query: string) => {
      editor.find(`'${query}'`, {
        wrap: true,
        caseSensitive: true,
        preventScroll: false,
      })

      toggleRunning()
    })

    window.bus.on(
      BusEvent.MSG_QUERY_EXPORT,
      (_event, request?: { q: string }) => {
        if (request) {
          window.location.href = `/exp?query=${encodeURIComponent(request.q)}`
        }
      },
    )

    window.bus.on("editor.insert.column", (_event, column) => {
      editor.insert(column)
      editor.focus()
    })

    window.bus.on(BusEvent.MSG_EDITOR_FOCUS, () => {
      editor.scrollToLine(
        editor.getCursorPosition().row + 1,
        true,
        true,
        () => {},
      )
      editor.focus()
    })
    window.bus.on("preferences.load", () => {
      loadPreferences(editor)
    })
    window.bus.on("preferences.save", () => {
      savePreferences(editor)
    })
    window.bus.on("editor.set", (_event, query) => {
      if (query) {
        editor.setValue(query)
      }
    })

    return () => {
      ro.disconnect()
    }

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  return (
    <Content ref={wrapper}>
      <ReactAce
        editorProps={{
          $blockScrolling: Infinity,
          $displayIndentGuide: true,
        }}
        fontSize={theme.fontSize.md}
        height="100%"
        mode="text"
        name="questdb-sql-editor"
        onChange={handleChange}
        ref={aceEditor}
        setOptions={{
          fontFamily: theme.fontMonospace,
          highlightActiveLine: false,
          showLineNumbers: true,
          showPrintMargin: false,
        }}
        theme="dracula"
        value={value}
        width="100%"
      />
      <Loader show={!!request} />
    </Content>
  )
}

export default Ace
