import React, { useContext, useEffect, useState } from "react"
import Editor, { Monaco } from "@monaco-editor/react"
import dracula from "./dracula"
import { editor } from "monaco-editor"
import { theme } from "../../../theme"
import { QuestContext, useEditor } from "../../../providers"
import { usePreferences } from "./usePreferences"
import {
  getQueryRequestFromEditor,
  getQueryRequestFromLastExecutedQuery,
  Request,
} from "./utils"
import { PaneContent, Text } from "components"
import { useDispatch, useSelector } from "react-redux"
import { actions, selectors } from "../../../store"
import { BusEvent } from "consts"
import { ErrorResult } from "utils/questdb"
import * as QuestDB from "utils/questdb"
import { NotificationType } from "types"
import QueryResult from "../QueryResult"
import Loader from "../Loader"
import styled from "styled-components"

type IStandaloneCodeEditor = editor.IStandaloneCodeEditor

const Content = styled(PaneContent)`
  position: relative;
  overflow: hidden;
`

const MonacoEditor = () => {
  const { editorRef, insertTextAtCursor } = useEditor()
  const { loadPreferences, savePreferences } = usePreferences()
  const { quest } = useContext(QuestContext)
  const [request, setRequest] = useState<Request | undefined>()
  const [lastExecutedQuery, setLastExecutedQuery] = useState("")
  const dispatch = useDispatch()
  const running = useSelector(selectors.query.getRunning)

  const handleEditorDidMount = (
    editor: IStandaloneCodeEditor,
    monaco: Monaco,
  ) => {
    if (editorRef) {
      editorRef.current = editor

      // Support legacy bus events for non-react codebase
      window.bus.on(BusEvent.MSG_EDITOR_INSERT_COLUMN, (_event, column) => {
        insertTextAtCursor(column)
      })
    }
    monaco.editor.defineTheme("dracula", dracula)
    monaco.editor.setTheme("dracula")

    loadPreferences(editor)
  }

  useEffect(() => {
    if (!running.value && request) {
      quest.abort()
      dispatch(actions.query.stopRunning())
      setRequest(undefined)
    }
  }, [request, quest, dispatch, running])

  useEffect(() => {
    if (running.value && editorRef?.current) {
      const request = running.isRefresh
        ? getQueryRequestFromLastExecutedQuery(lastExecutedQuery)
        : getQueryRequestFromEditor(editorRef.current)

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
                    <Text
                      color="draculaForeground"
                      ellipsis
                      title={result.query}
                    >
                      {result.query}
                    </Text>
                  ),
                }),
              )
              bus.trigger(BusEvent.MSG_QUERY_SCHEMA)
            }

            if (result.type === QuestDB.Type.DQL) {
              setLastExecutedQuery(request.query)
              dispatch(
                actions.query.addNotification({
                  content: (
                    <QueryResult {...result.timings} rowCount={result.count} />
                  ),
                  sideContent: (
                    <Text
                      color="draculaForeground"
                      ellipsis
                      title={result.query}
                    >
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
                  <Text
                    color="draculaForeground"
                    ellipsis
                    title={request.query}
                  >
                    {request.query}
                  </Text>
                ),
                type: NotificationType.ERROR,
              }),
            )
          })

        setRequest(request)
      } else {
        dispatch(actions.query.stopRunning())
      }
    }
  }, [quest, dispatch, running])

  useEffect(() => {
    const editor = editorRef?.current

    if (running.value && editor) {
      savePreferences(editor)
    }
  }, [running, savePreferences])

  return (
    <Content>
      <Editor
        defaultLanguage="sql"
        onMount={handleEditorDidMount}
        options={{
          fontSize: 14,
          fontFamily: theme.fontMonospace,
          renderLineHighlight: "none",
        }}
      />
      <Loader show={!!request} />
    </Content>
  )
}

export default MonacoEditor
