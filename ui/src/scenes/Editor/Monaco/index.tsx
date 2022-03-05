import React, { useContext, useEffect, useState } from "react"
import Editor, { Monaco, loader } from "@monaco-editor/react"
import dracula from "./dracula"
import { editor, IDisposable } from "monaco-editor"
import { theme } from "../../../theme"
import { QuestContext, useEditor } from "../../../providers"
import { usePreferences } from "./usePreferences"
import {
  appendQuery,
  getErrorRange,
  getQueryRequestFromEditor,
  getQueryRequestFromLastExecutedQuery,
  QuestDBLanguageName,
  Request,
  setErrorMarker,
  clearModelMarkers,
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
import {
  conf as QuestDBLanguageConf,
  language as QuestDBLanguage,
  createQuestDBCompletionProvider,
  createSchemaCompletionProvider,
} from "./questdb-sql"
import { color } from "../../../utils"

loader.config({
  paths: {
    vs: "assets/vs",
  },
})

type IStandaloneCodeEditor = editor.IStandaloneCodeEditor

const Content = styled(PaneContent)`
  position: relative;
  overflow: hidden;

  .monaco-scrollable-element > .scrollbar > .slider {
    background: ${color("draculaSelection")};
  }
`

enum Command {
  EXECUTE = "execute",
  FOCUS_GRID = "focus_grid",
  CLEANUP_NOTIFICATIONS = "clean_notifications",
}

const MonacoEditor = () => {
  const { editorRef, monacoRef, insertTextAtCursor } = useEditor()
  const [editorReady, setEditorReady] = useState(false)
  const { loadPreferences, savePreferences } = usePreferences()
  const { quest } = useContext(QuestContext)
  const [request, setRequest] = useState<Request | undefined>()
  const [lastExecutedQuery, setLastExecutedQuery] = useState("")
  const dispatch = useDispatch()
  const running = useSelector(selectors.query.getRunning)
  const tables = useSelector(selectors.query.getTables)
  const [
    schemaCompletionHandle,
    setSchemaCompletionHandle,
  ] = useState<IDisposable>()

  const toggleRunning = (isRefresh: boolean = false) => {
    dispatch(actions.query.toggleRunning(isRefresh))
  }

  const handleEditorBeforeMount = (monaco: Monaco) => {
    monaco.languages.register({ id: QuestDBLanguageName })

    monaco.languages.setMonarchTokensProvider(
      QuestDBLanguageName,
      QuestDBLanguage,
    )

    monaco.languages.setLanguageConfiguration(
      QuestDBLanguageName,
      QuestDBLanguageConf,
    )

    monaco.languages.registerCompletionItemProvider(
      QuestDBLanguageName,
      createQuestDBCompletionProvider(),
    )

    setSchemaCompletionHandle(
      monaco.languages.registerCompletionItemProvider(
        QuestDBLanguageName,
        createSchemaCompletionProvider(tables),
      ),
    )

    monaco.editor.defineTheme("dracula", dracula)
  }

  const handleEditorDidMount = (
    editor: IStandaloneCodeEditor,
    monaco: Monaco,
  ) => {
    monaco.editor.setTheme("dracula")

    if (monacoRef) {
      monacoRef.current = monaco
    }

    if (editorRef) {
      editorRef.current = editor

      // Support legacy bus events for non-react codebase
      window.bus.on(BusEvent.MSG_EDITOR_INSERT_COLUMN, (_event, column) => {
        insertTextAtCursor(column)
      })

      window.bus.on(BusEvent.MSG_QUERY_FIND_N_EXEC, (_event, query: string) => {
        const text = `${query};`
        appendQuery(editor, text)
        toggleRunning()
      })

      window.bus.on(BusEvent.MSG_QUERY_EXEC, (_event, query: { q: string }) => {
        const matches = editor
          .getModel()
          ?.findMatches(query.q, true, false, true, null, true)
        if (matches) {
          // TODO: Display a query marker on correct line
        }
        toggleRunning(true)
      })

      window.bus.on(
        BusEvent.MSG_QUERY_EXPORT,
        (_event, request?: { q: string }) => {
          if (request) {
            window.location.href = `/exp?query=${encodeURIComponent(request.q)}`
          }
        },
      )

      window.bus.on(BusEvent.MSG_EDITOR_FOCUS, () => {
        const position = editor.getPosition()
        if (position) {
          editor.setPosition({
            lineNumber: position.lineNumber + 1,
            column: position?.column,
          })
        }
        editor.focus()
      })

      editor.addAction({
        id: Command.FOCUS_GRID,
        label: "Focus Grid",
        keybindings: [monaco.KeyCode.F2],
        run: () => {
          window.bus.trigger(BusEvent.GRID_FOCUS)
        },
      })

      editor.addAction({
        id: Command.EXECUTE,
        label: "Execute command",
        keybindings: [
          monaco.KeyCode.F9,
          monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
        ],
        run: () => {
          toggleRunning()
        },
      })

      editor.addAction({
        id: Command.CLEANUP_NOTIFICATIONS,
        label: "Clear all notifications",
        keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyK],
        run: () => {
          dispatch(actions.query.cleanupNotifications())
        },
      })
    }

    loadPreferences(editor)

    // Insert query, if one is found in the URL
    const params = new URLSearchParams(window.location.search)
    const query = params.get("query")
    if (query) {
      appendQuery(editor, query)
    }

    const executeQuery = params.get("executeQuery")
    if (executeQuery) {
      toggleRunning()
    }
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
      if (monacoRef?.current) {
        clearModelMarkers(monacoRef.current, editorRef.current)
      }

      const request = running.isRefresh
        ? getQueryRequestFromLastExecutedQuery(lastExecutedQuery)
        : getQueryRequestFromEditor(editorRef.current)

      if (request?.query) {
        void quest
          .queryRaw(request.query, { limit: "0,1000", explain: true })
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
                  jitCompiled: result.explain?.jitCompiled ?? false,
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

            if (editorRef?.current && monacoRef?.current) {
              const errorRange = getErrorRange(
                editorRef.current,
                request,
                error.position,
              )
              if (errorRange) {
                setErrorMarker(
                  monacoRef?.current,
                  editorRef.current,
                  errorRange,
                  error.error,
                )
              }
            }
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

  useEffect(() => {
    if (tables.length > 0) {
      setEditorReady(true)

      if (monacoRef?.current) {
        schemaCompletionHandle?.dispose()
        setSchemaCompletionHandle(
          monacoRef.current.languages.registerCompletionItemProvider(
            QuestDBLanguageName,
            createSchemaCompletionProvider(tables),
          ),
        )
      }
    }
  }, [tables, monacoRef])

  return (
    <Content>
      {editorReady && (
        <Editor
          beforeMount={handleEditorBeforeMount}
          defaultLanguage={QuestDBLanguageName}
          onMount={handleEditorDidMount}
          options={{
            fixedOverflowWidgets: true,
            fontSize: 14,
            fontFamily: theme.fontMonospace,
            renderLineHighlight: "gutter",
            minimap: {
              enabled: false,
            },
            scrollBeyondLastLine: false,
          }}
          theme="vs-dark"
        />
      )}
      <Loader show={!!request || !tables} />
    </Content>
  )
}

export default MonacoEditor
