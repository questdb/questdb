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
import { useDispatch, useSelector } from "react-redux"
import { actions, selectors } from "../../../store"

type IStandaloneCodeEditor = editor.IStandaloneCodeEditor

const MonacoEditor = () => {
  const { editorRef } = useEditor()
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
    }
    monaco.editor.defineTheme("dracula", dracula)
    monaco.editor.setTheme("dracula")
  }

  useEffect(() => {
    if (editorRef?.current) {
      if (running.value) {
        const request = running.isRefresh
          ? getQueryRequestFromLastExecutedQuery(lastExecutedQuery)
          : getQueryRequestFromEditor(editorRef.current)
        console.log(request)
      } else if (request) {
        quest.abort()
        dispatch(actions.query.stopRunning())
        setRequest(undefined)
      }
    }
  }, [request, quest, dispatch, running])

  useEffect(() => {
    const editor = editorRef?.current

    if (running.value && editor) {
      savePreferences(editor)
    }
  }, [running, savePreferences])

  return (
    <Editor
      defaultLanguage="sql"
      onMount={handleEditorDidMount}
      options={{
        fontSize: 14,
        fontFamily: theme.fontMonospace,
        renderLineHighlight: "none",
      }}
    />
  )
}

export default MonacoEditor
