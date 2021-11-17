import React, { useRef } from "react"
import Editor, { Monaco } from "@monaco-editor/react"
import dracula from "./dracula"
import { editor } from "monaco-editor"
import { theme } from "../../../theme"

type IStandaloneCodeEditor = editor.IStandaloneCodeEditor

const MonacoEditor = () => {
  const editorRef = useRef<IStandaloneCodeEditor | null>(null)

  const handleEditorDidMount = (
    editor: IStandaloneCodeEditor,
    monaco: Monaco,
  ) => {
    editorRef.current = editor
    monaco.editor.defineTheme("dracula", dracula)
    monaco.editor.setTheme("dracula")
  }

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
