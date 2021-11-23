import React, {
  createContext,
  MutableRefObject,
  PropsWithChildren,
  useContext,
  useRef,
} from "react"
import { editor } from "monaco-editor"
import { Monaco } from "@monaco-editor/react"
import {
  insertTextAtCursor,
  appendQuery,
} from "../../scenes/Editor/Monaco/utils"

type IStandaloneCodeEditor = editor.IStandaloneCodeEditor

type ContextProps = {
  editorRef: MutableRefObject<IStandaloneCodeEditor | null> | null
  monacoRef: MutableRefObject<Monaco | null> | null
  insertTextAtCursor: (text: string) => void
  getValue: () => void
  appendQuery: (query: string) => void
}

const defaultValues = {
  editorRef: null,
  monacoRef: null,
  insertTextAtCursor: (text: string) => undefined,
  getValue: () => undefined,
  appendQuery: (query: string) => undefined,
}

const EditorContext = createContext<ContextProps>(defaultValues)

export const EditorProvider = ({ children }: PropsWithChildren<{}>) => {
  const editorRef = useRef<IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<Monaco | null>(null)

  /*
    To avoid re-rendering components that subscribe to this context
    we don't set value via a useState hook
   */
  const getValue = () => {
    return editorRef.current?.getValue()
  }

  return (
    <EditorContext.Provider
      value={{
        editorRef,
        monacoRef,
        getValue,
        insertTextAtCursor: (text) => {
          if (editorRef?.current) {
            insertTextAtCursor(editorRef.current, text)
          }
        },
        appendQuery: (text) => {
          if (editorRef?.current) {
            appendQuery(editorRef.current, text)
          }
        },
      }}
    >
      {children}
    </EditorContext.Provider>
  )
}

export const useEditor = () => {
  return useContext(EditorContext)
}
