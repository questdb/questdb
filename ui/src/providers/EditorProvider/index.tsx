import React, {
  createContext,
  MutableRefObject,
  PropsWithChildren,
  useContext,
  useRef,
} from "react"
import { editor } from "monaco-editor"

type IStandaloneCodeEditor = editor.IStandaloneCodeEditor

type ContextProps = {
  editorRef: MutableRefObject<IStandaloneCodeEditor | null> | null
  insertTextAtCursor: (text: string) => void
  getValue: () => void
}

const defaultValues = {
  editorRef: null,
  insertTextAtCursor: (text: string) => undefined,
  getValue: () => undefined,
}

const EditorContext = createContext<ContextProps>(defaultValues)

export const EditorProvider = ({ children }: PropsWithChildren<{}>) => {
  const editorRef = useRef<IStandaloneCodeEditor | null>(null)

  /*
    To avoid re-rendering components that subscribe to this context
    we don't set value via a useState hook
   */
  const getValue = () => {
    return editorRef.current?.getValue()
  }

  const insertTextAtCursor = (text: string) => {
    editorRef?.current?.trigger("keyboard", "type", { text })
  }

  return (
    <EditorContext.Provider value={{ editorRef, getValue, insertTextAtCursor }}>
      {children}
    </EditorContext.Provider>
  )
}

export const useEditor = () => {
  return useContext(EditorContext)
}
