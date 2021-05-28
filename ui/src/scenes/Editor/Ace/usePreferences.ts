import { useContext } from "react"
import * as ace from "ace-builds"
import { StoreKey } from "utils/localStorage/types"
import { LocalStorageContext } from "providers/LocalStorageProvider"

export const usePreferences = () => {
  const { queryText, editorCol, editorLine, updateSettings } = useContext(
    LocalStorageContext,
  )

  const loadPreferences = (editor: ace.Ace.Editor) => {
    if (queryText) {
      editor.setValue(queryText)
    }

    if (editorLine && editorCol) {
      setTimeout(() => {
        editor.gotoLine(editorLine, editorCol, false)
      }, 1000)
    }
  }

  const savePreferences = (editor: ace.Ace.Editor) => {
    updateSettings(StoreKey.QUERY_TEXT, editor.getValue())
    updateSettings(StoreKey.EDITOR_COL, editor.getCursorPosition().column)
    updateSettings(StoreKey.EDITOR_LINE, editor.getCursorPosition().row + 1)
  }

  return {
    loadPreferences,
    savePreferences,
  }
}
