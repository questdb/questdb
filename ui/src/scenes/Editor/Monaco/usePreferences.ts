/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import { useContext } from "react"
import { StoreKey } from "utils/localStorage/types"
import { LocalStorageContext } from "providers/LocalStorageProvider"
import { editor } from "monaco-editor"

type IStandaloneCodeEditor = editor.IStandaloneCodeEditor

export const usePreferences = () => {
  const { queryText, editorCol, editorLine, updateSettings } = useContext(
    LocalStorageContext,
  )

  const loadPreferences = (editor: IStandaloneCodeEditor) => {
    if (queryText) {
      editor.setValue(queryText)
    }

    if (editorLine && editorCol) {
      editor.setPosition({ column: editorCol, lineNumber: editorLine })
    }
  }

  const savePreferences = (editor: IStandaloneCodeEditor) => {
    updateSettings(StoreKey.QUERY_TEXT, editor.getValue())

    if (editor.getPosition()) {
      const lineNumber = editor?.getPosition()?.lineNumber
      const columnNumber = editor?.getPosition()?.column
      if (lineNumber) {
        updateSettings(StoreKey.EDITOR_LINE, lineNumber)
      }
      if (columnNumber) {
        updateSettings(StoreKey.EDITOR_COL, columnNumber)
      }
    }
  }

  return {
    loadPreferences,
    savePreferences,
  }
}
