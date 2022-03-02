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
import { editor, IPosition, IRange } from "monaco-editor"
import { Monaco } from "@monaco-editor/react"

type IStandaloneCodeEditor = editor.IStandaloneCodeEditor

export const QuestDBLanguageName: string = "questdb-sql"

export type Request = Readonly<{
  query: string
  row: number
  column: number
}>

export const getSelectedText = (
  editor: IStandaloneCodeEditor,
): string | undefined => {
  const model = editor.getModel()
  const selection = editor.getSelection()
  return model && selection ? model.getValueInRange(selection) : undefined
}

export const getQueryFromCursor = (
  editor: IStandaloneCodeEditor,
): Request | undefined => {
  const text = editor.getValue()
  const position = editor.getPosition()

  let row = 0

  let column = 0

  const sqlTextStack = []
  let startRow = 0
  let startCol = 0
  let startPos = -1
  let sql = null
  let inQuote = false

  if (!position) return

  for (let i = 0; i < text.length; i++) {
    const char = text[i]

    switch (char) {
      case ";": {
        if (inQuote) {
          column++
          continue
        }

        if (
          row < position.lineNumber - 1 ||
          (row === position.lineNumber - 1 && column < position.column)
        ) {
          sqlTextStack.push({
            row: startRow,
            col: startCol,
            position: startPos,
            limit: i,
          })
          startRow = row
          startCol = column
          startPos = i + 1
          column++
        } else {
          sql = text.substring(startPos === -1 ? 0 : startPos, i)
        }
        break
      }

      case " ": {
        // ignore leading space
        if (startPos === i) {
          startRow = row
          startCol = column
          startPos = i + 1
        }

        column++
        break
      }

      case "\n": {
        row++
        column = 0

        if (startPos === i) {
          startRow = row
          startCol = column
          startPos = i + 1
          column++
        }
        break
      }

      case "'": {
        inQuote = !inQuote
        column++
        break
      }

      default: {
        column++
        break
      }
    }

    if (sql) {
      break
    }
  }

  if (!sql) {
    sql = startPos === -1 ? text : text.substring(startPos)
  }

  if (sql.length === 0) {
    const prev = sqlTextStack.pop()

    if (prev) {
      return {
        column: prev.col,
        query: text.substring(prev.position, prev.limit),
        row: prev.row,
      }
    }

    return
  }

  return {
    column: startCol,
    query: sql,
    row: startRow,
  }
}

export const getQueryFromSelection = (
  editor: IStandaloneCodeEditor,
): Request | undefined => {
  const selection = editor.getSelection()
  const selectedText = getSelectedText(editor)
  if (selection && selectedText) {
    let n = selectedText.length
    let column = selectedText.charAt(n)

    while (n > 0 && (column === " " || column === "\n" || column === ";")) {
      n--
      column = selectedText.charAt(n)
    }

    if (n > 0) {
      return {
        query: selectedText.substr(0, n + 1),
        row: selection.startLineNumber,
        column: selection.startColumn,
      }
    }
  }
}

export const getQueryRequestFromEditor = (
  editor: IStandaloneCodeEditor,
): Request | undefined => {
  const selectedText = getSelectedText(editor)
  if (selectedText) {
    return getQueryFromSelection(editor)
  }
  return getQueryFromCursor(editor)
}

export const getQueryRequestFromLastExecutedQuery = (
  query: string,
): Request | undefined => {
  return {
    query,
    row: 0,
    column: 0,
  }
}

export const getErrorRange = (
  editor: IStandaloneCodeEditor,
  request: Request,
  errorPosition: number,
): IRange | null => {
  const position = toTextPosition(request, errorPosition)
  const model = editor.getModel()
  if (model) {
    const selection = editor.getSelection()
    const selectedText = getSelectedText(editor)
    let wordAtPosition
    if (selection && selectedText) {
      wordAtPosition = model.getWordAtPosition({
        column: selection.startColumn,
        lineNumber: selection.startLineNumber,
      })
    } else {
      wordAtPosition = model.getWordAtPosition(position)
    }
    if (wordAtPosition) {
      return {
        startColumn: wordAtPosition.startColumn,
        endColumn: wordAtPosition.endColumn,
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
      }
    }
  }
  return null
}

export const insertTextAtCursor = (
  editor: IStandaloneCodeEditor,
  text: string,
) => {
  editor.trigger("keyboard", "type", { text })
  editor.focus()
}

const insertText = ({
  editor,
  lineNumber,
  column,
  text,
}: {
  editor: IStandaloneCodeEditor
  lineNumber: number
  column: number
  text: string
}) => {
  editor.executeEdits("", [
    {
      range: {
        startLineNumber: lineNumber,
        startColumn: column,
        endLineNumber: lineNumber,
        endColumn: column,
      },
      text,
    },
  ])
}

/** `getTextFixes` is used to create correct prefix and suffix for the text that is inserted in the editor.
 * When inserting text, we want it to be neatly aligned with surrounding empty lines.
 * For example, appending text at the last line should add one new line, whereas in other cases it should add two.
 * This function defines these rules.
 */
const getTextFixes = ({
  model,
  position,
}: {
  model: ReturnType<typeof editor.getModel>
  position: IPosition
}) => {
  const isLastLine = position.lineNumber === model?.getLineCount()
  const lineAtCursor = model?.getLineContent(position.lineNumber)

  const char = "\n"

  type Rule = {
    when: () => boolean
    then: () => { prefix: number; suffix: number }
  }

  const rules: Rule[] = [
    // editor is empty
    {
      when: () => model?.getValue() === "",
      then: () => ({ prefix: 0, suffix: 1 }),
    },

    // cursor in middle of the code, but the current line is empty
    {
      when: () => !isLastLine && lineAtCursor === "",
      then: () => ({ prefix: 0, suffix: 2 }),
    },

    // cursor is in middle of the code, and current line is not empty
    {
      when: () => !isLastLine && lineAtCursor !== "",
      then: () => ({ prefix: 1, suffix: 2 }),
    },

    // cursor is at the last line, which is empty
    {
      when: () => isLastLine && lineAtCursor === "",
      then: () => ({ prefix: 1, suffix: 1 }),
    },

    // cursor is at the last line, which is not empty
    {
      when: () => isLastLine && lineAtCursor !== "",
      then: () => ({ prefix: 2, suffix: 1 }),
    },
  ]

  const defaultThen = {
    then: () => ({ prefix: 0, suffix: 0 }),
  }

  const { prefix, suffix } = (
    rules.find(({ when }) => when()) ?? defaultThen
  ).then()

  return { prefix: char.repeat(prefix), suffix: char.repeat(suffix) }
}

export const appendQuery = (editor: IStandaloneCodeEditor, query: string) => {
  const model = editor.getModel()
  if (model) {
    const position = editor.getPosition()

    if (position) {
      const isLineEmpty = model.getLineContent(position.lineNumber) === ""
      const newLines = query.split("\n")

      const positionInsert = {
        lineStart: position.lineNumber + 1,
        lineEnd: position.lineNumber + newLines.length,
        columnStart: 0,
        columnEnd: newLines.sort((a, b) => b.length - a.length)[0].length + 1,
      }

      const positionSelect = {
        lineStart: positionInsert.lineStart + (isLineEmpty ? 0 : 1),
        lineEnd: positionInsert.lineEnd + (isLineEmpty ? 0 : 1),
        columnStart: 0,
        columnEnd: positionInsert.columnEnd,
      }

      const { prefix, suffix } = getTextFixes({ model, position })

      insertText({
        editor,
        lineNumber: positionInsert.lineStart,
        column: positionInsert.columnStart,
        text: `${prefix}${query}${suffix}`,
      })

      editor.setSelection({
        startColumn: positionSelect.columnStart,
        endColumn: positionSelect.columnEnd,
        startLineNumber: positionSelect.lineStart,
        endLineNumber: positionSelect.lineEnd,
      })
    }
    editor.focus()
  }
}

export const clearModelMarkers = (
  monaco: Monaco,
  editor: IStandaloneCodeEditor,
) => {
  const model = editor.getModel()

  if (model) {
    monaco.editor.setModelMarkers(model, QuestDBLanguageName, [])
  }
}

export const setErrorMarker = (
  monaco: Monaco,
  editor: IStandaloneCodeEditor,
  errorRange: IRange,
  message: string,
) => {
  const model = editor.getModel()

  if (model) {
    monaco.editor.setModelMarkers(model, QuestDBLanguageName, [
      {
        message,
        severity: monaco.MarkerSeverity.Error,
        startLineNumber: errorRange.startLineNumber,
        endLineNumber: errorRange.endLineNumber,
        startColumn: errorRange.startColumn,
        endColumn: errorRange.endColumn,
      },
    ])
  }
}

export const toTextPosition = (
  request: Request,
  position: number,
): IPosition => {
  const end = Math.min(position, request.query.length)
  let row = 0
  let column = 0

  for (let i = 0; i < end; i++) {
    if (request.query.charAt(i) === "\n") {
      row++
      column = 0
    } else {
      column++
    }
  }

  return {
    lineNumber: row + 1 + request.row,
    column: (row === 0 ? column + request.column : column) + 1,
  }
}
