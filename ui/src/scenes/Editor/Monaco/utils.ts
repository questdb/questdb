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
import { editor } from "monaco-editor"

type IStandaloneCodeEditor = editor.IStandaloneCodeEditor

type Position = Readonly<{
  column: number
  row: number
}>

export type Request = Readonly<{
  query: string
  row: number
  column: number
}>

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
          row < position.lineNumber ||
          (row === position.lineNumber && column < position.column)
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
  if (selection && selection.toString().length === 0) {
    const query = selection.toString()
    let n = query.length
    let column = query.charAt(n)

    while (n > 0 && (column === " " || column === "\n" || column === ";")) {
      n--
      column = query.charAt(n)
    }

    if (n > 0) {
      return {
        query: query.substr(0, n + 1),
        row: selection.startLineNumber,
        column: selection.startColumn,
      }
    }
  }
}

export const getQueryRequestFromEditor = (
  editor: IStandaloneCodeEditor,
): Request | undefined => {
  const model = editor.getModel()
  const selection = editor.getSelection()
  if (model && selection && model.getValueInRange(selection).length === 0) {
    return getQueryFromCursor(editor)
  }
  return getQueryFromSelection(editor)
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

export const toTextPosition = (
  request: Request,
  position: number,
): Position => {
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
    row: row + 1 + request.row,
    column: (row === 0 ? column + request.column : column) + 1,
  }
}
