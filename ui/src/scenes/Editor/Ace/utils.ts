import * as ace from "ace-builds"

enum StoreKey {
  TEXT = "query.text",
  LINE = "editor.line",
  COL = "editor.col",
}

type Position = Readonly<{
  column: number
  row: number
}>

export type Request = Readonly<{
  query: string
  row: number
  column: number
}>

export const loadPreferences = (editor: ace.Ace.Editor) => {
  const query = localStorage.getItem(StoreKey.TEXT)

  if (query) {
    editor.setValue(query)
  }

  const row = parseInt(localStorage.getItem(StoreKey.LINE) || "0", 10)
  const col = parseInt(localStorage.getItem(StoreKey.COL) || "0", 10)

  if (row && col) {
    setTimeout(() => {
      editor.gotoLine(row, col, false)
    }, 10)
  }
}

export const savePreferences = (editor: ace.Ace.Editor) => {
  localStorage.setItem(StoreKey.TEXT, editor.getValue())
  localStorage.setItem(StoreKey.LINE, `${editor.getCursorPosition().row + 1}`)
  localStorage.setItem(StoreKey.COL, `${editor.getCursorPosition().column}`)
}

export const getQueryFromCursor = (
  editor: ace.Ace.Editor,
): Request | undefined => {
  const text = editor.getValue()
  const position = editor.getCursorPosition()
  let row = 0

  let column = 0

  const sqlTextStack = []
  let startRow = 0
  let startCol = 0
  let startPos = -1
  let sql = null
  let inQuote = false

  for (let i = 0; i < text.length; i++) {
    const char = text[i]

    switch (char) {
      case ";": {
        if (inQuote) {
          column++
          continue
        }

        if (
          row < position.row ||
          (row === position.row && column < position.column)
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
  editor: ace.Ace.Editor,
): Request | undefined => {
  const query = editor.getSelectedText()
  let n = query.length
  let column = query.charAt(n)

  while (n > 0 && (column === " " || column === "\n" || column === ";")) {
    n--
    column = query.charAt(n)
  }

  if (n > 0) {
    const { start } = editor.getSelectionRange()

    return {
      query: query.substr(0, n + 1),
      row: start.row,
      column: start.column,
    }
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
