import { ColumnDefinition, DatasetType } from "utils"

const minColumnWidth = 60
const maxRowsToAnalyze = 100

export type ColumnTextAlignment = "left" | "right"

export type ColumnWidths = number[] | string[]

export type ComputedGridWidths = {
  columnWidths: ColumnWidths
  totalWidth: number | string
}

type QueryLimit = {
  from: number
  to: number
}

export const getQueryLimit = (
  startIndex: number,
  endIndex: number,
  perPage: number,
): QueryLimit => {
  return {
    from: Math.floor(startIndex / perPage) * perPage,
    to: Math.ceil(endIndex / perPage) * perPage,
  }
}

export const getColumnTextAlignment = (
  columnType: string,
): ColumnTextAlignment => {
  if (columnType === "STRING" || columnType === "SYMBOL") {
    return "left"
  }
  return "right"
}

export const computeColumnWidths = (
  columns: ColumnDefinition[],
  dataset: DatasetType[],
  wrapperWidth: number,
): ComputedGridWidths => {
  const columnWidths: number[] = []
  const columnWidthsPercentage: string[] = []
  let totalWidth = 0
  let maxWidthPercentage = 100

  columns.forEach((column) => {
    const width = Math.max(
      minColumnWidth,
      Math.ceil(column.name.length * 8 * 1.2 + 8),
    )
    columnWidths.push(width)
    totalWidth += width
  })

  const max =
    dataset.length > maxRowsToAnalyze ? maxRowsToAnalyze : dataset.length

  for (let i = 0; i < max; i++) {
    const row = dataset[i]
    let sum = 0

    for (let k = 0; k < row.length; k++) {
      const cell = row[k]
      const str = cell !== null ? cell.toString() : "null"
      const width = Math.max(minColumnWidth, str.length * 8 + 8)
      columnWidths[k] = Math.max(width, columnWidths[k])
      sum += columnWidths[k]
    }

    totalWidth = Math.max(totalWidth, sum)
  }

  if (totalWidth < wrapperWidth) {
    if (totalWidth * 2 < wrapperWidth) {
      maxWidthPercentage = 50
    }

    for (let i = 0; i < columnWidths.length; i++) {
      columnWidthsPercentage.push(
        `${Math.min(
          (columnWidths[i] * 100) / totalWidth,
          maxWidthPercentage,
        )}%`,
      )
    }
  }

  if (totalWidth > wrapperWidth) {
    return {
      columnWidths,
      totalWidth,
    }
  } else {
    return {
      columnWidths: columnWidthsPercentage,
      totalWidth: `100%`,
    }
  }
}
