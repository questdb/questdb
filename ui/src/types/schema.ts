export type ColumnShape = Readonly<{
  description: string
  name: string
  type: string
}>

export type TableShape = Readonly<{
  description: string
  name: string
  columns: ColumnShape[]
}>
