import { format, FormatOptions } from "sql-formatter"

export const formatSql = (statement: string, options?: FormatOptions) => {
  return format(statement, {
    language: "postgresql",
    ...options,
  })
}
