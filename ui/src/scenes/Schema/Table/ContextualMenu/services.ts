import * as QuestDB from "utils/questdb"
import { trim } from "ramda"

export const formatTableSchemaQueryResult = (
  name: string,
  result: QuestDB.QueryRawResult,
): string => {
  if (result.type === QuestDB.Type.DQL) {
    let query = `CREATE TABLE '${name}' (`

    for (let i = 0; i < result.count; i++) {
      const column = result.dataset[i]
      const name = column[0]
      const typeDef = column[1]
      const indexed = column[2]
      const indexBlockCapacity = column[3]
      const symbolCached = column[4]
      const symbolCapacity = column[5]

      query += `${name} ${typeDef} `
      if (typeDef === "SYMBOL") {
        query += symbolCapacity ? `capacity ${symbolCapacity} ` : ""
        query += symbolCached ? "cache " : "nocache "
      }
      query += indexed ? "index " : ""
      query +=
        indexed && indexBlockCapacity ? `capacity ${indexBlockCapacity} ` : ""

      query = trim(query)

      if (i !== result.count - 1) {
        query += ", "
      }
    }

    return `${trim(query)})`
  } else {
    throw new Error("Could not format table schema")
  }
}
