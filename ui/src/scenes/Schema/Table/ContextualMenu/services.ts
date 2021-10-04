import * as QuestDB from "utils/questdb"
import { trim } from "ramda"

export const formatTableSchemaQueryResult = (
  name: string,
  result: QuestDB.QueryRawResult,
): string => {
  if (result.type === QuestDB.Type.DQL) {
    let designatedName = null
    let query = `CREATE TABLE '${name}' (`

    for (let i = 0; i < result.count; i++) {
      const [
        name,
        typeDef,
        indexed,
        indexBlockCapacity,
        symbolCached,
        symbolCapacity,
        designated,
      ] = result.dataset[i]

      query += `${name} ${typeDef} `

      if (typeDef === "SYMBOL") {
        query += symbolCapacity ? `capacity ${symbolCapacity} ` : ""
        if (symbolCached) {
          query += "CACHE "
        }
      }

      if (indexed) {
        query += "index "
        if (indexBlockCapacity) {
          query += `capacity ${indexBlockCapacity} `
        }
      }

      if (designated) {
        designatedName = name
      }

      query = trim(query)

      if (i !== result.count - 1) {
        query += ", "
      }
    }

    query += ")"

    if (designatedName) {
      query += ` timestamp (${designatedName})`
    }

    return `${query};`
  } else {
    throw new Error("Could not format table schema")
  }
}
