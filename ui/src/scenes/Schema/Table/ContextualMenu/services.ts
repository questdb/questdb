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
