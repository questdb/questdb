/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

type ColumnDefinition = Readonly<{ name: string; type: string }>

type Value = string | number | boolean
type RawData = Record<string, Value>

export enum Type {
  DDL = "ddl",
  DQL = "dql",
  ERROR = "error",
}

export type Timings = {
  compiler: number
  count: number
  execute: number
  fetch: number
}

type RawDqlResult = {
  columns: ColumnDefinition[]
  count: number
  dataset: any[][]
  ddl: undefined
  error: undefined
  query: string
  timings: Timings
}

type RawDdlResult = {
  ddl: "OK"
}

type RawErrorResult = {
  ddl: undefined
  error: "<error message>"
  position: number
  query: string
}

type DdlResult = {
  query: string
  type: Type.DDL
}

type RawResult = RawDqlResult | RawDdlResult | RawErrorResult

export type ErrorResult = RawErrorResult & {
  type: Type.ERROR
}

export type QueryRawResult =
  | (Omit<RawDqlResult, "ddl"> & { type: Type.DQL })
  | DdlResult
  | ErrorResult

export type QueryResult<T extends Record<string, any>> =
  | {
      columns: ColumnDefinition[]
      count: number
      data: T[]
      timings: Timings
      type: Type.DQL
    }
  | ErrorResult
  | DdlResult

export type Table = {
  table: string
}

export type Column = {
  column: string
  indexed: boolean
  type: string
}

export type Options = {
  limit?: string
}

export class Client {
  private _host: string
  private _controllers: AbortController[] = []

  constructor(host?: string) {
    if (!host) {
      this._host = window.location.origin
    } else {
      this._host = host
    }
  }

  static encodeParams = (
    params: Record<string, string | number | boolean | undefined>,
  ) =>
    Object.keys(params)
      .filter((k) => typeof params[k] !== "undefined")
      .map(
        (k) =>
          `${encodeURIComponent(k)}=${encodeURIComponent(
            params[k] as string | number | boolean,
          )}`,
      )
      .join("&")

  abort = () => {
    this._controllers.forEach((controller) => {
      controller.abort()
    })
    this._controllers = []
  }

  async query<T>(query: string, options?: Options): Promise<QueryResult<T>> {
    const result = await this.queryRaw(query, options)

    if (result.type === Type.DQL) {
      const { columns, count, dataset, timings } = result

      const parsed = (dataset.map(
        (row) =>
          row.reduce(
            (acc: RawData, val: Value, idx) => ({
              ...acc,
              [columns[idx].name]: val,
            }),
            {},
          ) as RawData,
      ) as unknown) as T[]

      return {
        columns,
        count,
        data: parsed,
        timings,
        type: Type.DQL,
      }
    }

    return result
  }

  async queryRaw(query: string, options?: Options): Promise<QueryRawResult> {
    const controller = new AbortController()
    const payload = {
      ...options,
      count: true,
      src: "con",
      query,
      timings: true,
    }

    this._controllers.push(controller)
    let response: Response
    const start = new Date()

    try {
      response = await fetch(
        `${this._host}/exec?${Client.encodeParams(payload)}`,
        { signal: controller.signal },
      )
    } catch (error) {
      const err = {
        position: -1,
        query,
        type: Type.ERROR,
      }

      if (error instanceof DOMException) {
        // eslint-disable-next-line prefer-promise-reject-errors
        return Promise.reject({
          ...err,
          error: error.code === 20 ? "Cancelled by user" : error.toString(),
        })
      }

      // eslint-disable-next-line prefer-promise-reject-errors
      return Promise.reject({
        ...err,
        error: "An error occured, please try again",
      })
    } finally {
      const index = this._controllers.indexOf(controller)

      if (index >= 0) {
        this._controllers.splice(index, 1)
      }
    }

    if (response.ok) {
      const fetchTime = (new Date().getTime() - start.getTime()) * 1e6
      const data = (await response.json()) as RawResult

      if (data.ddl) {
        return {
          query,
          type: Type.DDL,
        }
      }

      if (data.error) {
        // eslint-disable-next-line prefer-promise-reject-errors
        return Promise.reject({
          ...data,
          type: Type.ERROR,
        })
      }

      return {
        ...data,
        timings: {
          ...data.timings,
          fetch: fetchTime,
        },
        type: Type.DQL,
      }
    }

    // eslint-disable-next-line prefer-promise-reject-errors
    return Promise.reject({
      error: `QuestDB is not reachable [${response.status}]`,
      position: -1,
      query,
      type: Type.ERROR,
    })
  }

  async showTables(): Promise<QueryResult<Table>> {
    const response = await this.query<Table>("SHOW TABLES;")

    if (response.type === Type.DQL) {
      return {
        ...response,
        data: response.data.slice().sort((a, b) => {
          if (a.table > b.table) {
            return 1
          }

          if (a.table < b.table) {
            return -1
          }

          return 0
        }),
      }
    }

    return response
  }

  async showColumns(table: string): Promise<QueryResult<Column>> {
    return await this.query<Column>(`SHOW COLUMNS FROM '${table}';`)
  }
}
