type ColumnDefinition = Readonly<{ name: string; type: string }>

type Value = string | number | boolean
type RawData = Record<string, Value>

export enum Type {
  DDL = "ddl",
  DQL = "dql",
  ERROR = "error",
}

type HostConfig = Readonly<{
  host: string
  port: number
}>

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
  query: string
  timings: Timings
}

type RawDdlResult = {
  ddl: "OK"
}

type RawErrorResult = {
  error: string
  position: number
  query: string
}

type DdlResult = {
  query: string
  type: Type.DDL
}

type RawResult = RawDqlResult | RawDdlResult

export type ErrorResult = RawErrorResult & {
  type: Type.ERROR
}

export type Result<T extends Record<string, any>> =
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
  tableName: string
}

export type Column = {
  columnName: string
  columnType: string
}

export type Options = {
  limit?: string
}

const hostConfig: HostConfig = {
  host: "http://localhost",
  port: 9000,
}

export class Client {
  private _config: HostConfig
  private _controllers: AbortController[] = []

  constructor(config?: string | Partial<HostConfig>) {
    if (!config) {
      this._config = hostConfig
    } else if (typeof config === "string") {
      this._config = {
        ...hostConfig,
        host: config,
      }
    } else if (typeof config === "object") {
      this._config = {
        ...hostConfig,
        ...config,
      }
    } else {
      this._config = hostConfig
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

  async query<T>(query: string, options?: Options): Promise<Result<T>> {
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

  async queryRaw(
    query: string,
    options?: Options,
  ): Promise<
    (Omit<RawDqlResult, "ddl"> & { type: Type.DQL }) | DdlResult | ErrorResult
  > {
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
        `${this._config.host}:${this._config.port}/exec?${Client.encodeParams(
          payload,
        )}`,
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

      return {
        ...data,
        timings: {
          ...data.timings,
          fetch: fetchTime,
        },
        type: Type.DQL,
      }
    }

    if (/quest/.test(response.headers.get("server") || "")) {
      const data = (await response.json()) as RawErrorResult

      // eslint-disable-next-line prefer-promise-reject-errors
      return Promise.reject({
        ...data,
        type: Type.ERROR,
      })
    }

    // eslint-disable-next-line prefer-promise-reject-errors
    return Promise.reject({
      error: "QuestDB is not reachable",
      position: -1,
      query,
      type: Type.ERROR,
    })
  }

  async showTables(): Promise<Result<Table>> {
    const response = await this.query<Table>("SHOW TABLES;")

    if (response.type === Type.DQL) {
      return {
        ...response,
        data: response.data.slice().sort((a, b) => {
          if (a.tableName > b.tableName) {
            return 1
          }

          if (a.tableName < b.tableName) {
            return -1
          }

          return 0
        }),
      }
    }

    return response
  }

  async showColumns(table: string): Promise<Result<Column>> {
    return await this.query<Column>(`SHOW COLUMNS FROM '${table}';`)
  }
}
