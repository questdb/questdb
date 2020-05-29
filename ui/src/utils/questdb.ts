type ColumnDefinition = Readonly<{ name: string; type: string }>

type Value = string | number | boolean
type RawData = Record<string, Value>

export const encodeParams = (
  params: Record<string, string | number | boolean>,
) =>
  Object.keys(params)
    .map((k) => `${encodeURIComponent(k)}=${encodeURIComponent(params[k])}`)
    .join("&")

type HostConfig = Readonly<{
  host: string
  port: number
}>

export type Result<T extends Record<string, any>> = {
  columns: ColumnDefinition[]
  count: number
  data: T[]
}

type ExecResult = {
  columns: ColumnDefinition[]
  count: number
  dataset: any[][]
  query: string
}

export type QuestDBTable = {
  tableName: string
}

export type QuestDBColumn = {
  columnName: string
  columnType: string
}

const hostConfig: HostConfig = {
  host: "http://localhost",
  port: 9000,
}

export class QuestDB {
  private _config: HostConfig

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

  async query<T>(query: string): Promise<Result<T>> {
    const payload = {
      query,
    }
    const response = await fetch(
      `${this._config.host}:${this._config.port}/exec?${encodeParams(payload)}`,
    )

    const data = (await response.json()) as ExecResult

    const parsed = (data.dataset.map(
      (row) =>
        row.reduce(
          (acc: RawData, val: Value, idx) => ({
            ...acc,
            [data.columns[idx].name]: val,
          }),
          {},
        ) as RawData,
    ) as unknown) as T[]

    return {
      columns: data.columns,
      count: data.count,
      data: parsed,
    }
  }

  async queryRaw(query: string): Promise<ExecResult> {
    const payload = {
      query,
    }
    const response = await fetch(
      `${this._config.host}:${this._config.port}/exec?${encodeParams(payload)}`,
    )

    const data = (await response.json()) as ExecResult

    return data
  }

  async showTables(): Promise<Result<QuestDBTable>> {
    const response = await this.query<QuestDBTable>("SHOW TABLES;")

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

  async showColumns(table: string): Promise<Result<QuestDBColumn>> {
    return await this.query<QuestDBColumn>(`SHOW COLUMNS FROM '${table}';`)
  }
}
