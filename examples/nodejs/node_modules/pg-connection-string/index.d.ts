export function parse(connectionString: string): ConnectionOptions

export interface ConnectionOptions {
  host: string | null
  password?: string
  user?: string
  port?: string | null
  database: string | null | undefined
  client_encoding?: string
  ssl?: boolean | string

  application_name?: string
  fallback_application_name?: string
  options?: string
}
