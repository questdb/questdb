export type ConfigShape = Readonly<{
  enabled: string
  id: string
}>

export type WorkerPayloadShape = Readonly<{
  id: string
  host: string
  lastUpdated: string
}>
