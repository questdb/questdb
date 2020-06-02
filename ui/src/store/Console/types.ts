export type QueryShape = Readonly<{
  name: string
  value: string
}>

export type ConfigurationShape = Readonly<{
  readOnly: boolean
  savedQueries: QueryShape[]
}>

export type ConsoleStateShape = Readonly<{
  configuration?: ConfigurationShape
}>

export enum ConsoleAT {
  BOOTSTRAP = "CONSOLE/BOOTSTRAP",
  SET_CONFIGURATION = "CONSOLE/SET_CONFIGURATION",
}

export type BootstrapAction = Readonly<{
  type: ConsoleAT.BOOTSTRAP
}>

type SetConfigurationAction = Readonly<{
  payload: ConfigurationShape
  type: ConsoleAT.SET_CONFIGURATION
}>

export type ConsoleAction = BootstrapAction | SetConfigurationAction
