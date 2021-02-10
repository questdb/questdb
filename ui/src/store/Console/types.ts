export type QueryShape = Readonly<{
  name?: string
  value: string
}>

export type ConfigurationShape = Readonly<{
  githubBanner: boolean
  readOnly?: boolean
  savedQueries: QueryShape[]
}>

export type ConsoleStateShape = Readonly<{
  configuration?: ConfigurationShape
  sideMenuOpened: boolean
}>

export enum ConsoleAT {
  BOOTSTRAP = "CONSOLE/BOOTSTRAP",
  REFRESH_AUTH_TOKEN = "CONSOLE/REFRESH_AUTH_TOKEN",
  SET_CONFIGURATION = "CONSOLE/SET_CONFIGURATION",
  TOGGLE_SIDE_MENU = "CONSOLE/TOGGLE_SIDE_MENU",
}

export type BootstrapAction = Readonly<{
  type: ConsoleAT.BOOTSTRAP
}>

export type RefreshAuthTokenAction = Readonly<{
  type: ConsoleAT.REFRESH_AUTH_TOKEN
}>

type SetConfigurationAction = Readonly<{
  payload: ConfigurationShape
  type: ConsoleAT.SET_CONFIGURATION
}>

type ToggleSideMenuAction = Readonly<{
  type: ConsoleAT.TOGGLE_SIDE_MENU
}>

export type ConsoleAction =
  | BootstrapAction
  | RefreshAuthTokenAction
  | SetConfigurationAction
  | ToggleSideMenuAction
