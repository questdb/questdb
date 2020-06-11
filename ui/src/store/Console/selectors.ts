import { ConfigurationShape, StoreShape } from "types"

import { defaultConfiguration } from "./reducers"

const getConfiguration: (store: StoreShape) => ConfigurationShape = (store) =>
  store.console.configuration || defaultConfiguration

const getSideMenuOpened: (store: StoreShape) => boolean = (store) =>
  store.console.sideMenuOpened

export default {
  getConfiguration,
  getSideMenuOpened,
}
