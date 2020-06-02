import { ConfigurationShape, StoreShape } from "types"

import { defaultConfiguration } from "./reducers"

const getConfiguration: (store: StoreShape) => ConfigurationShape = (store) =>
  store.console.configuration || defaultConfiguration

export default {
  getConfiguration,
}
