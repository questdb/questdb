import { ConfigurationShape, ConsoleAction, ConsoleAT } from "types"

const bootstrap = (): ConsoleAction => ({
  type: ConsoleAT.BOOTSTRAP,
})

const setConfiguration = (payload: ConfigurationShape): ConsoleAction => ({
  payload,
  type: ConsoleAT.SET_CONFIGURATION,
})

export default { bootstrap, setConfiguration }
