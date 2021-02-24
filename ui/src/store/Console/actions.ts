import { ConfigurationShape, ConsoleAction, ConsoleAT } from "types"

const bootstrap = (): ConsoleAction => ({
  type: ConsoleAT.BOOTSTRAP,
})

const refreshAuthToken = (init: boolean): ConsoleAction => ({
  payload: init,
  type: ConsoleAT.REFRESH_AUTH_TOKEN,
})

const setConfiguration = (payload: ConfigurationShape): ConsoleAction => ({
  payload,
  type: ConsoleAT.SET_CONFIGURATION,
})

const toggleSideMenu = (): ConsoleAction => ({
  type: ConsoleAT.TOGGLE_SIDE_MENU,
})

export default { bootstrap, refreshAuthToken, setConfiguration, toggleSideMenu }
