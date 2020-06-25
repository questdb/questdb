import {
  ConfigurationShape,
  ConsoleAction,
  ConsoleAT,
  ConsoleStateShape,
} from "types"

export const initialState: ConsoleStateShape = {
  sideMenuOpened: false,
}

export const defaultConfiguration: ConfigurationShape = {
  githubBanner: false,
  readOnly: false,
  savedQueries: [],
}

const _console = (
  state = initialState,
  action: ConsoleAction,
): ConsoleStateShape => {
  switch (action.type) {
    case ConsoleAT.SET_CONFIGURATION: {
      return {
        ...state,
        configuration: {
          ...defaultConfiguration,
          ...action.payload,
        },
      }
    }

    case ConsoleAT.TOGGLE_SIDE_MENU: {
      return {
        ...state,
        sideMenuOpened: !state.sideMenuOpened,
      }
    }

    default:
      return state
  }
}

export default _console
