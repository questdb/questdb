import {
  ConfigurationShape,
  ConsoleAction,
  ConsoleAT,
  ConsoleStateShape,
} from "types"

export const initialState: ConsoleStateShape = {}

export const defaultConfiguration: ConfigurationShape = {
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
        configuration: {
          ...defaultConfiguration,
          ...action.payload,
        },
      }
    }

    default:
      return state
  }
}

export default _console
