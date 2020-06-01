import { combineReducers } from "redux"

import _console, {
  initialState as consoleInitialState,
} from "./Console/reducers"

const rootReducer = combineReducers({
  console: _console,
})

export const initialState = {
  console: consoleInitialState,
}

export default rootReducer
