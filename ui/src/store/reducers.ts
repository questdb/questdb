import { combineReducers } from "redux"

import _console, {
  initialState as consoleInitialState,
} from "./Console/reducers"

import query, { initialState as queryInitialState } from "./Query/reducers"

const rootReducer = combineReducers({
  console: _console,
  query,
})

export const initialState = {
  console: consoleInitialState,
  query: queryInitialState,
}

export default rootReducer
