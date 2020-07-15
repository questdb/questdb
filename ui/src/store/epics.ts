import { combineEpics } from "redux-observable"

import consoleEpic from "./Console/epics"

export default combineEpics(...consoleEpic)
