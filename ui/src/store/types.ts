import { ConsoleAction } from "./Console/types"
import { QueryAction } from "./Query/types"
import rootReducer from "./reducers"

export type StoreAction = ConsoleAction | QueryAction

export type StoreShape = ReturnType<typeof rootReducer>

export * from "./Console/types"
export * from "./Query/types"
