import { ConsoleAction } from "./Console/types"
import rootReducer from "./reducers"

export type StoreAction = ConsoleAction

export type StoreShape = ReturnType<typeof rootReducer>

export * from "./Console/types"
