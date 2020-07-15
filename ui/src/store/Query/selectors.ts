import { NotificationShape, StoreShape } from "types"

import type { QueryRawResult } from "utils/questdb"

const getNotifications: (store: StoreShape) => NotificationShape[] = (store) =>
  store.query.notifications

const getResult: (store: StoreShape) => undefined | QueryRawResult = (store) =>
  store.query.result

const getRunning: (store: StoreShape) => boolean = (store) =>
  store.query.running

export default {
  getNotifications,
  getResult,
  getRunning,
}
