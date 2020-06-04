import { NotificationShape, StoreShape } from "types"

const getNotifications: (store: StoreShape) => NotificationShape[] = (store) =>
  store.query.notifications

const getRunning: (store: StoreShape) => boolean = (store) =>
  store.query.running

export default {
  getNotifications,
  getRunning,
}
