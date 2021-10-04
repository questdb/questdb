import type { ReactNode } from "react"

import type { QueryRawResult } from "utils/questdb"

import {
  NotificationShape,
  NotificationType,
  QueryAction,
  QueryAT,
} from "types"

const addNotification = (
  payload: Partial<NotificationShape> & { content: ReactNode },
): QueryAction => ({
  payload: {
    createdAt: new Date(),
    type: NotificationType.SUCCESS,
    ...payload,
  },
  type: QueryAT.ADD_NOTIFICATION,
})

const cleanupNotifications = (): QueryAction => ({
  type: QueryAT.CLEANUP_NOTIFICATIONS,
})

const removeNotification = (payload: Date): QueryAction => ({
  payload,
  type: QueryAT.REMOVE_NOTIFICATION,
})

const setResult = (payload: QueryRawResult): QueryAction => ({
  payload,
  type: QueryAT.SET_RESULT,
})

const stopRunning = (): QueryAction => ({
  type: QueryAT.STOP_RUNNING,
})

const toggleRunning = (): QueryAction => ({
  type: QueryAT.TOGGLE_RUNNING,
})

export default {
  addNotification,
  cleanupNotifications,
  removeNotification,
  setResult,
  stopRunning,
  toggleRunning,
}
