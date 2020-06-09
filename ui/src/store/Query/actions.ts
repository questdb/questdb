import type { ReactNode } from "react"

import {
  NotificationShape,
  NotificationType,
  QueryAction,
  QueryAT,
} from "types"

const addNotification = (
  payload: Partial<NotificationShape> & { title: ReactNode },
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
  stopRunning,
  toggleRunning,
}
