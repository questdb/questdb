import type { ReactNode } from "react"

export enum NotificationType {
  ERROR = "error",
  INFO = "info",
  SUCCESS = "success",
}

export type NotificationShape = Readonly<{
  createdAt: Date
  title: ReactNode
  line1?: ReactNode
  line2?: ReactNode
  type: NotificationType
}>

export type QueryStateShape = Readonly<{
  notifications: NotificationShape[]
  running: boolean
}>

export enum QueryAT {
  ADD_NOTIFICATION = "QUERY/ADD_NOTIFICATION",
  CLEANUP_NOTIFICATIONS = "QUERY/CLEANUP_NOTIFICATIONS",
  REMOVE_NOTIFICATION = "QUERY/REMOVE_NOTIFICATION",
  STOP_RUNNING = "QUERY/STOP_RUNNING",
  TOGGLE_RUNNING = "QUERY/TOGGLE_RUNNING",
}

type AddNotificationAction = Readonly<{
  payload: NotificationShape
  type: QueryAT.ADD_NOTIFICATION
}>

type CleanupNotificationsAction = Readonly<{
  type: QueryAT.CLEANUP_NOTIFICATIONS
}>

type RemoveNotificationAction = Readonly<{
  payload: Date
  type: QueryAT.REMOVE_NOTIFICATION
}>

type StopRunningAction = Readonly<{
  type: QueryAT.STOP_RUNNING
}>

type ToggleRunningAction = Readonly<{
  type: QueryAT.TOGGLE_RUNNING
}>

export type QueryAction =
  | AddNotificationAction
  | CleanupNotificationsAction
  | RemoveNotificationAction
  | StopRunningAction
  | ToggleRunningAction
