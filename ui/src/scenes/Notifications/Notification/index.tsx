import React from "react"
import { CSSTransition } from "react-transition-group"
import { TransitionDuration } from "components"
import { NotificationShape, NotificationType } from "types"
import { SuccessNotification } from "./SuccessNotification"
import { ErrorNotification } from "./ErrorNotification"
import { InfoNotification } from "./InfoNotification"

const Notification = (props: NotificationShape) => {
  const { type, ...rest } = props
  return (
    <CSSTransition
      classNames="fade-reg"
      timeout={TransitionDuration.REG}
      unmountOnExit
      {...rest}
    >
      {type === NotificationType.SUCCESS ? (
        <SuccessNotification {...props} />
      ) : type === NotificationType.ERROR ? (
        <ErrorNotification {...props} />
      ) : (
        <InfoNotification {...props} />
      )}
    </CSSTransition>
  )
}

export default Notification
