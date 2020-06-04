import React from "react"
import { useSelector } from "react-redux"
import { TransitionGroup } from "react-transition-group"
import { createGlobalStyle } from "styled-components"

import { slideTransition } from "components"
import { selectors } from "store"

import Notification from "./Notification"

const NotificationsStyles = createGlobalStyle`
  .notifications{
    position: fixed;
    top: 4rem;
    right: 1.5rem;
    display: flex;
    flex-direction: column;
    z-index: 10;
  }

  ${slideTransition};
`

const Notifications = () => {
  const notifications = useSelector(selectors.query.getNotifications)

  return (
    <>
      <NotificationsStyles left={340} />
      <TransitionGroup className="notifications">
        {notifications.map((notification) => (
          <Notification
            key={notification.createdAt ? notification.createdAt.getTime() : 0}
            {...notification}
          />
        ))}
        <></>
      </TransitionGroup>
    </>
  )
}

export default Notifications
