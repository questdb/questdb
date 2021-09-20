import React, {
  useCallback,
  useContext,
  useState,
  useEffect,
  useRef,
  useLayoutEffect,
} from "react"
import { useDispatch, useSelector } from "react-redux"
import { TransitionGroup } from "react-transition-group"
import styled from "styled-components"
import {
  PaneContent,
  PaneMenu,
  PaneWrapper,
  Text,
  SecondaryButton,
  useScreenSize,
} from "components"
import { color } from "utils"
import { actions, selectors } from "store"
import { LocalStorageContext } from "providers/LocalStorageProvider"
import { TerminalBox } from "@styled-icons/remix-line/TerminalBox"
import { Subtract } from "@styled-icons/remix-line/Subtract"
import { ArrowUpS } from "@styled-icons/remix-line/ArrowUpS"

import Notification from "./Notification"

const Wrapper = styled(PaneWrapper)<{ minimized: boolean }>`
  flex: ${(props) => (props.minimized ? "initial" : "1")};
  overflow-x: auto;
  max-height: 350px;
`

const Menu = styled(PaneMenu)`
  justify-content: space-between;
`

const Content = styled(PaneContent)<{ minimized: boolean }>`
  display: ${(props) => (props.minimized ? "none" : "flex")};
  overflow: auto;
  padding: 1rem;
  height: 100%;
`

const Header = styled(Text)`
  display: flex;
  align-items: center;
`

const TerminalBoxIcon = styled(TerminalBox)`
  margin-right: 1rem;
`

const ActivityIcon = styled.span`
  position: relative;
  margin-left: 0.5rem;
  top: -0.5rem;
  color: ${color("draculaPink")};
  font-size: 2rem;
  line-height: 2rem;
`

const ClearAllNotifications = styled.div`
  display: flex;
  width: 100%;
  justify-content: center;
  margin-top: auto;
`

const Notifications = () => {
  const notifications = useSelector(selectors.query.getNotifications)
  const { isNotificationEnabled } = useContext(LocalStorageContext)
  const { sm } = useScreenSize()
  const [isMinimized, setIsMinimized] = useState(true)
  const clearAllNotificationsRef = useRef<HTMLDivElement | null>(null)
  const contentRef = useRef<HTMLDivElement | null>(null)
  const [hasUnread, setHasUnread] = useState(false)
  const dispatch = useDispatch()

  const scrollToBottom = () => {
    contentRef.current?.scrollTo({
      top: clearAllNotificationsRef.current?.offsetTop,
    })
  }

  const toggleMinimized = useCallback(() => {
    setHasUnread(false)
    setIsMinimized(!isMinimized)
  }, [isMinimized])

  const cleanupNotifications = useCallback(() => {
    dispatch(actions.query.cleanupNotifications())
  }, [dispatch])

  useLayoutEffect(() => {
    if (notifications.length > 0) {
      setHasUnread(true)
      scrollToBottom()
    }
  }, [notifications])

  useLayoutEffect(() => {
    if (!isMinimized) {
      scrollToBottom()
    }
  }, [isMinimized])

  useEffect(() => {
    if (sm) {
      setIsMinimized(true)
    }
  }, [sm])

  if (!isNotificationEnabled) {
    return <></>
  }

  return (
    <Wrapper minimized={isMinimized}>
      <Menu>
        <Header color="draculaForeground">
          <TerminalBoxIcon size="18px" />
          Log {hasUnread && isMinimized && <ActivityIcon>&bull;</ActivityIcon>}
        </Header>
        <SecondaryButton onClick={toggleMinimized}>
          {isMinimized ? <ArrowUpS size="18px" /> : <Subtract size="18px" />}
        </SecondaryButton>
      </Menu>
      <Content minimized={isMinimized} ref={contentRef}>
        <TransitionGroup className="notifications">
          {notifications.map((notification) => (
            <Notification
              key={
                notification.createdAt ? notification.createdAt.getTime() : 0
              }
              {...notification}
            />
          ))}
        </TransitionGroup>
        <ClearAllNotifications ref={clearAllNotificationsRef}>
          <SecondaryButton
            disabled={notifications.length === 0}
            onClick={cleanupNotifications}
          >
            Clear all
          </SecondaryButton>
        </ClearAllNotifications>
      </Content>
    </Wrapper>
  )
}

export default Notifications
