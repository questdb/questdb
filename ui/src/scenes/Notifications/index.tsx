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
import { actions, selectors } from "store"
import { LocalStorageContext } from "providers/LocalStorageProvider"
import { TerminalBox } from "@styled-icons/remix-line/TerminalBox"
import { Subtract } from "@styled-icons/remix-line/Subtract"
import { ArrowUpS } from "@styled-icons/remix-line/ArrowUpS"

import Notification from "./Notification"

const Wrapper = styled(PaneWrapper)<{ minimized: boolean }>`
  flex: ${(props) => (props.minimized ? "initial" : "1")};
  overflow: auto;
  max-height: 35rem;
`

const Menu = styled(PaneMenu)`
  justify-content: space-between;
`

const Content = styled(PaneContent)<{ minimized: boolean }>`
  overflow: ${(props) => (props.minimized ? "hidden" : "auto")};
  padding: ${(props) => (props.minimized ? "0" : "0 0 1rem")};
  flex: initial;
  height: ${(props) => (props.minimized ? "4rem" : "100%")};
`

const Header = styled(Text)`
  display: flex;
  align-items: center;
`

const TerminalBoxIcon = styled(TerminalBox)`
  margin-right: 1rem;
`

const ClearAllNotifications = styled.div`
  display: flex;
  width: 100%;
  justify-content: center;
  margin-top: auto;
`

const ClearAllNotificationsButton = styled(SecondaryButton)`
  margin-top: 1rem;
`

const Notifications = () => {
  const notifications = useSelector(selectors.query.getNotifications)
  const { isNotificationEnabled } = useContext(LocalStorageContext)
  const { sm } = useScreenSize()
  const [isMinimized, setIsMinimized] = useState(true)
  const contentRef = useRef<HTMLDivElement | null>(null)
  const dispatch = useDispatch()

  const scrollToBottom = () => {
    contentRef.current?.scrollTo({
      top: contentRef.current?.scrollHeight,
    })
  }

  const toggleMinimized = useCallback(() => {
    setIsMinimized(!isMinimized)
  }, [isMinimized])

  const cleanupNotifications = useCallback(() => {
    dispatch(actions.query.cleanupNotifications())
  }, [dispatch])

  useLayoutEffect(() => {
    if (notifications.length > 0) {
      scrollToBottom()
    }
  }, [notifications])

  useLayoutEffect(() => {
    scrollToBottom()
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
          Log
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
        {!isMinimized && (
          <ClearAllNotifications>
            <ClearAllNotificationsButton
              disabled={notifications.length === 0}
              onClick={cleanupNotifications}
            >
              Clear all
            </ClearAllNotificationsButton>
          </ClearAllNotifications>
        )}
      </Content>
    </Wrapper>
  )
}

export default Notifications
