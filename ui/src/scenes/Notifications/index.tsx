import React, {
  useCallback,
  useContext,
  useState,
  useEffect,
  useRef,
  useLayoutEffect,
} from "react"
import { useSelector } from "react-redux"
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
import { selectors } from "store"
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

const Notifications = () => {
  const notifications = useSelector(selectors.query.getNotifications)
  const { isNotificationEnabled } = useContext(LocalStorageContext)
  const { sm } = useScreenSize()
  const [isMinimized, setIsMinimized] = useState(true)
  const notificationsEndRef = useRef<HTMLDivElement | null>(null)
  const contentRef = useRef<HTMLDivElement | null>(null)
  const [hasUnread, setHasUnread] = useState(false)

  const scrollToBottom = () => {
    contentRef.current?.scrollTo({
      top: notificationsEndRef.current?.offsetTop,
    })
  }

  const toggleMinimized = useCallback(() => {
    setHasUnread(false)
    setIsMinimized(!isMinimized)
  }, [isMinimized])

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
        <div ref={notificationsEndRef} />
      </Content>
    </Wrapper>
  )
}

export default Notifications
