import { format } from "date-fns/fp"
import React, { useCallback, useState } from "react"
import { useDispatch } from "react-redux"
import { CSSTransition } from "react-transition-group"
import { Close } from "@styled-icons/remix-line/Close"
import { Pushpin } from "@styled-icons/remix-line/Pushpin"
import { Pushpin2 } from "@styled-icons/remix-line/Pushpin2"
import styled, { css, keyframes } from "styled-components"

import { bezierTransition, Text, Toast, TransitionDuration } from "components"
import { actions } from "store"
import { Color, NotificationShape, NotificationType } from "types"
import { color } from "utils"

type Props = NotificationShape

type AnimationPlay = "paused" | "running"

const Wrapper = styled(Toast)`
  margin-top: 1rem;
  padding-right: 3rem;
  border-right: none;
  box-shadow: ${color("black")} 0 0 4px;

  overflow: hidden;
  ${bezierTransition};

  &:hover {
    background: ${color("draculaSelection")};
  }
`

const Title = styled(Text)<{ hasLine1: boolean }>`
  display: flex;
  margin-bottom: ${({ hasLine1 }) => (hasLine1 ? "0.4rem" : "0")};
`

const baseIconStyles = css`
  position: absolute;
  top: 0.1rem;
  right: 0.7rem;
  color: ${color("gray2")};

  &:hover {
    color: ${color("white")};
    cursor: pointer;
  }
`

const CloseIcon = styled(Close)`
  ${baseIconStyles};
`

const disappear = keyframes`
  from {
    left: 0;
  }

  to {
    left: 100%
  }
`

const Out = styled.div<{ animationPlay: AnimationPlay }>`
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  height: 1px;
  background: ${color("gray2")};
  animation: ${disappear} 120s linear 0s 1 normal forwards;
  animation-play-state: ${({ animationPlay }) => animationPlay};
`

const Pin = styled(Pushpin2)`
  ${baseIconStyles};
  top: 1.8rem;
  right: 0.8rem;
`

const Unpin = styled(Pushpin)`
  ${baseIconStyles};
  top: 1.8rem;
  right: 0.8rem;
`

const getBorderColor = (type: NotificationType): Color => {
  if (type === NotificationType.SUCCESS) {
    return "draculaGreen"
  }

  if (type === NotificationType.ERROR) {
    return "draculaRed"
  }

  return "draculaCyan"
}

const Notification = ({ createdAt, line1, title, type, ...rest }: Props) => {
  const [pinned, setPinned] = useState(false)
  const [animationPlay, setAnimationPlay] = useState<AnimationPlay>("running")
  const dispatch = useDispatch()
  const handleCloseClick = useCallback(() => {
    dispatch(actions.query.removeNotification(createdAt))
  }, [createdAt, dispatch])
  const handlePinClick = useCallback(() => {
    setPinned(!pinned)
  }, [pinned])
  const handleMouseEnter = useCallback(() => {
    setAnimationPlay("paused")
  }, [])
  const handleMouseLeave = useCallback(() => {
    setAnimationPlay("running")
  }, [])

  return (
    <CSSTransition
      classNames="slide"
      timeout={TransitionDuration.REG}
      unmountOnExit
      {...rest}
    >
      <Wrapper
        borderColor={getBorderColor(type)}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        <Title color="gray2" hasLine1={!!line1}>
          [{format("HH:mm:ss", createdAt)}]&nbsp;{title}
        </Title>

        {line1}

        {!pinned && (
          <Out
            animationPlay={animationPlay}
            onAnimationEnd={handleCloseClick}
          />
        )}

        <CloseIcon onClick={handleCloseClick} size="16px" />

        {pinned ? (
          <Unpin onClick={handlePinClick} size="14px" />
        ) : (
          <Pin onClick={handlePinClick} size="14px" />
        )}
      </Wrapper>
    </CSSTransition>
  )
}

export default Notification
