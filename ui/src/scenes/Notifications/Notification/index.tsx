import { format } from "date-fns/fp"
import React, { useCallback } from "react"
import { useDispatch } from "react-redux"
import { CSSTransition } from "react-transition-group"
import { Close } from "@styled-icons/remix-line/Close"
import styled, { css } from "styled-components"

import { bezierTransition, Text, Toast, TransitionDuration } from "components"
import { actions } from "store"
import { Color, NotificationShape, NotificationType } from "types"
import { color } from "utils"

type Props = NotificationShape

const Wrapper = styled(Toast)`
  margin-bottom: 1rem;
  padding-right: 3rem;
  border-right: none;
  box-shadow: ${color("black")} 0 0 4px;
  width: 100%;

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
  const dispatch = useDispatch()

  const handleCloseClick = useCallback(() => {
    dispatch(actions.query.removeNotification(createdAt))
  }, [createdAt, dispatch])

  return (
    <CSSTransition
      classNames="fade-reg"
      timeout={TransitionDuration.REG}
      unmountOnExit
      {...rest}
    >
      <Wrapper borderColor={getBorderColor(type)}>
        <Title color="gray2" hasLine1={!!line1}>
          [{format("HH:mm:ss", createdAt)}]&nbsp;{title}
        </Title>

        {line1}

        <CloseIcon onClick={handleCloseClick} size="18px" />
      </Wrapper>
    </CSSTransition>
  )
}

export default Notification
