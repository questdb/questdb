import React from "react"
import styled from "styled-components"
import { Wrapper, Content, SideContent } from "../styles"
import { Timestamp } from "../Timestamp"
import { NotificationShape } from "types"
import { CloseOutline } from "@styled-icons/evaicons-outline/CloseOutline"
import { color } from "utils"

const CloseOutlineIcon = styled(CloseOutline)`
  color: ${color("draculaRed")};
`

export const ErrorNotification = (props: NotificationShape) => {
  const { createdAt, content, sideContent } = props
  return (
    <Wrapper>
      <Timestamp createdAt={createdAt} />
      <CloseOutlineIcon size="18px" />
      <Content>{content}</Content>
      <SideContent>{sideContent}</SideContent>
    </Wrapper>
  )
}
