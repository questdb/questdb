import React from "react"
import { Wrapper, SideContent, Content } from "../styles"
import { NotificationShape } from "types"
import { Timestamp } from "../Timestamp"

export const InfoNotification = (props: NotificationShape) => {
  const { createdAt, content, sideContent } = props
  return (
    <Wrapper>
      <Timestamp createdAt={createdAt} />
      <Content>{content}</Content>
      <SideContent>{sideContent}</SideContent>
    </Wrapper>
  )
}
