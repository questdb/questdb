import React from "react"
import styled from "styled-components"
import { Wrapper, SideContent, Content } from "../styles"
import { NotificationShape } from "types"
import { CheckmarkOutline } from "@styled-icons/evaicons-outline/CheckmarkOutline"
import { color } from "utils"
import { Timestamp } from "../Timestamp"

const CheckmarkOutlineIcon = styled(CheckmarkOutline)`
  color: ${color("draculaGreen")};
`

export const SuccessNotification = (props: NotificationShape) => {
  const { createdAt, content, sideContent } = props
  return (
    <Wrapper>
      <Timestamp createdAt={createdAt} />
      <CheckmarkOutlineIcon size="18px" />
      <Content>{content}</Content>
      <SideContent>{sideContent}</SideContent>
    </Wrapper>
  )
}
