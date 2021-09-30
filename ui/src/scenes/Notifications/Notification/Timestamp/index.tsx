import React from "react"
import styled from "styled-components"
import { Text } from "components"
import { format } from "date-fns/fp"

type Props = {
  createdAt: Date
}

const TimestampText = styled(Text)`
  display: flex;
  margin-right: 0.5rem;
`

export const Timestamp = ({ createdAt }: Props) => (
  <TimestampText color="gray2">[{format("HH:mm:ss", createdAt)}]</TimestampText>
)
