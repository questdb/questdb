import React from "react"
import styled from "styled-components"
import { CloudOff } from "@styled-icons/remix-line/CloudOff"
import { Text } from "../../../components"
import { ErrorResult } from "../../../utils"

type Props = Readonly<{
  error?: ErrorResult
}>

const Wrapper = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  text-align: center;
  width: 100%;
  height: 100%;
`

const CloudOffIcon = styled(CloudOff)`
  height: 3rem;
`

const ErrorTextLine = styled(Text)`
  margin-top: 1rem;
`

const LoadingError = ({ error }: Props) => {
  return (
    <Wrapper>
      <CloudOffIcon />
      <ErrorTextLine color="white">Cannot load tables</ErrorTextLine>
      {error && (
        <ErrorTextLine color="gray2" size="sm">
          {error.error}
        </ErrorTextLine>
      )}
    </Wrapper>
  )
}

export default LoadingError
