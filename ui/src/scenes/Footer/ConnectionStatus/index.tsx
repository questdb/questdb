import React, { useState, useEffect } from "react"
import styled from "styled-components"
import { color } from "utils"
import { BusEvent } from "../../../consts"
import { Text } from "../../../components"

const Wrapper = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: flex-start;
  align-items: center;
`

const StatusIcon = styled.div<{ isConnected: boolean }>`
  display: block;
  width: 0.6rem;
  height: 0.6rem;
  border-radius: 50%;
  background-color: ${(props) =>
    props.isConnected ? color("draculaGreen") : color("draculaRed")};
  margin-right: 0.6rem;
`

const ConnectionStatus = () => {
  const [isConnected, setIsConnected] = useState<boolean>(true)
  useEffect(() => {
    window.bus.on(BusEvent.MSG_CONNECTION_OK, () => {
      setIsConnected(true)
    })

    window.bus.on(BusEvent.MSG_CONNECTION_ERROR, () => {
      setIsConnected(false)
    })
  }, [isConnected])

  return (
    <Wrapper>
      <StatusIcon isConnected={isConnected} />
      {isConnected ? (
        <Text color="white">Connected</Text>
      ) : (
        <Text color="white">Error connecting to QuestDB</Text>
      )}
    </Wrapper>
  )
}

export default ConnectionStatus
