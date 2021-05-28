import React, { ChangeEvent, useCallback, useContext } from "react"
import styled from "styled-components"
import { Input, Text } from "components"
import { LocalStorageContext } from "providers/LocalStorageProvider"
import { StoreKey } from "utils/localStorage/types"

const Wrapper = styled.div`
  & > :last-child:not(:first-child) {
    margin-left: 1rem;
  }
`

const Duration = () => {
  const { notificationDelay, updateSettings } = useContext(LocalStorageContext)

  const handleChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      updateSettings(StoreKey.NOTIFICATION_DELAY, e.target.value)
    },
    [updateSettings],
  )

  return (
    <Wrapper>
      <Input
        max={60}
        min={1}
        onChange={handleChange}
        step="1"
        style={{ width: "60px" }}
        type="number"
        value={notificationDelay}
      />
      <Text color="draculaForeground">seconds</Text>
    </Wrapper>
  )
}

export default Duration
