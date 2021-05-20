import React, { ChangeEvent, useCallback, useState } from "react"
import styled from "styled-components"
import { Input, Text } from "components"
import { getValue, setValue } from "utils/localStorage"

const Wrapper = styled.div`
  & > :last-child:not(:first-child) {
    margin-left: 1rem;
  }
`

const Duration = () => {
  const persistedValue = getValue("notification.enabled") ?? "5"
  const [duration, setDuration] = useState<string>(persistedValue)

  const handleChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    setDuration(value)
    setValue("notification.delay", value)
  }, [])

  return (
    <Wrapper>
      <Input
        max={60}
        min={1}
        onChange={handleChange}
        step="1"
        style={{ width: "60px" }}
        type="number"
        value={duration}
      />
      <Text color="draculaForeground">seconds</Text>
    </Wrapper>
  )
}

export default Duration
